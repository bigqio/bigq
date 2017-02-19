using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// The BigQ server listens on TCP and Websockets and acts as a message queue and distribution network for connected clients.
    /// </summary>
    public class Server
    {
        #region Public-Members

        /// <summary>
        /// Contains configuration-related variables for the server.  
        /// </summary>
        public ServerConfiguration Config;

        #endregion

        #region Private-Members

        //
        // configuration
        //
        private DateTime CreatedUTC;
        private Random RNG;
        private string ServerGUID = "00000000-0000-0000-0000-000000000000";

        //
        // resources
        //
        private MessageBuilder MsgBuilder;
        private ConnectionManager ConnMgr;
        private ChannelManager ChannelMgr;
        private ConcurrentDictionary<string, DateTime> ClientActiveSendMap;     // Receiver GUID, AddedUTC

        //
        // TCP server variables
        //
        private IPAddress TCPListenerIPAddress;
        private TcpListener TCPListener;
        private int TCPActiveConnectionThreads;
        private CancellationTokenSource TCPCancellationTokenSource;
        private CancellationToken TCPCancellationToken;

        //
        // TCP SSL server variables
        //
        private IPAddress TCPSSLListenerIPAddress;
        private TcpListener TCPSSLListener;
        private int TCPSSLActiveConnectionThreads;
        private X509Certificate2 TCPSSLCertificate;
        private CancellationTokenSource TCPSSLCancellationTokenSource;
        private CancellationToken TCPSSLCancellationToken;

        //
        // websocket server variables
        //
        private IPAddress WSListenerIPAddress;
        private HttpListener WSListener;
        private int WSActiveConnectionThreads;
        private CancellationTokenSource WSCancellationTokenSource;
        private CancellationToken WSCancellationToken;

        //
        // websocket SSL server variables
        //
        private IPAddress WSSSLListenerIPAddress;
        private HttpListener WSSSLListener;
        private int WSSSLActiveConnectionThreads;
        private CancellationTokenSource WSSSLCancellationTokenSource;
        private CancellationToken WSSSLCancellationToken;

        //
        // authentication and authorization
        //
        private string UsersLastModified;
        private ConcurrentList<User> UsersList;
        private CancellationTokenSource UsersCancellationTokenSource;
        private CancellationToken UsersCancellationToken;

        private string PermissionsLastModified;
        private ConcurrentList<Permission> PermissionsList;
        private CancellationTokenSource PermissionsCancellationTokenSource;
        private CancellationToken PermissionsCancellationToken;

        //
        // cleanup
        //
        private CancellationTokenSource CleanupCancellationTokenSource;
        private CancellationToken CleanupCancellationToken;

        #endregion

        #region Public-Delegates

        /// <summary>
        /// Delegate method called when the server receives a message from a connected client.
        /// </summary>
        public Func<Message, bool> MessageReceived;

        /// <summary>
        /// Delegate method called when the server stops.
        /// </summary>
        public Func<bool> ServerStopped;

        /// <summary>
        /// Delegate method called when a client connects to the server.
        /// </summary>
        public Func<Client, bool> ClientConnected;

        /// <summary>
        /// Delegate method called when a client issues the login command.
        /// </summary>
        public Func<Client, bool> ClientLogin;

        /// <summary>
        /// Delegate method called when the connection to the server is severed.
        /// </summary>
        public Func<Client, bool> ClientDisconnected;

        /// <summary>
        /// Delegate method called when the server desires to send a log message.
        /// </summary>
        public Func<string, bool> LogMessage;

        #endregion

        #region Constructors

        /// <summary>
        /// Start an instance of the BigQ server process.
        /// </summary>
        /// <param name="configFile">The full path and filename of the configuration file.  Leave null for a default configuration.</param>
        public Server(string configFile)
        {
            #region Load-and-Validate-Config

            CreatedUTC = DateTime.Now.ToUniversalTime();
            RNG = new Random((int)DateTime.Now.Ticks);

            Config = null;

            if (String.IsNullOrEmpty(configFile))
            {
                Config = ServerConfiguration.DefaultConfig();
            }
            else
            {
                Config = ServerConfiguration.LoadConfig(configFile);
            }

            if (Config == null) throw new Exception("Unable to initialize configuration.");

            Config.ValidateConfig();

            #endregion

            #region Set-Class-Variables

            if (!String.IsNullOrEmpty(Config.GUID)) ServerGUID = Config.GUID;

            MsgBuilder = new MessageBuilder(ServerGUID);
            ConnMgr = new ConnectionManager(Config);
            ChannelMgr = new ChannelManager(Config);
            ClientActiveSendMap = new ConcurrentDictionary<string, DateTime>();

            TCPActiveConnectionThreads = 0;
            TCPSSLActiveConnectionThreads = 0;
            WSActiveConnectionThreads = 0;
            WSSSLActiveConnectionThreads = 0;

            UsersLastModified = "";
            UsersList = new ConcurrentList<User>();
            PermissionsLastModified = "";
            PermissionsList = new ConcurrentList<Permission>();

            #endregion

            #region Set-Delegates-to-Null

            MessageReceived = null;
            ServerStopped = null;
            ClientConnected = null;
            ClientLogin = null;
            ClientDisconnected = null;
            LogMessage = null;

            #endregion

            #region Accept-SSL-Certificates

            if (Config.AcceptInvalidSSLCerts) ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };

            #endregion

            #region Stop-Existing-Tasks

            if (TCPCancellationTokenSource != null) TCPCancellationTokenSource.Cancel();
            if (TCPSSLCancellationTokenSource != null) TCPSSLCancellationTokenSource.Cancel();
            if (WSCancellationTokenSource != null) WSCancellationTokenSource.Cancel();
            if (WSSSLCancellationTokenSource != null) WSSSLCancellationTokenSource.Cancel();

            if (UsersCancellationTokenSource != null) UsersCancellationTokenSource.Cancel();
            if (PermissionsCancellationTokenSource != null) PermissionsCancellationTokenSource.Cancel();

            if (CleanupCancellationTokenSource != null) CleanupCancellationTokenSource.Cancel();

            #endregion

            #region Start-Users-and-Permissions-File-Monitor

            UsersCancellationTokenSource = new CancellationTokenSource();
            UsersCancellationToken = UsersCancellationTokenSource.Token;
            Task.Run(() => MonitorUsersFile(), UsersCancellationToken);

            PermissionsCancellationTokenSource = new CancellationTokenSource();
            PermissionsCancellationToken = PermissionsCancellationTokenSource.Token;
            Task.Run(() => MonitorPermissionsFile(), PermissionsCancellationToken);

            #endregion

            #region Start-Cleanup-Task

            CleanupCancellationTokenSource = new CancellationTokenSource();
            CleanupCancellationToken = CleanupCancellationTokenSource.Token;
            Task.Run(() => CleanupTask(), CleanupCancellationToken);

            #endregion

            #region Start-Server-Channels

            if (Config.ServerChannels != null && Config.ServerChannels.Count > 0)
            {
                Client CurrentClient = new Client();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGUID = ServerGUID;
                CurrentClient.SourceIP = "127.0.0.1";
                CurrentClient.SourcePort = 0;
                CurrentClient.ServerIP = CurrentClient.SourceIP;
                CurrentClient.ServerPort = CurrentClient.SourcePort;
                CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUTC = CurrentClient.CreatedUTC;

                foreach (Channel curr in Config.ServerChannels)
                {
                    if (!AddChannel(CurrentClient, curr))
                    {
                        Log("*** Unable to add server channel " + curr.ChannelName);
                    }
                    else
                    {
                        Log("Added server channel " + curr.ChannelName);
                    }
                }
            }

            #endregion

            #region Start-Servers

            if (Config.TcpServer.Enable)
            {
                #region Start-TCP-Server

                if (String.IsNullOrEmpty(Config.TcpServer.IP))
                {
                    TCPListenerIPAddress = System.Net.IPAddress.Any;
                    Config.TcpServer.IP = TCPListenerIPAddress.ToString();
                }
                else
                {
                    TCPListenerIPAddress = IPAddress.Parse(Config.TcpServer.IP);
                }

                TCPListener = new TcpListener(TCPListenerIPAddress, Config.TcpServer.Port);
                Log("Starting TCP server at: tcp://" + Config.TcpServer.IP + ":" + Config.TcpServer.Port);

                TCPCancellationTokenSource = new CancellationTokenSource();
                TCPCancellationToken = TCPCancellationTokenSource.Token;
                Task.Run(() => TCPAcceptConnections(), TCPCancellationToken);

                #endregion
            }

            if (Config.TcpSSLServer.Enable)
            {
                #region Start-TCP-SSL-Server

                TCPSSLCertificate = null;
                if (String.IsNullOrEmpty(Config.TcpSSLServer.PFXCertPassword)) TCPSSLCertificate = new X509Certificate2(Config.TcpSSLServer.PFXCertFile);
                else TCPSSLCertificate = new X509Certificate2(Config.TcpSSLServer.PFXCertFile, Config.TcpSSLServer.PFXCertPassword);

                if (String.IsNullOrEmpty(Config.TcpSSLServer.IP))
                {
                    TCPSSLListenerIPAddress = System.Net.IPAddress.Any;
                    Config.TcpSSLServer.IP = TCPSSLListenerIPAddress.ToString();
                }
                else
                {
                    TCPSSLListenerIPAddress = IPAddress.Parse(Config.TcpSSLServer.IP);
                }

                TCPSSLListener = new TcpListener(TCPSSLListenerIPAddress, Config.TcpSSLServer.Port);
                Log("Starting TCP SSL server at: tcp://" + Config.TcpSSLServer.IP + ":" + Config.TcpSSLServer.Port);

                TCPSSLCancellationTokenSource = new CancellationTokenSource();
                TCPSSLCancellationToken = TCPSSLCancellationTokenSource.Token;
                Task.Run(() => TCPSSLAcceptConnections(), TCPSSLCancellationToken);

                #endregion
            }

            if (Config.WebsocketServer.Enable)
            {
                #region Start-Websocket-Server

                if (String.IsNullOrEmpty(Config.WebsocketServer.IP))
                {
                    WSListenerIPAddress = System.Net.IPAddress.Any;
                    Config.WebsocketServer.IP = "+";
                }

                string prefix = "http://" + Config.WebsocketServer.IP + ":" + Config.WebsocketServer.Port + "/";
                WSListener = new HttpListener();
                WSListener.Prefixes.Add(prefix);
                Log("Starting Websocket server at: " + prefix);

                WSCancellationTokenSource = new CancellationTokenSource();
                WSCancellationToken = WSCancellationTokenSource.Token;
                Task.Run(() => WSAcceptConnections(), WSCancellationToken);

                #endregion
            }

            if (Config.WebsocketSSLServer.Enable)
            {
                #region Start-Websocket-SSL-Server

                //
                //
                // No need to set up the certificate; this is done by binding the certificate to the port
                //
                //

                if (String.IsNullOrEmpty(Config.WebsocketSSLServer.IP))
                {
                    WSSSLListenerIPAddress = System.Net.IPAddress.Any;
                    Config.WebsocketSSLServer.IP = "+";
                }

                string prefix = "https://" + Config.WebsocketSSLServer.IP + ":" + Config.WebsocketSSLServer.Port + "/";
                WSSSLListener = new HttpListener();
                WSSSLListener.Prefixes.Add(prefix);
                Log("Starting Websocket SSL server at: " + prefix);

                WSSSLCancellationTokenSource = new CancellationTokenSource();
                WSSSLCancellationToken = WSSSLCancellationTokenSource.Token;
                Task.Run(() => WSSSLAcceptConnections(), WSSSLCancellationToken);

                #endregion
            }

            #endregion
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Close and dispose of server resources.
        /// </summary>
        public void Close()
        {
            if (TCPCancellationTokenSource != null) TCPCancellationTokenSource.Cancel();
            if (TCPSSLCancellationTokenSource != null) TCPSSLCancellationTokenSource.Cancel();
            if (WSCancellationTokenSource != null) WSCancellationTokenSource.Cancel();
            if (WSSSLCancellationTokenSource != null) WSSSLCancellationTokenSource.Cancel();

            if (UsersCancellationTokenSource != null) UsersCancellationTokenSource.Cancel();
            if (PermissionsCancellationTokenSource != null) PermissionsCancellationTokenSource.Cancel();

            if (CleanupCancellationTokenSource != null) CleanupCancellationTokenSource.Cancel();
            return;
        }
        
        /// <summary>
        /// Retrieve list of all channels.
        /// </summary>
        /// <returns>List of Channel objects.</returns>
        public List<Channel> ListChannels()
        {
            return ChannelMgr.GetChannels();
        }

        /// <summary>
        /// Retrieve list of members in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListChannelMembers(string guid)
        {
            return ChannelMgr.GetChannelMembers(guid);
        }

        /// <summary>
        /// Retrieve list of subscribers in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListChannelSubscribers(string guid)
        {
            return ChannelMgr.GetChannelSubscribers(guid);
        }

        /// <summary>
        /// Retrieve list of all clients on the server.
        /// </summary>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListClients()
        {
            return ConnMgr.GetClients();
        }

        /// <summary>
        /// Retrieve list of all client GUID to IP:port maps.
        /// </summary>
        /// <returns>A dictionary containing client GUIDs (keys) and IP:port strings (values).</returns>
        public Dictionary<string, string> ListClientGUIDMaps()
        {
            return ConnMgr.GetGUIDMaps();
        }

        /// <summary>
        /// Retrieve list of client GUIDs to which the server is currently transmitting messages and on behalf of which sender.
        /// </summary>
        /// <returns>A dictionary containing recipient GUID (key) and sender GUID (value).</returns>
        public Dictionary<string, DateTime> ListClientActiveSend()
        {
            return GetAllClientActiveSendMap();
        }

        /// <summary>
        /// Clear the list of client GUIDs to which the server is currently transmitting messages.  This API should only be used for debugging when advised by support.
        /// </summary>
        public void ClearClientActiveSend()
        {
            ClientActiveSendMap.Clear();
        }

        /// <summary>
        /// Retrieve the connection count.
        /// </summary>
        /// <returns>An int containing the number of active connections (sum of websocket and TCP).</returns>
        public int ConnectionCount()
        {
            return TCPActiveConnectionThreads + TCPSSLActiveConnectionThreads + WSActiveConnectionThreads + WSSSLActiveConnectionThreads;
        }

        /// <summary>
        /// Retrieve all objects in the user configuration file defined in the server configuration file.
        /// </summary>
        /// <returns></returns>
        public List<User> ListCurrentUsersFile()
        {
            return GetCurrentUsersFile();
        }

        /// <summary>
        /// Retrieve all objects in the permissions configuration file defined in the server configuration file.
        /// </summary>
        /// <returns></returns>
        public List<Permission> ListCurrentPermissionsFile()
        {
            return GetCurrentPermissionsFile();
        }

        #endregion

        #region Private-Transport-Connection-Heartbeat

        #region Agnostic
        
        private bool SendMessageImmediately(Client currentClient, Message currentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** SendMessageImmediately null client supplied");
                    return false;
                }

                if (
                    !currentClient.IsTCP 
                    && !currentClient.IsTCPSSL
                    && !currentClient.IsWebsocket
                    && !currentClient.IsWebsocketSSL
                    )
                {
                    Log("*** SendMessageImmediately unable to discern transport for client " + currentClient.IpPort());
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** SendMessageImmediately null message supplied");
                    return false;
                }

                #endregion

                #region Process

                if (currentClient.IsTCP)
                {
                    return TCPDataSender(currentClient, currentMessage);
                }
                if (currentClient.IsTCPSSL)
                {
                    return TCPSSLDataSender(currentClient, currentMessage);
                }
                else if (currentClient.IsWebsocket)
                {
                    return WSDataSender(currentClient, currentMessage);
                }
                else if (currentClient.IsWebsocketSSL)
                {
                    return WSDataSender(currentClient, currentMessage);
                }
                else
                {
                    Log("*** SendMessageImmediately unable to discern transport for client " + currentClient.IpPort());
                    return false;
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("SendMessageImmediately (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("SendMessageImmediately (null)", e);
                }

                return false;
            }
        }

        private bool ChannelDataSender(Client currentClient, Channel currentChannel, Message currentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (currentChannel == null)
                {
                    Log("*** ChannelDataSender null channel supplied");
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** ChannelDataSender null message supplied");
                    return false;
                }

                #endregion

                #region Process-by-Channel-Type

                if (Helper.IsTrue(currentChannel.Broadcast))
                {
                    #region Broadcast-Channel

                    List<Client> currChannelMembers = ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
                    if (currChannelMembers == null || currChannelMembers.Count < 1)
                    {
                        Log("*** ChannelDataSender no members found in channel " + currentChannel.ChannelGUID);
                        return true;
                    }

                    currentMessage.SenderGUID = currentClient.ClientGUID;
                    foreach (Client curr in currChannelMembers)
                    {
                        Task.Run(() =>
                        {
                            currentMessage.RecipientGUID = curr.ClientGUID;
                            bool ResponseSuccess = false;
                            ResponseSuccess = QueueClientMessage(curr, currentMessage);
                            if (!ResponseSuccess)
                            {
                                Log("*** ChannelDataSender error queuing channel message from " + currentMessage.SenderGUID + " to member " + currentMessage.RecipientGUID + " in channel " + currentMessage.ChannelGUID);
                            }
                        });
                    }

                    return true;

                    #endregion
                }
                else if (Helper.IsTrue(currentChannel.Multicast))
                {
                    #region Multicast-Channel-to-Subscribers

                    List<Client> currChannelSubscribers = ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
                    if (currChannelSubscribers == null || currChannelSubscribers.Count < 1)
                    {
                        Log("*** ChannelDataSender no subscribers found in channel " + currentChannel.ChannelGUID);
                        return true;
                    }

                    currentMessage.SenderGUID = currentClient.ClientGUID;
                    foreach (Client curr in currChannelSubscribers)
                    {
                        Task.Run(() =>
                        {
                            currentMessage.RecipientGUID = curr.ClientGUID;
                            bool respSuccess = false;
                            respSuccess = QueueClientMessage(curr, currentMessage);
                            if (!respSuccess)
                            {
                                Log("*** ChannelDataSender error queuing channel message from " + currentMessage.SenderGUID + " to subscriber " + currentMessage.RecipientGUID + " in channel " + currentMessage.ChannelGUID);
                            }
                        });
                    }

                    return true;

                    #endregion
                }
                else if (Helper.IsTrue(currentChannel.Unicast))
                {
                    #region Unicast-Channel-to-Subscriber

                    List<Client> currChannelSubscribers = ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
                    if (currChannelSubscribers == null || currChannelSubscribers.Count < 1)
                    {
                        Log("*** ChannelDataSender no subscribers found in channel " + currentChannel.ChannelGUID);
                        return true;
                    }

                    currentMessage.SenderGUID = currentClient.ClientGUID;
                    Client recipient = currChannelSubscribers[RNG.Next(0, currChannelSubscribers.Count)];
                    Task.Run(() =>
                    {
                        currentMessage.RecipientGUID = recipient.ClientGUID;
                        bool respSuccess = false;
                        respSuccess = QueueClientMessage(recipient, currentMessage);
                        if (!respSuccess)
                        {
                            Log("*** ChannelDataSender error queuing channel message from " + currentMessage.SenderGUID + " to subscriber " + currentMessage.RecipientGUID + " in channel " + currentMessage.ChannelGUID);
                        }
                    });

                    return true;

                    #endregion
                }
                else
                {
                    #region Unknown

                    Log("*** ChannelDataSender channel is not designated as broadcast, multicast, or unicast, deleting");
                    return RemoveChannel(currentChannel);

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("ChannelDataSender (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("ChannelDataSender (null)", e);
                }

                return false;
            }
        }
        
        private bool QueueClientMessage(Client currentClient, Message currentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** QueueClientMessage null client supplied");
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** QueueClientMessage null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.SenderGUID))
                {
                    Log("*** QueueClientMessage null sender GUID supplied in message");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    Log("*** QueueClientMessage null recipient GUID in supplied message");
                    return false;
                }
                
                if (currentClient.MessageQueue == null)
                {
                    currentClient.MessageQueue = new BlockingCollection<Message>();
                }

                #endregion

                #region Process

                Log("QueueClientMessage queued message for client " + currentClient.IpPort() + " " + currentClient.ClientGUID + " from " + currentMessage.SenderGUID);
                currentClient.MessageQueue.Add(currentMessage);
                return true;

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("QueueClientMessage (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("QueueClientMessage (null)", e);
                }

                return false;
            }
        }

        private bool ProcessClientQueue(Client currentClient)
        {
            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** ProcessClientQueue null client supplied");
                    return false;
                }

                if (currentClient.MessageQueue == null)
                {
                    currentClient.MessageQueue = new BlockingCollection<Message>();
                }

                #endregion

                #region Process

                while (true)
                {
                    Message currMessage = currentClient.MessageQueue.Take(currentClient.ProcessClientQueueToken);
                    if (currMessage != null)
                    {
                        bool success = SendMessageImmediately(currentClient, currMessage);
                        if (!success)
                        {
                            Log("*** ProcessClientQueue unable to deliver message from " + currMessage.SenderGUID + " to " + currMessage.RecipientGUID + ", requeuing");
                            currentClient.MessageQueue.Add(currMessage);
                        }
                        else
                        {
                            Log("ProcessClientQueue successfully sent message from " + currMessage.SenderGUID + " to " + currMessage.RecipientGUID);
                        }
                    }
                    else
                    {
                        Log("*** ProcessClientQueue received null message from queue for client " + currentClient.ClientGUID);
                    }
                }

                #endregion
            }
            catch (OperationCanceledException)
            {
                Log("ProcessClientQueue canceled for client " + currentClient.IpPort() + " likely due to disconnect");
                return false;
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("ProcessClientQueue (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("ProcessClientQueue (null)", e);
                }

                return false;
            }
        }

        #endregion

        #region TCP-Server

        private void TCPAcceptConnections()
        {
            try
            {
                #region Accept-TCP-Connections

                TCPListener.Start();
                while (!TCPCancellationToken.IsCancellationRequested)
                {
                    // Log("TCPAcceptConnections waiting for next connection");

                    TcpClient client = TCPListener.AcceptTcpClientAsync().Result;
                    client.LingerState.Enabled = false;
                    
                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string clientIp = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();
                        int clientPort = ((IPEndPoint)client.Client.RemoteEndPoint).Port;
                        Log("TCPAcceptConnections accepted connection from " + clientIp + ":" + clientPort);

                        #endregion

                        #region Increment-Counters

                        TCPActiveConnectionThreads++;

                        //
                        //
                        // Do not decrement in this block, decrement is done by the connection reader
                        //
                        //

                        #endregion

                        #region Add-to-Client-List

                        Client currClient = new Client();
                        currClient.SourceIP = clientIp;
                        currClient.SourcePort = clientPort;
                        currClient.ClientTCPInterface = client;
                        currClient.ClientTCPSSLInterface = null;
                        currClient.ClientSSLStream = null;
                        currClient.ClientHTTPContext = null;
                        currClient.ClientWSContext = null;
                        currClient.ClientWSInterface = null;
                        currClient.ClientWSSSLContext = null;
                        currClient.ClientWSSSLInterface = null;
                        currClient.MessageQueue = new BlockingCollection<Message>();

                        currClient.IsTCP = true;
                        currClient.IsTCPSSL = false;
                        currClient.IsWebsocket = false;
                        currClient.IsWebsocketSSL = false;
                        currClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        currClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        ConnMgr.AddClient(currClient);

                        #endregion

                        #region Start-Data-Receiver

                        currClient.DataReceiverTokenSource = new CancellationTokenSource();
                        currClient.DataReceiverToken = currClient.DataReceiverTokenSource.Token;
                        Log("TCPAcceptConnections starting data receiver for " + currClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)");
                        Task.Run(() => TCPDataReceiver(currClient), currClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        currClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        currClient.ProcessClientQueueToken = currClient.ProcessClientQueueTokenSource.Token;
                        Log("TCPAcceptConnections starting queue processor for " + currClient.IpPort());
                        Task.Run(() => ProcessClientQueue(currClient), currClient.ProcessClientQueueToken);

                        #endregion

                    }, TCPCancellationToken);
                }

                #endregion
            }
            catch (Exception e)
            {
                LogException("TCPAcceptConnections", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private void TCPDataReceiver(Client currentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** TCPDataReceiver null client supplied");
                    return;
                }

                if (currentClient.ClientTCPInterface == null)
                {
                    Log("*** TCPDataReceiver null TcpClient supplied within client");
                    return;
                }

                #endregion

                #region Wait-for-Data

                if (!currentClient.ClientTCPInterface.Connected)
                {
                    Log("*** TCPDataReceiver client " + currentClient.IpPort() + " is no longer connected");
                    return;
                }

                NetworkStream clientStream = currentClient.ClientTCPInterface.GetStream();

                while (true)
                {
                    #region Retrieve-Message

                    Message currMessage = null;
                    if (!Helper.TCPMessageRead(
                        currentClient.ClientTCPInterface, 
                        (Config.Debug.Enable && (Config.Debug.Enable && Config.Debug.MsgResponseTime)),
                        out currMessage))
                    {
                        Log("*** TCPDataReceiver disconnect detected for client " + currentClient.IpPort());
                        return;
                    }

                    if (currMessage == null)
                    {
                        Task.Delay(30).Wait();
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPDataReceiver received message from " + currentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();
                    }

                    if (!currMessage.IsValid())
                    {
                        Log("TCPDataReceiver invalid message received from client " + currentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPDataReceiver verified message validity from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(currentClient, currMessage);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPDataReceiver processed message from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(currMessage));
                    // Log("TCPDataReceiver finished processing message from client " + CurrentClient.IpPort());

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("TCPDataReceiver (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("TCPDataReceiver (null)", e);
                }
            }
            finally
            {
                TCPActiveConnectionThreads--;
                Log("TCPDataReceiver closed data receiver for " + currentClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)");
                currentClient.Close();
            }
        }

        private bool TCPDataSender(Client currentClient, Message currentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** TCPDataSender null client supplied");
                    return false;
                }

                if (currentClient.ClientTCPInterface == null)
                {
                    Log("*** TCPDataSender null TcpClient supplied within client object for client " + currentClient.ClientGUID);
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** TCPDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.SenderGUID))
                {
                    Log("*** TCPDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    Log("*** TCPDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion
                
                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(currentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Task.Delay(25).Wait();
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** TCPDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** TCPDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                if (!Helper.TCPMessageWrite(currentClient.ClientTCPInterface, currentMessage, (Config.Debug.Enable && Config.Debug.MsgResponseTime)))
                {
                    Log("TCPDataSender unable to send data to client " + currentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(currentMessage.Command))
                    {
                        Log("TCPDataSender successfully sent data to client " + currentClient.IpPort() + " for command " + currentMessage.Command);
                    }
                    else
                    {
                        Log("TCPDataSender successfully sent data to client " + currentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("TCPDataSender (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("TCPDataSender (null)", e);
                }

                return false;
            }
            finally
            {
                //
                // remove active client send lock
                //
                if (locked)
                {
                    DateTime removedVal = DateTime.Now;
                    int removeLoopCount = 0;
                    while (!ClientActiveSendMap.TryRemove(currentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Task.Delay(25).Wait();
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(currentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** TCPDataSender locked send map attempting to remove recipient GUID " + currentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void TCPHeartbeatManager(Client currentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Null-Values

                if (Config.Heartbeat.IntervalMs <= 0)
                {
                    Log("*** TCPHeartbeatManager invalid heartbeat interval, using 1000ms");
                    Config.Heartbeat.IntervalMs = 1000;
                }

                if (currentClient == null)
                {
                    Log("*** TCPHeartbeatManager null client supplied");
                    return;
                }

                if (currentClient.ClientTCPInterface == null)
                {
                    Log("*** TCPHeartbeatManager null TcpClient supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(currentClient.ClientGUID))
                {
                    Log("*** TCPHeartbeatManager null client GUID in supplied client");
                    return;
                }

                #endregion

                #region Variables

                DateTime threadStart = DateTime.Now;
                DateTime lastHeartbeatAttempt = DateTime.Now;
                DateTime lastSuccess = DateTime.Now;
                DateTime lastFailure = DateTime.Now;
                int numConsecutiveFailures = 0;
                bool firstRun = true;

                #endregion

                #region Process

                while (true)
                {
                    #region Sleep

                    if (firstRun)
                    {
                        firstRun = false;
                    }
                    else
                    {
                        Task.Delay(Config.Heartbeat.IntervalMs).Wait();
                    }

                    #endregion
                    
                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message heartbeatMessage = MsgBuilder.HeartbeatRequest(currentClient);
                    if (Config.Debug.SendHeartbeat) Log("TCPHeartbeatManager sending heartbeat to " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID);

                    if (!TCPDataSender(currentClient, heartbeatMessage))
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** TCPHeartbeatManager failed to send heartbeat to client " + currentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** TCPHeartbeatManager maximum number of failed heartbeats reached, abandoning client " + currentClient.IpPort());
                            return;
                        }
                    }
                    else
                    {
                        numConsecutiveFailures = 0;
                        lastSuccess = DateTime.Now;
                    }

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("TCPHeartbeatManager (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("TCPHeartbeatManager (null)", e);
                }
            }
            finally
            {
                List<Channel> affectedChannels = null;
                ConnMgr.RemoveClient(currentClient.IpPort());
                ChannelMgr.RemoveClientChannels(currentClient.ClientGUID, out affectedChannels);
                ChannelDestroyEvent(affectedChannels);
                ChannelMgr.RemoveClient(currentClient.IpPort());
                if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(currentClient));
            }
        }

        #endregion

        #region TCP-SSL-Server

        private void TCPSSLAcceptConnections()
        {
            try
            {
                #region Accept-TCP-SSL-Connections

                TCPSSLListener.Start();
                while (!TCPSSLCancellationToken.IsCancellationRequested)
                {
                    // Log("TCPAcceptConnections waiting for next connection");

                    TcpClient client = TCPSSLListener.AcceptTcpClientAsync().Result;
                    client.LingerState.Enabled = false;

                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string clientIp = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();
                        int clientPort = ((IPEndPoint)client.Client.RemoteEndPoint).Port;
                        Log("TCPSSLAcceptConnections accepted connection from " + clientIp + ":" + clientPort);

                        #endregion

                        #region Initialize-and-Authenticate-as-Server

                        SslStream sslStream = null;
                        if (Config.AcceptInvalidSSLCerts)
                        {
                            sslStream = new SslStream(client.GetStream(), false, new RemoteCertificateValidationCallback(TCPSSLValidateCert));
                        }
                        else
                        {
                            //
                            // do not accept invalid SSL certificates
                            //
                            sslStream = new SslStream(client.GetStream(), false);
                        }

                        sslStream.AuthenticateAsServer(TCPSSLCertificate, true, SslProtocols.Tls, false);
                        Log("TCPSSLAcceptConnections SSL authentication complete with " + clientIp + ":" + clientPort);

                        #endregion

                        #region Increment-Counters

                        TCPSSLActiveConnectionThreads++;

                        //
                        //
                        // Do not decrement in this block, decrement is done by the connection reader
                        //
                        //

                        #endregion

                        #region Add-to-Client-List

                        Client currClient = new Client();
                        currClient.SourceIP = clientIp;
                        currClient.SourcePort = clientPort;
                        currClient.ClientTCPInterface = null;
                        currClient.ClientTCPSSLInterface = client;
                        currClient.ClientSSLStream = sslStream;
                        currClient.ClientHTTPContext = null;
                        currClient.ClientWSContext = null;
                        currClient.ClientWSInterface = null;
                        currClient.ClientWSSSLContext = null;
                        currClient.ClientWSSSLInterface = null;
                        currClient.MessageQueue = new BlockingCollection<Message>();

                        currClient.IsTCP = false;
                        currClient.IsTCPSSL = true;
                        currClient.IsWebsocket = false;
                        currClient.IsWebsocketSSL = false;
                        currClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        currClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        ConnMgr.AddClient(currClient);

                        #endregion

                        #region Start-Data-Receiver

                        Log("TCPSSLAcceptConnections starting data receiver for " + currClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)");
                        currClient.DataReceiverTokenSource = new CancellationTokenSource();
                        currClient.DataReceiverToken = currClient.DataReceiverTokenSource.Token;
                        Task.Run(() => TCPSSLDataReceiver(currClient), currClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        currClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        currClient.ProcessClientQueueToken = currClient.ProcessClientQueueTokenSource.Token;
                        Log("TCPSSLAcceptConnections starting queue processor for " + currClient.IpPort());
                        Task.Run(() => ProcessClientQueue(currClient), currClient.ProcessClientQueueToken);

                        #endregion
                        
                    }, TCPSSLCancellationToken);
                }

                #endregion
            }
            catch (Exception e)
            {
                LogException("TCPSSLAcceptConnections", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private void TCPSSLDataReceiver(Client currentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** TCPSSLDataReceiver null client supplied");
                    return;
                }

                if (currentClient.ClientTCPSSLInterface == null)
                {
                    Log("*** TCPSSLDataReceiver null TcpClient supplied within client");
                    return;
                }

                if (currentClient.ClientSSLStream == null)
                {
                    Log("*** TCPSSLDataReceiver null SslStream supplied within client");
                    return;
                }

                if (!currentClient.ClientSSLStream.CanRead || !currentClient.ClientSSLStream.CanWrite)
                {
                    Log("*** TCPSSLDataReceiver supplied SslStream is either not readable or not writeable");
                    return;
                }

                #endregion

                #region Wait-for-Data

                if (!currentClient.ClientTCPSSLInterface.Connected)
                {
                    Log("*** TCPSSLDataReceiver client " + currentClient.IpPort() + " is no longer connected");
                    return;
                }

                NetworkStream clientStream = currentClient.ClientTCPSSLInterface.GetStream();

                while (true)
                {
                    #region Retrieve-Message

                    Message message = Helper.TCPSSLMessageRead(currentClient.ClientTCPSSLInterface, currentClient.ClientSSLStream, (Config.Debug.Enable && (Config.Debug.Enable && Config.Debug.MsgResponseTime)));
                    if (message == null)
                    {
                        // Log("*** TCPSSLDataReceiver unable to read from client " + CurrentClient.IpPort());
                        Task.Delay(30).Wait();
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPSSLDataReceiver received message from " + currentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();
                    }

                    if (!message.IsValid())
                    {
                        Log("TCPSSLDataReceiver invalid message received from client " + currentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPSSLDataReceiver verified message validity from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(currentClient, message);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPSSLDataReceiver processed message from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(message));
                    // Log("TCPSSLDataReceiver finished processing message from client " + CurrentClient.IpPort());

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("TCPSSLDataReceiver (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("TCPSSLDataReceiver (null)", e);
                }
            }
            finally
            {
                TCPSSLActiveConnectionThreads--;
                Log("TCPSSLDataReceiver closed data receiver for " + currentClient.IpPort() + " (now " + TCPSSLActiveConnectionThreads + " connections active)");

                currentClient.Close();
            }
        }

        private bool TCPSSLDataSender(Client currentClient, Message currentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** TCPSSLDataSender null client supplied");
                    return false;
                }

                if (currentClient.ClientTCPSSLInterface == null)
                {
                    Log("*** TCPSSLDataSender null TcpClient supplied within client object for client " + currentClient.ClientGUID);
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** TCPSSLDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.SenderGUID))
                {
                    Log("*** TCPSSLDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    Log("*** TCPSSLDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion
                
                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(currentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Task.Delay(25).Wait();
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** TCPSSLDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** TCPSSLDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                if (!Helper.TCPSSLMessageWrite(currentClient.ClientTCPSSLInterface, currentClient.ClientSSLStream, currentMessage, (Config.Debug.Enable && Config.Debug.MsgResponseTime)))
                {
                    Log("TCPSSLDataSender unable to send data to client " + currentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(currentMessage.Command))
                    {
                        Log("TCPSSLDataSender successfully sent data to client " + currentClient.IpPort() + " for command " + currentMessage.Command);
                    }
                    else
                    {
                        Log("TCPSSLDataSender successfully sent data to client " + currentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("TCPSSLDataSender (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("TCPSSLDataSender (null)", e);
                }

                return false;
            }
            finally
            {
                //
                // remove active client send lock
                //
                if (locked)
                {
                    DateTime removedVal = DateTime.Now;
                    int removeLoopCount = 0;
                    while (!ClientActiveSendMap.TryRemove(currentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Task.Delay(25).Wait();
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(currentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** TCPSSLDataSender locked send map attempting to remove recipient GUID " + currentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void TCPSSLHeartbeatManager(Client currentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Null-Values

                if (Config.Heartbeat.IntervalMs <= 0)
                {
                    Log("*** TCPSSLHeartbeatManager invalid heartbeat interval, using 1000ms");
                    Config.Heartbeat.IntervalMs = 1000;
                }

                if (currentClient == null)
                {
                    Log("*** TCPSSLHeartbeatManager null client supplied");
                    return;
                }

                if (currentClient.ClientTCPSSLInterface == null)
                {
                    Log("*** TCPSSLHeartbeatManager null TcpClient supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(currentClient.ClientGUID))
                {
                    Log("*** TCPSSLHeartbeatManager null client GUID in supplied client");
                    return;
                }

                #endregion

                #region Variables

                DateTime threadStart = DateTime.Now;
                DateTime lastHeartbeatAttempt = DateTime.Now;
                DateTime lastSuccess = DateTime.Now;
                DateTime lastFailure = DateTime.Now;
                int numConsecutiveFailures = 0;
                bool firstRun = true;

                #endregion

                #region Process

                while (true)
                {
                    #region Sleep

                    if (firstRun)
                    {
                        firstRun = false;
                    }
                    else
                    {
                        Task.Delay(Config.Heartbeat.IntervalMs).Wait();
                    }

                    #endregion
                    
                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message heartbeatMessage = MsgBuilder.HeartbeatRequest(currentClient);
                    if (Config.Debug.SendHeartbeat) Log("TCPSSLHeartbeatManager sending heartbeat to " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID);

                    if (!TCPSSLDataSender(currentClient, heartbeatMessage))
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** TCPSSLHeartbeatManager failed to send heartbeat to client " + currentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** TCPSSLHeartbeatManager maximum number of failed heartbeats reached, abandoning client " + currentClient.IpPort());
                            return;
                        }
                    }
                    else
                    {
                        numConsecutiveFailures = 0;
                        lastSuccess = DateTime.Now;
                    }

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("TCPSSLHeartbeatManager (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("TCPSSLHeartbeatManager (null)", e);
                }
            }
            finally
            {
                List<Channel> affectedChannels = null;
                ConnMgr.RemoveClient(currentClient.IpPort());
                ChannelMgr.RemoveClientChannels(currentClient.ClientGUID, out affectedChannels);
                ChannelDestroyEvent(affectedChannels);
                ChannelMgr.RemoveClient(currentClient.IpPort());
                if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(currentClient));
            }
        }

        private bool TCPSSLValidateCert(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            // return true; // Allow untrusted certificates.
            return Config.AcceptInvalidSSLCerts;
        }

        #endregion

        #region Websocket-Server

        private void WSAcceptConnections()
        {
            try
            {
                #region Accept-WS-Connections

                WSListener.Start();
                while (!WSCancellationToken.IsCancellationRequested)
                {
                    HttpListenerContext context = WSListener.GetContextAsync().Result;

                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string clientIp = context.Request.RemoteEndPoint.Address.ToString();
                        int clientPort = context.Request.RemoteEndPoint.Port;
                        Log("WSAcceptConnections accepted connection from " + clientIp + ":" + clientPort);

                        #region Increment-Counters

                        WSActiveConnectionThreads++;

                        //
                        //
                        // Do not decrement in this block, decrement is done by the connection reader
                        //
                        //

                        #endregion

                        #endregion

                        #region Get-Websocket-Context

                        WebSocketContext wsContext = null;
                        try
                        {
                            wsContext = context.AcceptWebSocketAsync(subProtocol: null).Result;
                        }
                        catch (Exception)
                        {
                            Log("*** WSAcceptConnections exception while gathering websocket context for client " + clientIp + ":" + clientPort);
                            context.Response.StatusCode = 500;
                            context.Response.Close();
                            return;
                        }

                        WebSocket client = wsContext.WebSocket;

                        #endregion

                        #region Add-to-Client-List

                        Client currentClient = new Client();
                        currentClient.SourceIP = clientIp;
                        currentClient.SourcePort = clientPort;
                        currentClient.ClientTCPInterface = null;
                        currentClient.ClientTCPSSLInterface = null;
                        currentClient.ClientHTTPContext = context;
                        currentClient.ClientHTTPSSLContext = context;
                        currentClient.ClientWSContext = wsContext;
                        currentClient.ClientWSInterface = client;
                        currentClient.ClientWSSSLContext = wsContext;
                        currentClient.ClientWSSSLInterface = client;
                        currentClient.MessageQueue = new BlockingCollection<Message>();

                        currentClient.IsTCP = false;
                        currentClient.IsTCPSSL = false;
                        currentClient.IsWebsocket = true;
                        currentClient.IsWebsocketSSL = false;
                        currentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        currentClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        ConnMgr.AddClient(currentClient);

                        #endregion

                        #region Start-Data-Receiver

                        Log("WSAcceptConnections starting data receiver for " + currentClient.IpPort() + " (now " + WSActiveConnectionThreads + " connections active)");
                        currentClient.DataReceiverTokenSource = new CancellationTokenSource();
                        currentClient.DataReceiverToken = currentClient.DataReceiverTokenSource.Token;
                        Task.Run(() => WSDataReceiver(currentClient), currentClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        currentClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        currentClient.ProcessClientQueueToken = currentClient.ProcessClientQueueTokenSource.Token;
                        Log("WSAcceptConnections starting queue processor for " + currentClient.IpPort());
                        Task.Run(() => ProcessClientQueue(currentClient), currentClient.ProcessClientQueueToken);

                        #endregion
                        
                    }, WSCancellationToken);
                }

                #endregion
            }
            catch (Exception e)
            {
                LogException("WSAcceptConnections", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private void WSDataReceiver(Client currentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** WSDataReceiver null client supplied");
                    return;
                }

                if (currentClient.ClientWSInterface == null)
                {
                    Log("*** WSDataReceiver null WebSocket supplied within client");
                    return;
                }

                #endregion

                #region Wait-for-Data

                while (true)
                {
                    #region Retrieve-Message

                    Message currentMessage = Helper.WSMessageRead(currentClient.ClientHTTPContext, currentClient.ClientWSInterface, Config.Debug.MsgResponseTime).Result;
                    if (currentMessage == null)
                    {
                        Log("WSDataReceiver unable to read message from client " + currentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSDataReceiver received message from " + currentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();

                        Task.Run(() => MessageReceived(currentMessage));
                    }

                    if (!currentMessage.IsValid())
                    {
                        Log("WSDataReceiver invalid message received from client " + currentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSDataReceiver verified message validity from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(currentClient, currentMessage);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSDataReceiver processed message from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(currentMessage));

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("WSDataReceiver (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("WSDataReceiver (null)", e);
                }
            }
            finally
            {
                WSActiveConnectionThreads--;
                Log("WSDataReceiver closed data receiver for " + currentClient.IpPort() + " (now " + WSActiveConnectionThreads + " connections active)");

                currentClient.Close();
            }
        }

        private bool WSDataSender(Client currentClient, Message currentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** WSDataSender null client supplied");
                    return false;
                }

                if (currentClient.ClientWSInterface == null)
                {
                    Log("*** WSDataSender null websocket supplied within client object for client " + currentClient.ClientGUID);
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** WSDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.SenderGUID))
                {
                    Log("*** WSDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    Log("*** WSDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion
                
                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(currentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Task.Delay(25).Wait();
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** WSDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** WSDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                bool success = Helper.WSMessageWrite(currentClient.ClientHTTPContext, currentClient.ClientWSInterface, currentMessage, Config.Debug.MsgResponseTime).Result;
                if (!success)
                {
                    Log("WSDataSender unable to send data to client " + currentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(currentMessage.Command))
                    {
                        Log("WSDataSender successfully sent data to client " + currentClient.IpPort() + " for command " + currentMessage.Command);
                    }
                    else
                    {
                        Log("WSDataSender successfully sent data to client " + currentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("WSDataSender (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("WSDataSender (null)", e);
                }

                return false;
            }
            finally
            {
                //
                // remove active client send lock
                //
                if (locked)
                {
                    DateTime removedVal = DateTime.Now;
                    int removeLoopCount = 0;
                    while (!ClientActiveSendMap.TryRemove(currentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Task.Delay(25).Wait();
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(currentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** WSDataSender locked send map attempting to remove recipient GUID " + currentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void WSHeartbeatManager(Client currentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Null-Values

                if (Config.Heartbeat.IntervalMs <= 0)
                {
                    Log("*** WSHeartbeatManager invalid heartbeat interval, using 1000ms");
                    Config.Heartbeat.IntervalMs = 1000;
                }

                if (currentClient == null)
                {
                    Log("*** WSHeartbeatManager null client supplied");
                    return;
                }

                if (currentClient.ClientWSInterface == null)
                {
                    Log("*** WSHeartbeatManager null websocket supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(currentClient.ClientGUID))
                {
                    Log("*** WSHeartbeatManager null client GUID in supplied client");
                    return;
                }

                #endregion

                #region Variables

                DateTime threadStart = DateTime.Now;
                DateTime lastHeartbeatAttempt = DateTime.Now;
                DateTime lastSuccess = DateTime.Now;
                DateTime lastFailure = DateTime.Now;
                int numConsecutiveFailures = 0;
                bool firstRun = true;

                #endregion

                #region Process

                while (true)
                {
                    #region Sleep

                    if (firstRun)
                    {
                        firstRun = false;
                    }
                    else
                    {
                        Task.Delay(Config.Heartbeat.IntervalMs).Wait();
                    }

                    #endregion
                    
                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message heartbeatMessage = MsgBuilder.HeartbeatRequest(currentClient);
                    if (Config.Debug.SendHeartbeat) Log("WSHeartbeatManager sending heartbeat to " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID);

                    bool success = Helper.WSMessageWrite(currentClient.ClientHTTPContext, currentClient.ClientWSInterface, heartbeatMessage, Config.Debug.MsgResponseTime).Result;
                    if (!success)
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** WSHeartbeatManager failed to send heartbeat to client " + currentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** WSHeartbeatManager maximum number of failed heartbeats reached, abandoning client " + currentClient.IpPort());
                            return;
                        }
                    }
                    else
                    {
                        numConsecutiveFailures = 0;
                        lastSuccess = DateTime.Now;
                    }

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("WSHeartbeatManager (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("WSHeartbeatManager (null)", e);
                }
            }
            finally
            {
                List<Channel> affectedChannels = null;
                ConnMgr.RemoveClient(currentClient.IpPort());
                ChannelMgr.RemoveClientChannels(currentClient.ClientGUID, out affectedChannels);
                ChannelDestroyEvent(affectedChannels);
                ChannelMgr.RemoveClient(currentClient.IpPort());
                if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(currentClient));
            }
        }

        #endregion

        #region Websocket-SSL-Server

        private void WSSSLAcceptConnections()
        {
            try
            {
                #region Accept-WS-SSL-Connections

                WSSSLListener.Start();
                while (!WSSSLCancellationToken.IsCancellationRequested)
                {
                    HttpListenerContext context = WSSSLListener.GetContextAsync().Result;

                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string clientIp = context.Request.RemoteEndPoint.Address.ToString();
                        int clientPort = context.Request.RemoteEndPoint.Port;
                        Log("WSSSLAcceptConnections accepted connection from " + clientIp + ":" + clientPort);

                        #endregion

                        #region Authenticate

                        //
                        //
                        // This is implemented by binding the certificate to the port
                        //
                        //

                        #endregion

                        #region Increment-Counters

                        WSSSLActiveConnectionThreads++;

                        //
                        //
                        // Do not decrement in this block, decrement is done by the connection reader
                        //
                        //

                        #endregion

                        #region Get-Websocket-Context

                        WebSocketContext wsContext = null;
                        try
                        {
                            wsContext = context.AcceptWebSocketAsync(subProtocol: null).Result;
                        }
                        catch (Exception)
                        {
                            Log("*** WSSSLAcceptConnections exception while gathering websocket context for client " + clientIp + ":" + clientPort);
                            context.Response.StatusCode = 500;
                            context.Response.Close();
                            return;
                        }

                        WebSocket client = wsContext.WebSocket;

                        #endregion

                        #region Add-to-Client-List

                        Client currentClient = new Client();
                        currentClient.SourceIP = clientIp;
                        currentClient.SourcePort = clientPort;
                        currentClient.ClientTCPInterface = null;
                        currentClient.ClientTCPSSLInterface = null;
                        currentClient.ClientHTTPContext = null;
                        currentClient.ClientWSContext = null;
                        currentClient.ClientWSInterface = null;
                        currentClient.ClientHTTPSSLContext = context;
                        currentClient.ClientWSSSLContext = wsContext;
                        currentClient.ClientWSSSLInterface = client;
                        currentClient.MessageQueue = new BlockingCollection<Message>();

                        currentClient.IsTCP = false;
                        currentClient.IsTCPSSL = false;
                        currentClient.IsWebsocket = false;
                        currentClient.IsWebsocketSSL = true;
                        currentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        currentClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        ConnMgr.AddClient(currentClient);

                        #endregion

                        #region Start-Data-Receiver

                        Log("WSSSLAcceptConnections starting data receiver for " + currentClient.IpPort() + " (now " + WSSSLActiveConnectionThreads + " connections active)");
                        currentClient.DataReceiverTokenSource = new CancellationTokenSource();
                        currentClient.DataReceiverToken = currentClient.DataReceiverTokenSource.Token;
                        Task.Run(() => WSSSLDataReceiver(currentClient), currentClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        currentClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        currentClient.ProcessClientQueueToken = currentClient.ProcessClientQueueTokenSource.Token;
                        Log("WSSSLAcceptConnections starting queue processor for " + currentClient.IpPort());
                        Task.Run(() => ProcessClientQueue(currentClient), currentClient.ProcessClientQueueToken);

                        #endregion
                        
                    }, WSSSLCancellationToken);
                }

                #endregion
            }
            catch (Exception e)
            {
                LogException("WSSSLAcceptConnections", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private void WSSSLDataReceiver(Client currentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** WSSSLDataReceiver null client supplied");
                    return;
                }

                if (currentClient.ClientWSSSLInterface == null)
                {
                    Log("*** WSSSLDataReceiver null WebSocket supplied within client");
                    return;
                }

                #endregion

                #region Wait-for-Data

                while (true)
                {
                    #region Retrieve-Message

                    Message currentMessage = Helper.WSMessageRead(currentClient.ClientHTTPSSLContext, currentClient.ClientWSSSLInterface, Config.Debug.MsgResponseTime).Result;
                    if (currentMessage == null)
                    {
                        Log("WSSSLDataReceiver unable to read message from client " + currentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSSSLDataReceiver received message from " + currentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();

                        Task.Run(() => MessageReceived(currentMessage));
                    }

                    if (!currentMessage.IsValid())
                    {
                        Log("WSSSLDataReceiver invalid message received from client " + currentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSSSLDataReceiver verified message validity from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(currentClient, currentMessage);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSSSLDataReceiver processed message from " + currentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(currentMessage));

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("WSSSLDataReceiver (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("WSSSLDataReceiver (null)", e);
                }
            }
            finally
            {
                WSSSLActiveConnectionThreads--;
                Log("WSSSLDataReceiver closed data receiver for " + currentClient.IpPort() + " (now " + WSSSLActiveConnectionThreads + " connections active)");

                currentClient.Close();
            }
        }

        private bool WSSSLDataSender(Client currentClient, Message currentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** WSSSLDataSender null client supplied");
                    return false;
                }

                if (currentClient.ClientWSSSLInterface == null)
                {
                    Log("*** WSSSLDataSender null websocket supplied within client object for client " + currentClient.ClientGUID);
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** WSSSLDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.SenderGUID))
                {
                    Log("*** WSSSLDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    Log("*** WSSSLDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion
                
                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(currentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Task.Delay(25).Wait();
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** WSSSLDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** WSSSLDataSender locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                bool success = Helper.WSMessageWrite(currentClient.ClientHTTPSSLContext, currentClient.ClientWSSSLInterface, currentMessage, Config.Debug.MsgResponseTime).Result;
                if (!success)
                {
                    Log("WSSSLDataSender unable to send data to client " + currentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(currentMessage.Command))
                    {
                        Log("WSSSLDataSender successfully sent data to client " + currentClient.IpPort() + " for command " + currentMessage.Command);
                    }
                    else
                    {
                        Log("WSSSLDataSender successfully sent data to client " + currentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("WSSSLDataSender (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("WSSSLDataSender (null)", e);
                }

                return false;
            }
            finally
            {
                //
                // remove active client send lock
                //
                if (locked)
                {
                    DateTime removedVal = DateTime.Now;
                    int removeLoopCount = 0;
                    while (!ClientActiveSendMap.TryRemove(currentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Task.Delay(25).Wait();
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(currentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** WSSSLDataSender locked send map attempting to remove recipient GUID " + currentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void WSSSLHeartbeatManager(Client currentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Null-Values

                if (Config.Heartbeat.IntervalMs <= 0)
                {
                    Log("*** WSSSLHeartbeatManager invalid heartbeat interval, using 1000ms");
                    Config.Heartbeat.IntervalMs = 1000;
                }

                if (currentClient == null)
                {
                    Log("*** WSSSLHeartbeatManager null client supplied");
                    return;
                }

                if (currentClient.ClientWSSSLInterface == null)
                {
                    Log("*** WSSSLHeartbeatManager null websocket supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(currentClient.ClientGUID))
                {
                    Log("*** WSSSLHeartbeatManager null client GUID in supplied client");
                    return;
                }

                #endregion

                #region Variables

                DateTime threadStart = DateTime.Now;
                DateTime lastHeartbeatAttempt = DateTime.Now;
                DateTime lastSuccess = DateTime.Now;
                DateTime lastFailure = DateTime.Now;
                int numConsecutiveFailures = 0;
                bool firstRun = true;

                #endregion

                #region Process

                while (true)
                {
                    #region Sleep

                    if (firstRun)
                    {
                        firstRun = false;
                    }
                    else
                    {
                        Task.Delay(Config.Heartbeat.IntervalMs).Wait();
                    }

                    #endregion
                    
                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message heartbeatMessage = MsgBuilder.HeartbeatRequest(currentClient);
                    if (Config.Debug.SendHeartbeat) Log("WSSSLHeartbeatManager sending heartbeat to " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID);

                    bool success = Helper.WSMessageWrite(currentClient.ClientHTTPSSLContext, currentClient.ClientWSSSLInterface, heartbeatMessage, Config.Debug.MsgResponseTime).Result;
                    if (!success)
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** WSSSLHeartbeatManager failed to send heartbeat to client " + currentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** WSSSLHeartbeatManager maximum number of failed heartbeats reached, abandoning client " + currentClient.IpPort());
                            return;
                        }
                    }
                    else
                    {
                        numConsecutiveFailures = 0;
                        lastSuccess = DateTime.Now;
                    }

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    LogException("WSSSLHeartbeatManager (" + currentClient.IpPort() + ")", e);
                }
                else
                {
                    LogException("WSSSLHeartbeatManager (null)", e);
                }
            }
            finally
            {
                List<Channel> affectedChannels = null;
                ConnMgr.RemoveClient(currentClient.IpPort());
                ChannelMgr.RemoveClientChannels(currentClient.ClientGUID, out affectedChannels);
                ChannelDestroyEvent(affectedChannels);
                ChannelMgr.RemoveClient(currentClient.IpPort());
                if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(currentClient));
            }
        }

        #endregion

        #endregion
        
        #region Private-Event-Methods

        private bool ServerJoinEvent(Client currentClient)
        {
            if (currentClient == null)
            {
                Log("*** ServerJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** ServerJoinEvent null ClientGUID suplied within Client");
                return true;
            }

            Log("ServerJoinEvent sending server join notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID);

            List<Client> currentClients = ConnMgr.GetClients(); 
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** ServerJoinEvent no clients found on server");
                return true;
            }

            Message msg = MsgBuilder.ServerJoinEvent(currentClient);

            foreach (Client curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, currentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            Log("*** ServerJoinEvent error queuing server join event to " + msg.RecipientGUID + " (join by " + currentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ServerLeaveEvent(Client currentClient)
        {
            if (currentClient == null)
            {
                Log("*** ServerLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** ServerLeaveEvent null ClientGUID suplied within Client");
                return true;
            }

            Log("ServerLeaveEvent sending server leave notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID);

            List<Client> currentClients = ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** ServerLeaveEvent no clients found on server");
                return true;
            }

            Message msg = MsgBuilder.ServerLeaveEvent(currentClient);

            foreach (Client curr in currentClients)
            {
                if (!String.IsNullOrEmpty(curr.ClientGUID))
                {
                    if (String.Compare(curr.ClientGUID, currentClient.ClientGUID) != 0)
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            Log("*** ServerLeaveEvent error queuing server leave event to " + msg.RecipientGUID + " (leave by " + currentClient.ClientGUID + ")");
                        }
                        else
                        {
                            Log("ServerLeaveEvent queued server leave event to " + msg.RecipientGUID + " (leave by " + currentClient.ClientGUID + ")");
                        }
                    }
                }
            }

            return true;
        }

        private bool ChannelJoinEvent(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                Log("*** ChannelJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** ChannelJoinEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                Log("*** ChannelJoinEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                Log("*** ChannelJoinEvent null GUID supplied within Channel");
                return true;
            }

            Log("ChannelJoinEvent sending channel join notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** ChannelJoinEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = MsgBuilder.ChannelJoinEvent(currentChannel, currentClient);

            foreach (Client curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, currentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            Log("*** ChannelJoinEvent error queuing channel join event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (join by " + currentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ChannelLeaveEvent(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                Log("*** ChannelLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** ChannelLeaveEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                Log("*** ChannelLeaveEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                Log("*** ChannelLeaveEvent null GUID supplied within Channel");
                return true;
            }

            Log("ChannelLeaveEvent sending channel leave notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** ChannelLeaveEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = MsgBuilder.ChannelLeaveEvent(currentChannel, currentClient);

            foreach (Client curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, currentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            Log("*** ChannelLeaveEvent error queuing channel leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ChannelCreateEvent(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                Log("*** ChannelCreateEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** ChannelCreateEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                Log("*** ChannelCreateEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                Log("*** ChannelCreateEvent null GUID supplied within Channel");
                return true;
            }

            if (Helper.IsTrue(currentChannel.Private))
            {
                Log("*** ChannelCreateEvent skipping create notification for channel " + currentChannel.ChannelGUID + " (private)");
                return true;
            }

            Log("ChannelCreateEvent sending channel create notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** ChannelCreateEvent no clients found on server");
                return true;
            }

            foreach (Client curr in currentClients)
            {
                Task.Run(() =>
                {
                    Message msg = MsgBuilder.ChannelCreateEvent(currentClient, currentChannel);
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                    if (!responseSuccess)
                    {
                        Log("*** ChannelCreateEvent error queuing channel create event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
                    }
                });
            }

            return true;
        }

        private bool ChannelDestroyEvent(List<Channel> channels)
        {
            if (channels == null || channels.Count < 1) return false;
            foreach (Channel currChannel in channels)
            {
                if (currChannel.Members != null && currChannel.Members.Count > 0)
                {
                    foreach (Client currMember in currChannel.Members)
                    {
                        Task.Run(() =>
                        {
                            Message msg = MsgBuilder.ChannelDestroyEvent(currMember, currChannel);
                            msg.RecipientGUID = currMember.ClientGUID;
                            bool responseSuccess = SendSystemMessage(msg);
                            if (!responseSuccess)
                            {
                                Log("*** ChannelDestroyEvent error sending channel destroy event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currChannel.OwnerGUID + ")");
                            }
                        });
                    }
                }

                if (currChannel.Subscribers != null && currChannel.Subscribers.Count > 0)
                {
                    foreach (Client currSubscriber in currChannel.Subscribers)
                    {
                        Task.Run(() =>
                        {
                            Message msg = MsgBuilder.ChannelDestroyEvent(currSubscriber, currChannel);
                            msg.RecipientGUID = currSubscriber.ClientGUID;
                            bool responseSuccess = SendSystemMessage(msg);
                            if (!responseSuccess)
                            {
                                Log("*** ChannelDestroyEvent error sending channel destroy event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currChannel.OwnerGUID + ")");
                            }
                        });
                    }
                }
            }

            return true;
        }

        private bool ChannelDestroyEvent(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                Log("*** ChannelDestroyEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** ChannelDestroyEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                Log("*** ChannelDestroyEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                Log("*** ChannelDestroyEvent null GUID supplied within Channel");
                return true;
            }

            if (Helper.IsTrue(currentChannel.Private))
            {
                Log("*** ChannelDestroyEvent skipping destroy notification for channel " + currentChannel.ChannelGUID + " (private)");
                return true;
            }

            Log("ChannelDestroyEvent sending channel destroy notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** ChannelDestroyEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            foreach (Client curr in currentClients)
            {
                Task.Run(() =>
                {
                    Message msg = MsgBuilder.ChannelDestroyEvent(currentClient, currentChannel);
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                    if (!responseSuccess)
                    {
                        Log("*** ChannelDestroyEvent error queuing channel leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
                    }
                });
            }

            return true;
        }

        private bool SubscriberJoinEvent(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                Log("*** SubscriberJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** SubscriberJoinEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                Log("*** SubscriberJoinEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                Log("*** SubscriberJoinEvent null GUID supplied within Channel");
                return true;
            }

            Log("SubscriberJoinEvent sending subcriber join notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** SubscriberJoinEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = MsgBuilder.ChannelSubscriberJoinEvent(currentChannel, currentClient);

            foreach (Client curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, currentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            Log("*** SubscriberJoinEvent error queuing subscriber join event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (join by " + currentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }
         
        private bool SubscriberLeaveEvent(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                Log("*** SubscriberLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                Log("*** SubscriberLeaveEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                Log("*** SubscriberLeaveEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                Log("*** SubscriberLeaveEvent null GUID supplied within Channel");
                return true;
            }

            Log("SubscriberLeaveEvent sending subscriber leave notification for " + currentClient.IpPort() + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                Log("*** SubscriberLeaveEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = MsgBuilder.ChannelSubscriberLeaveEvent(currentChannel, currentClient);

            foreach (Client curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, currentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            Log("*** SubscriberLeaveEvent error queuing subscriber leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        #endregion

        #region Private-Authorization-Methods

        private void MonitorUsersFile()
        {
            try
            {
                bool firstRun = true;

                while (true)
                {
                    #region Wait

                    if (!firstRun)
                    {
                        Task.Delay(5000).Wait();
                    }
                    else
                    {
                        firstRun = false;
                    }

                    #endregion

                    #region Check-if-Exists

                    if (!File.Exists(Config.Files.UsersFile))
                    {
                        UsersList = new ConcurrentList<User>();
                        continue;
                    }

                    #endregion

                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(UsersLastModified))
                    {
                        #region First-Read

                        Log("MonitorUsersFile loading " + Config.Files.UsersFile);

                        //
                        // get timestamp
                        //
                        UsersLastModified = File.GetLastWriteTimeUtc(Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(Config.Files.UsersFile);
                        if (String.IsNullOrEmpty(fileContents))
                        {
                            Log("*** MonitorUsersFile empty file found at " + Config.Files.UsersFile);
                            continue;
                        }

                        try
                        {
                            UsersList = Helper.DeserializeJson<ConcurrentList<User>>(Encoding.UTF8.GetBytes(fileContents), false);
                        }
                        catch (Exception EInner)
                        {
                            LogException("MonitorUsersFile", EInner);
                            Log("*** MonitorUsersFile unable to deserialize contents of " + Config.Files.UsersFile);
                            continue;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Subsequent-Read

                        //
                        // get timestamp
                        //
                        tempTimestamp = File.GetLastWriteTimeUtc(Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                        //
                        // compare and update
                        //
                        if (String.Compare(UsersLastModified, tempTimestamp) != 0)
                        {
                            Log("MonitorUsersFile loading " + Config.Files.UsersFile);

                            //
                            // get timestamp
                            //
                            UsersLastModified = File.GetLastWriteTimeUtc(Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(Config.Files.UsersFile);
                            if (String.IsNullOrEmpty(fileContents))
                            {
                                Log("*** MonitorUsersFile empty file found at " + Config.Files.UsersFile);
                                continue;
                            }

                            try
                            {
                                UsersList = Helper.DeserializeJson<ConcurrentList<User>>(Encoding.UTF8.GetBytes(fileContents), false);
                            }
                            catch (Exception EInner)
                            {
                                LogException("MonitorUsersFile", EInner);
                                Log("*** MonitorUsersFile unable to deserialize contents of " + Config.Files.UsersFile);
                                continue;
                            }
                        }

                        #endregion
                    }

                    #endregion
                }
            }
            catch (Exception e)
            {
                LogException("MonitorUsersFile", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private void MonitorPermissionsFile()
        {
            try
            {
                bool firstRun = true;

                while (true)
                {
                    #region Wait

                    if (!firstRun)
                    {
                        Task.Delay(5000).Wait();
                    }
                    else
                    {
                        firstRun = false;
                    }

                    #endregion

                    #region Check-if-Exists

                    if (!File.Exists(Config.Files.PermissionsFile))
                    {
                        PermissionsList = new ConcurrentList<Permission>();
                        continue;
                    }

                    #endregion

                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(PermissionsLastModified))
                    {
                        #region First-Read

                        Log("MonitorPermissionsFile loading " + Config.Files.PermissionsFile);

                        //
                        // get timestamp
                        //
                        PermissionsLastModified = File.GetLastWriteTimeUtc(Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(Config.Files.PermissionsFile);
                        if (String.IsNullOrEmpty(fileContents))
                        {
                            Log("*** MonitorPermissionsFile empty file found at " + Config.Files.PermissionsFile);
                            continue;
                        }

                        try
                        {
                            PermissionsList = Helper.DeserializeJson<ConcurrentList<Permission>>(Encoding.UTF8.GetBytes(fileContents), false);
                        }
                        catch (Exception EInner)
                        {
                            LogException("MonitorPermissionsFile", EInner);
                            Log("*** MonitorPermissionsFile unable to deserialize contents of " + Config.Files.PermissionsFile);
                            continue;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Subsequent-Read

                        //
                        // get timestamp
                        //
                        tempTimestamp = File.GetLastWriteTimeUtc(Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                        //
                        // compare and update
                        //
                        if (String.Compare(PermissionsLastModified, tempTimestamp) != 0)
                        {
                            Log("MonitorPermissionsFile loading " + Config.Files.PermissionsFile);

                            //
                            // get timestamp
                            //
                            PermissionsLastModified = File.GetLastWriteTimeUtc(Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(Config.Files.PermissionsFile);
                            if (String.IsNullOrEmpty(fileContents))
                            {
                                Log("*** MonitorPermissionsFile empty file found at " + Config.Files.PermissionsFile);
                                continue;
                            }

                            try
                            {
                                PermissionsList = Helper.DeserializeJson<ConcurrentList<Permission>>(Encoding.UTF8.GetBytes(fileContents), false);
                            }
                            catch (Exception EInner)
                            {
                                LogException("MonitorPermissionsFile", EInner);
                                Log("*** MonitorPermissionsFile unable to deserialize contents of " + Config.Files.PermissionsFile);
                                continue;
                            }
                        }

                        #endregion
                    }

                    #endregion
                }
            }
            catch (Exception e)
            {
                LogException("MonitorPermissionsFile", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private bool AllowConnection(string email, string ip)
        {
            try
            {
                if (UsersList != null && UsersList.Count > 0)
                {
                    #region Check-for-Null-Values

                    if (String.IsNullOrEmpty(email))
                    {
                        Log("*** AllowConnection no email supplied");
                        return false;
                    }

                    if (String.IsNullOrEmpty(ip))
                    {
                        Log("*** AllowConnection no IP supplied");
                        return false;
                    }

                    #endregion

                    #region Users-List-Present

                    User currUser = GetUser(email);
                    if (currUser == null)
                    {
                        Log("*** AllowConnection unable to find entry for email " + email);
                        return false;
                    }

                    if (String.IsNullOrEmpty(currUser.Permission))
                    {
                        #region No-Permissions-Only-Check-IP

                        if (currUser.IPWhiteList == null || currUser.IPWhiteList.Count < 1)
                        {
                            // deault permit
                            return true;
                        }
                        else
                        {
                            if (currUser.IPWhiteList.Contains(ip)) return true;
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Check-Permissions-Object

                        Permission currPermission = GetPermission(currUser.Permission);
                        if (currPermission == null)
                        {
                            Log("*** AllowConnection permission entry " + currUser.Permission + " not found for user " + email);
                            return false;
                        }

                        if (!currPermission.Login)
                        {
                            Log("*** AllowConnection login permission denied in permission entry " + currUser.Permission + " for user " + email);
                            return false;
                        }

                        #endregion

                        #region Check-IP

                        if (currUser.IPWhiteList == null || currUser.IPWhiteList.Count < 1)
                        {
                            // deault permit
                            return true;
                        }
                        else
                        {
                            if (currUser.IPWhiteList.Contains(ip)) return true;
                            return false;
                        }

                        #endregion
                    }

                    #endregion
                }
                else
                {
                    #region Default-Permit

                    return true;

                    #endregion
                }
            }
            catch (Exception e)
            {
                LogException("AllowConnection", e);
                return false;
            }
        }

        private User GetUser(string email)
        {
            try
            {
                #region Check-for-Null-Values

                if (UsersList == null || UsersList.Count < 1) return null;

                if (String.IsNullOrEmpty(email))
                {
                    Log("*** GetUser null email supplied");
                    return null;
                }

                #endregion

                #region Process

                foreach (User currUser in UsersList)
                {
                    if (String.IsNullOrEmpty(currUser.Email)) continue;
                    if (String.Compare(currUser.Email.ToLower(), email.ToLower()) == 0)
                    {
                        return currUser;
                    }
                }

                Log("*** GetUser unable to find email " + email);
                return null;

                #endregion
            }
            catch (Exception e)
            {
                LogException("GetUser", e);
                return null;
            }
        }

        private Permission GetUserPermission(string email)
        {
            try
            {
                #region Check-for-Null-Values

                if (PermissionsList == null || PermissionsList.Count < 1) return null;
                if (UsersList == null || UsersList.Count < 1) return null;

                if (String.IsNullOrEmpty(email))
                {
                    Log("*** GetUserPermissions null email supplied");
                    return null;
                }

                #endregion

                #region Process

                User currUser = GetUser(email);
                if (currUser == null)
                {
                    Log("*** GetUserPermission unable to find user " + email);
                    return null;
                }

                if (String.IsNullOrEmpty(currUser.Permission)) return null;
                return GetPermission(currUser.Permission);

                #endregion
            }
            catch (Exception e)
            {
                LogException("GetUserPermissions", e);
                return null;
            }
        }

        private Permission GetPermission(string permission)
        {
            try
            {
                #region Check-for-Null-Values

                if (PermissionsList == null || PermissionsList.Count < 1) return null;
                if (String.IsNullOrEmpty(permission))
                {
                    Log("*** GetPermission null permission supplied");
                    return null;
                }

                #endregion

                #region Process

                foreach (Permission currPermission in PermissionsList)
                {
                    if (String.IsNullOrEmpty(currPermission.Name)) continue;
                    if (String.Compare(permission.ToLower(), currPermission.Name.ToLower()) == 0)
                    {
                        return currPermission;
                    }
                }

                Log("*** GetPermission permission " + permission + " not found");
                return null;

                #endregion
            }
            catch (Exception e)
            {
                LogException("GetPermission", e);
                return null;
            }
        }

        private bool AuthorizeMessage(Message currentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (currentMessage == null)
                {
                    Log("*** AuthorizeMessage null message supplied");
                    return false;
                }

                if (UsersList == null || UsersList.Count < 1)
                {
                    // default permit
                    return true;
                }

                #endregion

                #region Process

                if (!String.IsNullOrEmpty(currentMessage.Email))
                {
                    #region Authenticate-Credentials

                    User currUser = GetUser(currentMessage.Email);
                    if (currUser == null)
                    {
                        Log("*** AuthenticateUser unable to find user " + currentMessage.Email);
                        return false;
                    }

                    if (!String.IsNullOrEmpty(currUser.Password))
                    {
                        if (String.Compare(currUser.Password, currentMessage.Password) != 0)
                        {
                            Log("*** AuthenticateUser invalid password supplied for user " + currentMessage.Email);
                            return false;
                        }
                    }

                    #endregion

                    #region Verify-Permissions

                    if (String.IsNullOrEmpty(currUser.Permission))
                    {
                        // default permit
                        // Log("AuthenticateUser default permit in use (user " + CurrentMessage.Email + " has null permission list)");
                        return true;
                    }

                    if (String.IsNullOrEmpty(currentMessage.Command))
                    {
                        // default permit
                        // Log("AuthenticateUser default permit in use (user " + CurrentMessage.Email + " sending message with no command)");
                        return true;
                    }

                    Permission currPermission = GetPermission(currUser.Permission);
                    if (currPermission == null)
                    {
                        Log("*** AuthorizeMessage unable to find permission " + currUser.Permission + " for user " + currUser.Email);
                        return false;
                    }

                    if (currPermission.Permissions == null || currPermission.Permissions.Count < 1)
                    {
                        // default permit
                        // Log("AuthorizeMessage default permit in use (no permissions found for permission name " + currUser.Permission);
                        return true;
                    }

                    if (currPermission.Permissions.Contains(currentMessage.Command))
                    {
                        // Log("AuthorizeMessage found permission for command " + CurrentMessage.Command + " in permission " + currUser.Permission + " for user " + currUser.Email);
                        return true;
                    }
                    else
                    {
                        Log("*** AuthorizeMessage permission " + currPermission.Name + " does not contain command " + currentMessage.Command + " for user " + currUser.Email);
                        return false;
                    }

                    #endregion
                }
                else
                {
                    #region No-Material

                    Log("*** AuthenticateUser no authentication material supplied");
                    return false;

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                LogException("AuthorizeMessage", e);
                return false;
            }
        }

        private List<User> GetCurrentUsersFile()
        {
            if (UsersList == null || UsersList.Count < 1)
            {
                Log("*** GetCurrentUsersFile no users listed or no users file");
                return null;
            }

            List<User> ret = new List<User>();
            foreach (User curr in UsersList)
            {
                ret.Add(curr);
            }

            Log("GetCurrentUsersFile returning " + ret.Count + " users");
            return ret;
        }

        private List<Permission> GetCurrentPermissionsFile()
        {
            if (PermissionsList == null || PermissionsList.Count < 1)
            {
                Log("*** GetCurrentPermissionsFile no permissions listed or no permissions file");
                return null;
            }

            List<Permission> ret = new List<Permission>();
            foreach (Permission curr in PermissionsList)
            {
                ret.Add(curr);
            }

            Log("GetCurrentPermissionsFile returning " + ret.Count + " permissions");
            return ret;
        }

        #endregion

        #region Private-Cleanup-Tasks

        private void CleanupTask()
        {
            try
            {
                bool firstRun = true;

                while (true)
                {
                    #region Wait

                    if (!firstRun)
                    {
                        Task.Delay(5000).Wait();
                    }
                    else
                    {
                        firstRun = false;
                    }

                    #endregion
                    
                    #region Process

                    foreach (KeyValuePair<string, DateTime> curr in ClientActiveSendMap)
                    {
                        if (String.IsNullOrEmpty(curr.Key)) continue;
                        if (DateTime.Compare(DateTime.Now.ToUniversalTime(), curr.Value) > 0)
                        {
                            Task.Run(() =>
                            {
                                int elapsed = 0;
                                while (true)
                                {
                                    Log("CleanupTask attempting to remove active send map for " + curr.Key + " (elapsed " + elapsed + "ms)");
                                    if (!ClientActiveSendMap.ContainsKey(curr.Key))
                                    {
                                        Log("CleanupTask key " + curr.Key + " no longer present in active send map, exiting");
                                        break;
                                    }
                                    else
                                    {
                                        DateTime removedVal = DateTime.Now;
                                        if (ClientActiveSendMap.TryRemove(curr.Key, out removedVal))
                                        {
                                            Log("CleanupTask key " + curr.Key + " removed by cleanup task, exiting");
                                            break;
                                        }
                                        Task.Delay(1000).Wait();
                                        elapsed += 1000;
                                    }
                                }
                            });
                        }
                    }
                    
                    #endregion
                }
            }
            catch (Exception e)
            {
                LogException("CleanupTask", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        #endregion

        #region Private-Locked-Methods

        //
        // Ensure that none of these methods call another method within this region
        // otherwise you have a lock within a lock!  There should be NO methods
        // outside of this region that have a lock statement
        //
        
        private Dictionary<string, DateTime> GetAllClientActiveSendMap()
        {
            if (ClientActiveSendMap == null || ClientActiveSendMap.Count < 1) return new Dictionary<string, DateTime>();
            Dictionary<string, DateTime> ret = ClientActiveSendMap.ToDictionary(entry => entry.Key, entry => entry.Value);
            return ret;
        }
        
        private bool AddChannel(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                Log("*** AddChannel null client supplied");
                return false;
            }

            if (currentChannel == null)
            {
                Log("*** AddChannel null channel supplied");
                return false;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID)) currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(currentChannel.ChannelName)) currentChannel.ChannelName = currentChannel.ChannelGUID;

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            if (currentChannel.CreatedUTC == null) currentChannel.CreatedUTC = timestamp;
            if (currentChannel.UpdatedUTC == null) currentChannel.UpdatedUTC = timestamp;
            currentChannel.Members = new List<Client>();
            currentChannel.Members.Add(currentClient);
            currentChannel.Subscribers = new List<Client>();
            currentChannel.OwnerGUID = currentClient.ClientGUID;

            if (ChannelMgr.ChannelExists(currentChannel.ChannelGUID))
            {
                Log("*** AddChannel channel GUID " + currentChannel.ChannelGUID + " already exists");
                return false;
            }

            ChannelMgr.AddChannel(currentChannel);

            Log("AddChannel successfully added channel with GUID " + currentChannel.ChannelGUID + " for client " + currentChannel.OwnerGUID);
            return true;
        }

        private bool RemoveChannel(Channel currentChannel)
        {
            currentChannel = ChannelMgr.GetChannelByGUID(currentChannel.ChannelGUID);
            if (currentChannel == null)
            {
                Log("*** RemoveChannel unable to find specified channel");
                return false;
            }
                
            if (String.Compare(currentChannel.OwnerGUID, ServerGUID) == 0)
            {
                Log("RemoveChannel skipping removal of channel " + currentChannel.ChannelGUID + " (server channel)");
                return true;
            }

            ChannelMgr.RemoveChannel(currentChannel.ChannelGUID);
            Log("RemoveChannel notifying channel members of channel removal");

            if (currentChannel.Members != null)
            {
                if (currentChannel.Members.Count > 0)
                {
                    //
                    // create another reference in case list is modified
                    //
                    Channel tempChannel = currentChannel;
                    List<Client> tempMembers = new List<Client>(currentChannel.Members);

                    Task.Run(() =>
                    {
                        foreach (Client Client in tempMembers)
                        {
                            if (String.Compare(Client.ClientGUID, currentChannel.OwnerGUID) != 0)
                            {
                                Log("RemoveChannel notifying channel " + tempChannel.ChannelGUID + " member " + Client.ClientGUID + " of channel deletion by owner");
                                SendSystemMessage(MsgBuilder.ChannelDeletedByOwner(Client, tempChannel));
                            }
                        }
                    }
                    );
                }
            }

            Log("RemoveChannel removed channel " + currentChannel.ChannelGUID + " successfully");
            return true;
        }

        private bool AddChannelMember(Client currentClient, Channel currentChannel)
        {
            if (ChannelMgr.AddChannelMember(currentChannel, currentClient))
            {
                if (Config.Notification.ChannelJoinNotification)
                {
                    ChannelJoinEvent(currentClient, currentChannel);
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private bool AddChannelSubscriber(Client currentClient, Channel currentChannel)
        {
            if (ChannelMgr.AddChannelSubscriber(currentChannel, currentClient))
            {
                if (Config.Notification.ChannelJoinNotification)
                {
                    SubscriberJoinEvent(currentClient, currentChannel);
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private bool RemoveChannelMember(Client currentClient, Channel currentChannel)
        {
            if (ChannelMgr.RemoveChannelMember(currentChannel, currentClient))
            {
                #region Send-Notifications

                if (Config.Notification.ChannelJoinNotification)
                {
                    List<Client> curr = ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
                    if (curr != null && currentChannel.Members != null && currentChannel.Members.Count > 0)
                    {
                        foreach (Client c in curr)
                        {
                            //
                            // create another reference in case list is modified
                            //
                            Channel TempChannel = currentChannel;
                            Task.Run(() =>
                            {
                                Log("RemoveChannelMember notifying channel " + TempChannel.ChannelGUID + " member " + c.ClientGUID + " of channel leave by member " + currentClient.ClientGUID);
                                SendSystemMessage(MsgBuilder.ChannelLeaveEvent(TempChannel, currentClient));
                            }
                            );
                        }
                    }
                }

                return true;

                #endregion
            }
            else
            {
                return false;
            }
        }

        private bool RemoveChannelSubscriber(Client currentClient, Channel currentChannel)
        {
            if (ChannelMgr.RemoveChannelMember(currentChannel, currentClient))
            {
                #region Send-Notifications

                if (Config.Notification.ChannelJoinNotification)
                {
                    List<Client> curr = ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
                    if (curr != null && currentChannel.Members != null && currentChannel.Members.Count > 0)
                    {
                        foreach (Client c in curr)
                        {
                            //
                            // create another reference in case list is modified
                            //
                            Channel tempChannel = currentChannel;
                            Task.Run(() =>
                            {
                                Log("RemoveChannelSubscriber notifying channel " + tempChannel.ChannelGUID + " member " + c.ClientGUID + " of channel leave by subscriber " + currentClient.ClientGUID);
                                SendSystemMessage(MsgBuilder.ChannelSubscriberLeaveEvent(tempChannel, currentClient));
                            }
                            );
                        }
                    }
                }

                return true;

                #endregion
            }
            else
            {
                return false;
            }
        }

        private bool IsChannelMember(Client currentClient, Channel currentChannel)
        {
            return ChannelMgr.IsChannelMember(currentClient, currentChannel);
        }

        private bool IsChannelSubscriber(Client currentClient, Channel currentChannel)
        {
            return ChannelMgr.IsChannelSubscriber(currentClient, currentChannel);
        }

        #endregion

        #region Private-Message-Processing-Methods
        
        private Channel BuildChannelFromMessageData(Client currentClient, Message currentMessage)
        {
            if (currentClient == null)
            {
                Log("*** BuildChannelFromMessageData null client supplied");
                return null;
            }

            if (currentMessage == null)
            {
                Log("*** BuildChannelFromMessageData null channel supplied");
                return null;
            }

            if (currentMessage.Data == null)
            {
                Log("*** BuildChannelFromMessageData null data supplied in message");
                return null;
            }

            Channel ret = null;
            try
            {
                ret = Helper.DeserializeJson<Channel>(currentMessage.Data, false);
            }
            catch (Exception e)
            {
                LogException("BuildChannelFromMessageData", e);
                ret = null;
            }

            if (ret == null)
            {
                Log("*** BuildChannelFromMessageData unable to convert message body to Channel object");
                return null;
            }

            // assume ret.Private is set in the request
            if (ret.Private == default(int)) ret.Private = 0;

            if (String.IsNullOrEmpty(ret.ChannelGUID)) ret.ChannelGUID = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(ret.ChannelName)) ret.ChannelName = ret.ChannelGUID;
            ret.CreatedUTC = DateTime.Now.ToUniversalTime();
            ret.UpdatedUTC = ret.CreatedUTC;
            ret.OwnerGUID = currentClient.ClientGUID;
            ret.Members = new List<Client>();
            ret.Members.Add(currentClient);
            ret.Subscribers = new List<Client>();
            return ret;
        }

        private bool MessageProcessor(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    Log("*** MessageProcessor null client supplied");
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** MessageProcessor null message supplied");
                    return false;
                }

                #endregion

                #region Variables-and-Initialization

                Client currentRecipient = null;
                Channel currentChannel = null;
                Message responseMessage = new Message();
                bool responseSuccess = false;
                currentMessage.Success = null;

                #endregion

                #region Verify-Client-GUID-Present

                if (String.IsNullOrEmpty(currentClient.ClientGUID))
                {
                    if (!String.IsNullOrEmpty(currentMessage.Command))
                    {
                        if (String.Compare(currentMessage.Command.ToLower(), "login") != 0)
                        {
                            #region Null-GUID-and-Not-Login

                            Log("*** MessageProcessor received message from client with no GUID");
                            responseSuccess = QueueClientMessage(currentClient, MsgBuilder.LoginRequired());
                            if (!responseSuccess)
                            {
                                Log("*** MessageProcessor unable to queue login required message to client " + currentClient.IpPort());
                            }
                            return responseSuccess;

                            #endregion
                        }
                    }
                }
                else
                {
                    #region Ensure-GUID-Exists

                    if (String.Compare(currentClient.ClientGUID, ServerGUID) != 0)
                    {
                        //
                        // All zeros is the BigQ server
                        //
                        Client verifyClient = ConnMgr.GetClientByGUID(currentClient.ClientGUID);
                        if (verifyClient == null)
                        {
                            Log("*** MessageProcessor received message from unknown client GUID " + currentClient.ClientGUID + " from " + currentClient.IpPort());
                            responseSuccess = QueueClientMessage(currentClient, MsgBuilder.LoginRequired());
                            if (!responseSuccess)
                            {
                                Log("*** MessageProcessor unable to queue login required message to client " + currentClient.IpPort());
                            }
                            return responseSuccess;
                        }
                    }

                    #endregion
                }

                #endregion

                #region Verify-Transport-Objects-Present

                if (String.Compare(currentClient.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (currentClient.IsTCP)
                    {
                        #region TCP

                        if (currentClient.ClientTCPInterface == null)
                        {
                            Log("*** MessageProcessor null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (currentClient.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (currentClient.ClientTCPSSLInterface == null)
                        {
                            Log("*** MessageProcessor null TCP SSL client within supplied client");
                            return false;
                        }

                        if (currentClient.ClientSSLStream == null)
                        {
                            Log("*** MessageProcessor null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (currentClient.IsWebsocket)
                    {
                        #region Websocket

                        if (currentClient.ClientHTTPContext == null)
                        {
                            Log("*** MessageProcessor null HTTP context within supplied client");
                            return false;
                        }

                        if (currentClient.ClientWSContext == null)
                        {
                            Log("*** MessageProcessor null websocket context witin supplied client");
                            return false;
                        }

                        if (currentClient.ClientWSInterface == null)
                        {
                            Log("*** MessageProcessor null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (currentClient.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (currentClient.ClientHTTPSSLContext == null)
                        {
                            Log("*** MessageProcessor null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (currentClient.ClientWSSSLContext == null)
                        {
                            Log("*** MessageProcessor null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (currentClient.ClientWSSSLInterface == null)
                        {
                            Log("*** MessageProcessor null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** MessageProcessor unknown transport for supplied client " + currentClient.IpPort() + " " + currentClient.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                #endregion

                #region Authorize-Message

                if (!AuthorizeMessage(currentMessage))
                {
                    if (String.IsNullOrEmpty(currentMessage.Command))
                    {
                        Log("*** MessageProcessor unable to authenticate or authorize message from " + currentMessage.Email + " " + currentMessage.SenderGUID);
                    }
                    else
                    {
                        Log("*** MessageProcessor unable to authenticate or authorize message of type " + currentMessage.Command + " from " + currentMessage.Email + " " + currentMessage.SenderGUID);
                    }

                    responseMessage = MsgBuilder.AuthorizationFailed(currentMessage);
                    responseSuccess = QueueClientMessage(currentClient, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue authorization failed message to client " + currentClient.IpPort());
                    }
                    return responseSuccess;
                }

                #endregion

                #region Process-Administrative-Messages

                if (!String.IsNullOrEmpty(currentMessage.Command))
                {
                    Log("MessageProcessor processing administrative message of type " + currentMessage.Command + " from client " + currentClient.IpPort());

                    switch (currentMessage.Command.ToLower())
                    {
                        case "echo":
                            responseMessage = ProcessEchoMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "login":
                            responseMessage = ProcessLoginMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "heartbeatrequest":
                            // no need to send response
                            return true;

                        case "joinchannel":
                            responseMessage = ProcessJoinChannelMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "leavechannel":
                            responseMessage = ProcessLeaveChannelMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "subscribechannel":
                            responseMessage = ProcessSubscribeChannelMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "unsubscribechannel":
                            responseMessage = ProcessUnsubscribeChannelMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "createchannel":
                            responseMessage = ProcessCreateChannelMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "deletechannel":
                            responseMessage = ProcessDeleteChannelMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "listchannels":
                            responseMessage = ProcessListChannelsMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "listchannelmembers":
                            responseMessage = ProcessListChannelMembersMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "listchannelsubscribers":
                            responseMessage = ProcessListChannelSubscribersMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "listclients":
                            responseMessage = ProcessListClientsMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        case "isclientconnected":
                            responseMessage = ProcessIsClientConnectedMessage(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;

                        default:
                            responseMessage = MsgBuilder.UnknownCommand(currentClient, currentMessage);
                            responseSuccess = QueueClientMessage(currentClient, responseMessage);
                            return responseSuccess;
                    }
                }

                #endregion

                #region Get-Recipient-or-Channel

                if (!String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    currentRecipient = ConnMgr.GetClientByGUID(currentMessage.RecipientGUID);
                }
                else if (!String.IsNullOrEmpty(currentMessage.ChannelGUID))
                {
                    currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                }
                else
                {
                    #region Recipient-Not-Supplied

                    Log("MessageProcessor no recipient specified either by RecipientGUID or ChannelGUID");
                    responseMessage = MsgBuilder.RecipientNotFound(currentClient, currentMessage);
                    responseSuccess = QueueClientMessage(currentClient, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue recipient not found message to " + currentClient.IpPort());
                    }
                    return false;

                    #endregion
                }

                #endregion

                #region Process-Recipient-Messages

                if (currentRecipient != null)
                {
                    #region Send-to-Recipient

                    responseSuccess = QueueClientMessage(currentRecipient, currentMessage);
                    if (!responseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue to recipient " + currentRecipient.ClientGUID + ", sent failure notification to sender");
                    }

                    return responseSuccess;

                    #endregion
                }
                else if (currentChannel != null)
                {
                    #region Send-to-Channel

                    if (Helper.IsTrue(currentChannel.Broadcast))
                    {
                        #region Broadcast-Message

                        responseSuccess = SendChannelMembersMessage(currentClient, currentChannel, currentMessage);
                        if (!responseSuccess)
                        {
                            Log("*** MessageProcessor unable to send to members in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
                        }

                        return responseSuccess;

                        #endregion
                    }
                    else if (Helper.IsTrue(currentChannel.Multicast))
                    {
                        #region Multicast-Message-to-Subscribers

                        responseSuccess = SendChannelSubscribersMessage(currentClient, currentChannel, currentMessage);
                        if (!responseSuccess)
                        {
                            Log("*** MessageProcessor unable to send to subscribers in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
                        }

                        return responseSuccess;

                        #endregion
                    }
                    else if (Helper.IsTrue(currentChannel.Unicast))
                    {
                        #region Unicast-Message-to-One-Subscriber

                        responseSuccess = SendChannelSubscriberMessage(currentClient, currentChannel, currentMessage);
                        if (!responseSuccess)
                        {
                            Log("*** MessageProcessor unable to send to subscriber in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
                        }

                        return responseSuccess;

                        #endregion
                    }
                    else
                    {
                        #region Unknown-Channel-Type

                        Log("*** MessageProcessor channel " + currentChannel.ChannelGUID + " not marked as broadcast, multicast, or unicast, deleting");
                        if (!RemoveChannel(currentChannel))
                        {
                            Log("*** MessageProcessor unable to remove channel " + currentChannel.ChannelGUID);
                        }

                        return false;

                        #endregion
                    }

                    #endregion
                }
                else
                {
                    #region Recipient-Not-Found

                    Log("MessageProcessor unable to find either recipient or channel");
                    responseMessage = MsgBuilder.RecipientNotFound(currentClient, currentMessage);
                    responseSuccess = QueueClientMessage(currentClient, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue recipient not found message to client " + currentClient.IpPort());
                    }
                    return false;

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("MessageProcessor " + currentMessage.Command + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendPrivateMessage(Client sender, Client rcpt, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (sender == null)
                {
                    Log("*** SendPrivateMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(sender.ClientGUID, ServerGUID) != 0)
                {
                    // all zeros is the server
                    if (sender.IsTCP)
                    {
                        if (sender.ClientTCPInterface == null)
                        {
                            Log("*** SendPrivateMessage null TCP client within supplied Sender");
                            return false;
                        }
                    }

                    if (sender.IsWebsocket)
                    {
                        if (sender.ClientHTTPContext == null)
                        {
                            Log("*** SendPrivateMessage null HTTP context within supplied Sender");
                            return false;
                        }

                        if (sender.ClientWSContext == null)
                        {
                            Log("*** SendPrivateMessage null websocket context within supplied Sender");
                            return false;
                        }
                        if (sender.ClientWSInterface == null)
                        {
                            Log("*** SendPrivateMessage null websocket object within supplied Sender");
                            return false;
                        }
                    }
                }

                if (rcpt == null)
                {
                    Log("*** SendPrivateMessage null Recipient supplied");
                    return false;
                }

                if (rcpt.IsTCP)
                {
                    if (rcpt.ClientTCPInterface == null)
                    {
                        Log("*** SendPrivateMessage null TCP client within supplied Recipient");
                        return false;
                    }
                }

                if (rcpt.IsWebsocket)
                {
                    if (rcpt.ClientHTTPContext == null)
                    {
                        Log("*** SendPrivateMessage null HTTP context within supplied Recipient");
                        return false;
                    }

                    if (rcpt.ClientWSContext == null)
                    {
                        Log("*** SendPrivateMessage null websocket context within supplied Recipient");
                        return false;
                    }

                    if (rcpt.ClientWSInterface == null)
                    {
                        Log("*** SendPrivateMessage null websocket object within supplied Recipient");
                        return false;
                    }
                }

                if (currentMessage == null)
                {
                    Log("*** SendPrivateMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool responseSuccess = false;
                Message responseMessage = new Message();

                #endregion

                #region Send-to-Recipient

                responseSuccess = QueueClientMessage(rcpt, currentMessage.Redact());

                #endregion

                #region Send-Success-or-Failure-to-Sender

                if (currentMessage.SyncRequest != null && Convert.ToBoolean(currentMessage.SyncRequest))
                {
                    #region Sync-Request

                    //
                    // do not send notifications for success/fail on a sync message
                    //

                    return true;

                    #endregion
                }
                else if (currentMessage.SyncRequest != null && Convert.ToBoolean(currentMessage.SyncResponse))
                {
                    #region Sync-Response

                    //
                    // do not send notifications for success/fail on a sync message
                    //

                    return true;

                    #endregion
                }
                else
                {
                    #region Async

                    if (responseSuccess)
                    {
                        if (Config.Notification.MsgAcknowledgement)
                        {
                            responseMessage = MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                            responseSuccess = QueueClientMessage(sender, responseMessage);
                        }
                        return true;
                    }
                    else
                    {
                        responseMessage = MsgBuilder.MessageQueueFailure(sender, currentMessage);
                        responseSuccess = QueueClientMessage(sender, responseMessage);
                        return false;
                    }

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendPrivateMessage " + currentMessage.SenderGUID + " -> " + currentMessage.RecipientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendChannelMembersMessage(Client sender, Channel currentChannel, Message currentMessage)
        {
            //
            // broadcast channel
            //
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (sender == null)
                {
                    Log("*** SendChannelMembersMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(sender.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (sender.IsTCP)
                    {
                        #region TCP

                        if (sender.ClientTCPInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (sender.ClientTCPSSLInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null TCP SSL client within supplied client");
                            return false;
                        }

                        if (sender.ClientSSLStream == null)
                        {
                            Log("*** SendChannelMembersMessage null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsWebsocket)
                    {
                        #region Websocket

                        if (sender.ClientHTTPContext == null)
                        {
                            Log("*** SendChannelMembersMessage null HTTP context within supplied client");
                            return false;
                        }

                        if (sender.ClientWSContext == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket context witin supplied client");
                            return false;
                        }

                        if (sender.ClientWSInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (sender.ClientHTTPSSLContext == null)
                        {
                            Log("*** SendChannelMembersMessage null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (sender.ClientWSSSLContext == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (sender.ClientWSSSLInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** SendChannelMembersMessage unknown transport for supplied client " + sender.IpPort() + " " + sender.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                if (currentChannel == null)
                {
                    Log("*** SendChannelMembersMessage null channel supplied");
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** SendChannelMembersMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool responseSuccess = false;
                Message responseMessage = new Message();

                #endregion

                #region Verify-Channel-Membership

                if (!IsChannelMember(sender, currentChannel))
                {
                    responseMessage = MsgBuilder.NotChannelMember(sender, currentMessage, currentChannel);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** SendChannelMembersMessage unable to queue not channel member message to " + sender.IpPort());
                    }
                    return false;
                }

                #endregion

                #region Send-to-Channel-and-Return-Success

                Task.Run(() =>
                {
                    responseSuccess = ChannelDataSender(sender, currentChannel, currentMessage.Redact());
                });

                if (Config.Notification.MsgAcknowledgement)
                {
                    responseMessage = MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** SendChannelMembersMessage unable to queue message queue success notification to " + sender.IpPort());
                    }
                }
                return true;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendChannelMembersMessage " + currentMessage.SenderGUID + " -> " + currentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendChannelSubscribersMessage(Client sender, Channel currentChannel, Message currentMessage)
        {
            //
            // multicast channel
            //
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (sender == null)
                {
                    Log("*** SendChannelSubscribersMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(sender.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (sender.IsTCP)
                    {
                        #region TCP

                        if (sender.ClientTCPInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (sender.ClientTCPSSLInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null TCP SSL client within supplied client");
                            return false;
                        }

                        if (sender.ClientSSLStream == null)
                        {
                            Log("*** SendChannelSubscribersMessage null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsWebsocket)
                    {
                        #region Websocket

                        if (sender.ClientHTTPContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null HTTP context within supplied client");
                            return false;
                        }

                        if (sender.ClientWSContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket context witin supplied client");
                            return false;
                        }

                        if (sender.ClientWSInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (sender.ClientHTTPSSLContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (sender.ClientWSSSLContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (sender.ClientWSSSLInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** SendChannelSubscribersMessage unknown transport for supplied client " + sender.IpPort() + " " + sender.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                if (currentChannel == null)
                {
                    Log("*** SendChannelSubscribersMessage null channel supplied");
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** SendChannelSubscribersMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool responseSuccess = false;
                Message responseMessage = new Message();

                #endregion

                #region Verify-Channel-Membership

                if (!IsChannelMember(sender, currentChannel))
                {
                    responseMessage = MsgBuilder.NotChannelMember(sender, currentMessage, currentChannel);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** SendChannelSubscribersMessage unable to queue not channel member message to " + sender.IpPort());
                    }
                    return false;
                }

                #endregion

                #region Send-to-Channel-Subscribers-and-Return-Success

                Task.Run(() =>
                {
                    responseSuccess = ChannelDataSender(sender, currentChannel, currentMessage.Redact());
                });

                if (Config.Notification.MsgAcknowledgement)
                {
                    responseMessage = MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** SendChannelSubscribersMessage unable to queue message queue success mesage to " + sender.IpPort());
                    }
                }
                return true;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendChannelSubscribersMessage " + currentMessage.SenderGUID + " -> " + currentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }
        
        private bool SendChannelSubscriberMessage(Client sender, Channel currentChannel, Message currentMessage)
        {
            //
            // unicast within a multicast channel
            //
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (sender == null)
                {
                    Log("*** SendChannelSubscriberMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(sender.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (sender.IsTCP)
                    {
                        #region TCP

                        if (sender.ClientTCPInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (sender.ClientTCPSSLInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null TCP SSL client within supplied client");
                            return false;
                        }

                        if (sender.ClientSSLStream == null)
                        {
                            Log("*** SendChannelSubscriberMessage null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsWebsocket)
                    {
                        #region Websocket

                        if (sender.ClientHTTPContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null HTTP context within supplied client");
                            return false;
                        }

                        if (sender.ClientWSContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket context witin supplied client");
                            return false;
                        }

                        if (sender.ClientWSInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (sender.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (sender.ClientHTTPSSLContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (sender.ClientWSSSLContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (sender.ClientWSSSLInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** SendChannelSubscriberMessage unknown transport for supplied client " + sender.IpPort() + " " + sender.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                if (currentChannel == null)
                {
                    Log("*** SendChannelSubscriberMessage null channel supplied");
                    return false;
                }

                if (currentMessage == null)
                {
                    Log("*** SendChannelSubscriberMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool responseSuccess = false;
                Message responseMessage = new Message();

                #endregion

                #region Verify-Channel-Membership

                if (!IsChannelMember(sender, currentChannel))
                {
                    responseMessage = MsgBuilder.NotChannelMember(sender, currentMessage, currentChannel);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** SendChannelSubscriberMessage unable to queue not channel member message to " + sender.IpPort());
                    }
                    return false;
                }

                #endregion

                #region Send-to-Channel-Subscriber-and-Return-Success

                Task.Run(() =>
                {
                    responseSuccess = ChannelDataSender(sender, currentChannel, currentMessage.Redact());
                });

                if (Config.Notification.MsgAcknowledgement)
                {
                    responseMessage = MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** SendChannelSubscriberMessage unable to queue message queue success mesage to " + sender.IpPort());
                    }
                }
                return true;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendChannelSubscriberMessage " + currentMessage.SenderGUID + " -> " + currentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemMessage(Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (currentMessage == null)
                {
                    Log("*** SendSystemMessage null message supplied");
                    return false;
                }

                #endregion

                #region Create-System-Client-Object

                Client currentClient = new Client();
                currentClient.Email = null;
                currentClient.Password = null;
                currentClient.ClientGUID = ServerGUID;
                currentClient.SourceIP = "127.0.0.1";
                currentClient.SourcePort = 0;
                currentClient.ServerIP = currentClient.SourceIP;
                currentClient.ServerPort = currentClient.SourcePort;
                currentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                currentClient.UpdatedUTC = currentClient.CreatedUTC;

                #endregion

                #region Variables

                Client currentRecipient = new Client();
                Channel currentChannel = new Channel();
                Message responseMessage = new Message();
                bool responseSuccess = false;

                #endregion

                #region Get-Recipient-or-Channel

                if (!String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    currentRecipient = ConnMgr.GetClientByGUID(currentMessage.RecipientGUID);
                }
                else if (!String.IsNullOrEmpty(currentMessage.ChannelGUID))
                {
                    currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                }
                else
                {
                    #region Recipient-Not-Supplied

                    Log("SendSystemMessage no recipient specified either by RecipientGUID or ChannelGUID");
                    return false;

                    #endregion
                }

                #endregion

                #region Process-Recipient-Messages

                if (currentRecipient != null)
                {
                    #region Send-to-Recipient

                    responseSuccess = QueueClientMessage(currentRecipient, currentMessage.Redact());
                    if (responseSuccess)
                    {
                        Log("SendSystemMessage successfully queued message to recipient " + currentRecipient.ClientGUID);
                        return true;
                    }
                    else
                    {
                        Log("*** SendSystemMessage unable to queue message to recipient " + currentRecipient.ClientGUID);
                        return false;
                    }

                    #endregion
                }
                else if (currentChannel != null)
                {
                    #region Send-to-Channel-and-Return-Success

                    responseSuccess = ChannelDataSender(currentClient, currentChannel, currentMessage.Redact());
                    if (responseSuccess)
                    {
                        Log("SendSystemMessage successfully sent message to channel " + currentChannel.ChannelGUID);
                        return true;
                    }
                    else
                    {
                        Log("*** SendSystemMessage unable to send message to channel " + currentChannel.ChannelGUID);
                        return false;
                    }

                    #endregion
                }
                else
                {
                    #region Recipient-Not-Found

                    Log("Unable to find either recipient or channel");
                    responseMessage = MsgBuilder.RecipientNotFound(currentClient, currentMessage);
                    responseSuccess = QueueClientMessage(currentClient, responseMessage);
                    if (!responseSuccess)
                    {
                        Log("*** SendSystemMessage unable to queue recipient not found message to " + currentClient.IpPort());
                    }
                    return false;

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendSystemMessage " + currentMessage.SenderGUID + " -> " + currentMessage.RecipientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemPrivateMessage(Client rcpt, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (rcpt == null)
                {
                    Log("*** SendSystemPrivateMessage null recipient supplied");
                    return false;
                }

                if (rcpt.IsTCP)
                {
                    if (rcpt.ClientTCPInterface == null)
                    {
                        Log("*** SendSystemPrivateMessage null TCP client found within supplied recipient");
                        return false;
                    }
                }

                if (rcpt.IsWebsocket)
                {
                    if (rcpt.ClientHTTPContext == null)
                    {
                        Log("*** SendSystemPrivateMessage null HTTP context found within supplied recipient");
                        return false;
                    }

                    if (rcpt.ClientWSContext == null)
                    {
                        Log("*** SendSystemPrivateMessage null websocket context found within supplied recipient");
                        return false;
                    }

                    if (rcpt.ClientWSInterface == null)
                    {
                        Log("*** SendSystemPrivateMessage null websocket object found within supplied recipient");
                        return false;
                    }
                }

                if (currentMessage == null)
                {
                    Log("*** SendSystemPrivateMessage null message supplied");
                    return false;
                }

                #endregion

                #region Create-System-Client-Object

                Client currentClient = new Client();
                currentClient.Email = null;
                currentClient.Password = null;
                currentClient.ClientGUID = ServerGUID;

                if (!String.IsNullOrEmpty(Config.TcpServer.IP)) currentClient.SourceIP = Config.TcpServer.IP;
                else currentClient.SourceIP = "127.0.0.1";

                currentClient.SourcePort = Config.TcpServer.Port;
                currentClient.ServerIP = currentClient.SourceIP;
                currentClient.ServerPort = currentClient.SourcePort;
                currentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                currentClient.UpdatedUTC = currentClient.CreatedUTC;

                #endregion

                #region Variables

                Channel currentChannel = new Channel();
                bool responseSuccess = false;

                #endregion

                #region Process-Recipient-Messages

                responseSuccess = QueueClientMessage(rcpt, currentMessage.Redact());
                if (!responseSuccess)
                {
                    Log("*** SendSystemPrivateMessage unable to queue message to " + rcpt.IpPort());
                }
                return responseSuccess;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendSystemPrivateMessage " + currentMessage.SenderGUID + " -> " + currentMessage.RecipientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemChannelMessage(Channel currentChannel, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (currentChannel == null)
                {
                    Log("*** SendSystemChannelMessage null channel supplied");
                    return false;
                }

                if (currentChannel.Subscribers == null || currentChannel.Subscribers.Count < 1)
                {
                    Log("SendSystemChannelMessage no subscribers in channel " + currentChannel.ChannelGUID);
                    return true;
                }

                if (currentMessage == null)
                {
                    Log("*** SendSystemPrivateMessage null message supplied");
                    return false;
                }

                #endregion

                #region Create-System-Client-Object

                Client currentClient = new Client();
                currentClient.Email = null;
                currentClient.Password = null;
                currentClient.ClientGUID = ServerGUID;

                if (!String.IsNullOrEmpty(Config.TcpServer.IP)) currentClient.SourceIP = Config.TcpServer.IP;
                else currentClient.SourceIP = "127.0.0.1";

                currentClient.SourcePort = Config.TcpServer.Port;
                currentClient.ServerIP = currentClient.SourceIP;
                currentClient.ServerPort = currentClient.SourcePort;
                currentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                currentClient.UpdatedUTC = currentClient.CreatedUTC;

                #endregion

                #region Override-Channel-Variables

                //
                // This is necessary so the message goes to members instead of subscribers
                // in case the channel is configured as a multicast channel
                //
                currentChannel.Broadcast = 1;
                currentChannel.Multicast = 0;                

                #endregion
                
                #region Send-to-Channel

                bool responseSuccess = ChannelDataSender(currentClient, currentChannel, currentMessage);
                return responseSuccess;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendSystemChannelMessage " + currentMessage.SenderGUID + " -> " + currentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion

        #region Private-Message-Handlers

        private Message ProcessEchoMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                currentMessage = currentMessage.Redact();
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = ServerGUID;
                currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                currentMessage.Success = true;
                return currentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessEchoMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessLoginMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            bool runClientLoginTask = false;
            bool runServerJoinNotification = false;

            try
            {
                // build response message and update client
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = ServerGUID;
                currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                currentClient.ClientGUID = currentMessage.RecipientGUID;
                currentClient.Email = currentMessage.Email;
                if (String.IsNullOrEmpty(currentClient.Email)) currentClient.Email = currentClient.ClientGUID;
                ConnMgr.UpdateClient(currentClient);

                // start heartbeat
                currentClient.HeartbeatTokenSource = new CancellationTokenSource();
                currentClient.HeartbeatToken = currentClient.HeartbeatTokenSource.Token;
                Log("ProcessLoginMessage starting heartbeat manager for " + currentClient.IpPort());
                Task.Run(() => TCPHeartbeatManager(currentClient));

                currentMessage = currentMessage.Redact();
                runClientLoginTask = true;
                runServerJoinNotification = true;

                return currentMessage;
            }
            finally
            {
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessLoginMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.ElapsedMilliseconds + "ms (before tasks)");

                if (runClientLoginTask)
                {
                    if (ClientLogin != null)
                    {
                        Task.Run(() => ClientLogin(currentClient));
                    }
                }

                if (runServerJoinNotification)
                {
                    if (Config.Notification.ServerJoinNotification)
                    {
                        Task.Run(() => ServerJoinEvent(currentClient));
                    }
                }

                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessLoginMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.ElapsedMilliseconds + "ms (after tasks)");
            }
        }

        private Message ProcessIsClientConnectedMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                currentMessage = currentMessage.Redact();
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = ServerGUID;
                currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();

                if (currentMessage.Data == null)
                {
                    currentMessage.Success = false;
                    currentMessage.Data = FailureData.ToBytes(ErrorTypes.BadRequest, "Data does not include client GUID", null);
                }
                else
                {
                    currentMessage.Success = true;
                    bool exists = ConnMgr.ClientExists(Encoding.UTF8.GetString(currentMessage.Data));
                    currentMessage.Data = SuccessData.ToBytes(null, exists);
                }

                return currentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessIsClientConnectedMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessJoinChannelMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = null;

                if (currentChannel == null)
                {
                    Log("*** ProcessJoinChannelMessage unable to find channel " + currentChannel.ChannelGUID);
                    responseMessage = MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                    return responseMessage;
                }
                else
                {
                    Log("ProcessJoinChannelMessage adding client " + currentClient.IpPort() + " as member to channel " + currentChannel.ChannelGUID);
                    if (!AddChannelMember(currentClient, currentChannel))
                    {
                        Log("*** ProcessJoinChannelMessage error while adding " + currentClient.IpPort() + " " + currentClient.ClientGUID + " as member of channel " + currentChannel.ChannelGUID);
                        responseMessage = MsgBuilder.ChannelJoinFailure(currentClient, currentMessage, currentChannel);
                        return responseMessage;
                    }
                    else
                    {
                        responseMessage = MsgBuilder.ChannelJoinSuccess(currentClient, currentMessage, currentChannel);
                        return responseMessage;
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessJoinChannelMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessSubscribeChannelMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = null;

                if (currentChannel == null)
                {
                    Log("*** ProcessSubscribeChannelMessage unable to find channel " + currentChannel.ChannelGUID);
                    responseMessage = MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                    return responseMessage;
                }

                if (currentChannel.Broadcast == 1)
                {
                    Log("ProcessSubscribeChannelMessage channel marked as broadcast, calling ProcessJoinChannelMessage");
                    return ProcessJoinChannelMessage(currentClient, currentMessage);
                }
                
                #region Add-Member

                Log("ProcessSubscribeChannelMessage adding client " + currentClient.IpPort() + " as subscriber to channel " + currentChannel.ChannelGUID);
                if (!AddChannelMember(currentClient, currentChannel))
                {
                    Log("*** ProcessSubscribeChannelMessage error while adding " + currentClient.IpPort() + " " + currentClient.ClientGUID + " as member of channel " + currentChannel.ChannelGUID);
                    responseMessage = MsgBuilder.ChannelJoinFailure(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }

                #endregion

                #region Add-Subscriber

                if (!AddChannelSubscriber(currentClient, currentChannel))
                {
                    Log("*** ProcessSubscribeChannelMessage error while adding " + currentClient.IpPort() + " " + currentClient.ClientGUID + " as subscriber to channel " + currentChannel.ChannelGUID);
                    responseMessage = MsgBuilder.ChannelSubscribeFailure(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }

                #endregion

                #region Return

                responseMessage = MsgBuilder.ChannelSubscribeSuccess(currentClient, currentMessage, currentChannel);
                return responseMessage;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessJoinChannelMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessLeaveChannelMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = new Message();

                if (currentChannel == null)
                {
                    responseMessage = MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                    return responseMessage;
                }
                else
                {
                    if (String.Compare(currentClient.ClientGUID, currentChannel.OwnerGUID) == 0)
                    {
                        #region Owner-Abandoning-Channel

                        if (!RemoveChannel(currentChannel))
                        {
                            Log("*** ProcessLeaveChannelMessage unable to remove owner " + currentClient.IpPort() + " from channel " + currentMessage.ChannelGUID);
                            return MsgBuilder.ChannelLeaveFailure(currentClient, currentMessage, currentChannel);
                        }
                        else
                        {
                            return MsgBuilder.ChannelDeleteSuccess(currentClient, currentMessage, currentChannel);
                        }

                        #endregion
                    }
                    else
                    {
                        #region Member-Leaving-Channel

                        if (!RemoveChannelMember(currentClient, currentChannel))
                        {
                            Log("*** ProcessLeaveChannelMessage unable to remove member " + currentClient.IpPort() + " " + currentClient.ClientGUID + " from channel " + currentMessage.ChannelGUID);
                            return MsgBuilder.ChannelLeaveFailure(currentClient, currentMessage, currentChannel);
                        }
                        else
                        {
                            if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(currentClient, currentChannel);
                            return MsgBuilder.ChannelLeaveSuccess(currentClient, currentMessage, currentChannel);
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessLeaveChannelMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessUnsubscribeChannelMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = new Message();

                if (currentChannel == null)
                {
                    responseMessage = MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                    return responseMessage;
                }
                
                if (currentChannel.Broadcast == 1)
                {
                    Log("ProcessUnsubscribeChannelMessage channel marked as broadcast, calling ProcessLeaveChannelMessage");
                    return ProcessLeaveChannelMessage(currentClient, currentMessage);
                }
                
                if (String.Compare(currentClient.ClientGUID, currentChannel.OwnerGUID) == 0)
                {
                    #region Owner-Abandoning-Channel

                    if (!RemoveChannel(currentChannel))
                    {
                        Log("*** ProcessUnsubscribeChannelMessage unable to remove owner " + currentClient.IpPort() + " from channel " + currentMessage.ChannelGUID);
                        return MsgBuilder.ChannelUnsubscribeFailure(currentClient, currentMessage, currentChannel);
                    }
                    else
                    {
                        return MsgBuilder.ChannelDeleteSuccess(currentClient, currentMessage, currentChannel);
                    }

                    #endregion
                }
                else
                {
                    #region Subscriber-Leaving-Channel

                    if (!RemoveChannelSubscriber(currentClient, currentChannel))
                    {
                        Log("*** ProcessUnsubscribeChannelMessage unable to remove subscrber " + currentClient.IpPort() + " " + currentClient.ClientGUID + " from channel " + currentMessage.ChannelGUID);
                        return MsgBuilder.ChannelUnsubscribeFailure(currentClient, currentMessage, currentChannel);
                    }
                    else
                    {
                        if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(currentClient, currentChannel);
                        return MsgBuilder.ChannelUnsubscribeSuccess(currentClient, currentMessage, currentChannel);
                    }

                    #endregion
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessUnsubscribeChannelMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessCreateChannelMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = new Message();

                if (currentChannel == null)
                {
                    Channel requestChannel = BuildChannelFromMessageData(currentClient, currentMessage);
                    if (requestChannel == null)
                    {
                        Log("*** ProcessCreateChannelMessage unable to build Channel from Message data");
                        responseMessage = MsgBuilder.DataError(currentClient, currentMessage, "unable to create Channel from supplied message data");
                        return responseMessage;
                    }
                    else
                    {
                        currentChannel = ChannelMgr.GetChannelByName(requestChannel.ChannelName);
                        if (currentChannel != null)
                        {
                            responseMessage = MsgBuilder.ChannelAlreadyExists(currentClient, currentMessage, currentChannel);
                            return responseMessage;
                        }
                        else
                        {
                            if (String.IsNullOrEmpty(requestChannel.ChannelGUID))
                            {
                                requestChannel.ChannelGUID = Guid.NewGuid().ToString();
                                Log("ProcessCreateChannelMessage adding GUID " + requestChannel.ChannelGUID + " to request (not supplied by requestor)");
                            }

                            requestChannel.OwnerGUID = currentClient.ClientGUID;

                            if (!AddChannel(currentClient, requestChannel))
                            {
                                Log("*** ProcessCreateChannelMessage error while adding channel " + currentChannel.ChannelGUID);
                                responseMessage = MsgBuilder.ChannelCreateFailure(currentClient, currentMessage);
                                return responseMessage;
                            }
                            else
                            {
                                ChannelCreateEvent(currentClient, requestChannel);
                            }

                            if (!AddChannelSubscriber(currentClient, requestChannel))
                            {
                                Log("*** ProcessCreateChannelMessage error while adding channel member " + currentClient.IpPort() + " to channel " + currentChannel.ChannelGUID);
                                responseMessage = MsgBuilder.ChannelJoinFailure(currentClient, currentMessage, currentChannel);
                                return responseMessage;
                            }

                            responseMessage = MsgBuilder.ChannelCreateSuccess(currentClient, currentMessage, requestChannel);
                            return responseMessage;
                        }
                    }
                }
                else
                {
                    responseMessage = MsgBuilder.ChannelAlreadyExists(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessCreateChannelMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessDeleteChannelMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = new Message();

                if (currentChannel == null)
                {
                    responseMessage = MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                    return responseMessage;
                }

                if (String.Compare(currentChannel.OwnerGUID, currentClient.ClientGUID) != 0)
                {
                    responseMessage = MsgBuilder.ChannelDeleteFailure(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }

                if (!RemoveChannel(currentChannel))
                {
                    Log("*** ProcessDeleteChannelMessage unable to remove channel " + currentChannel.ChannelGUID);
                    responseMessage = MsgBuilder.ChannelDeleteFailure(currentClient, currentMessage, currentChannel);
                }
                else
                {
                    responseMessage = MsgBuilder.ChannelDeleteSuccess(currentClient, currentMessage, currentChannel);
                    ChannelDestroyEvent(currentClient, currentChannel);
                }

                return responseMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessDeleteChannelMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListChannelsMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                List<Channel> ret = new List<Channel>();
                List<Channel> filtered = new List<Channel>();
                Channel currentChannel = new Channel();

                ret = ChannelMgr.GetChannels();
                if (ret == null || ret.Count < 1)
                {
                    Log("*** ProcessListChannelsMessage no channels retrieved");

                    currentMessage = currentMessage.Redact();
                    currentMessage.SyncResponse = currentMessage.SyncRequest;
                    currentMessage.SyncRequest = null;
                    currentMessage.RecipientGUID = currentMessage.SenderGUID;
                    currentMessage.SenderGUID = ServerGUID;
                    currentMessage.ChannelGUID = null;
                    currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    currentMessage.Success = true;
                    currentMessage.Data = SuccessData.ToBytes(null, new List<Channel>());
                    return currentMessage;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelsMessage retrieved GetAllChannels after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Channel curr in ret)
                    {
                        currentChannel = new Channel();
                        currentChannel.Subscribers = null;
                        currentChannel.ChannelGUID = curr.ChannelGUID;
                        currentChannel.ChannelName = curr.ChannelName;
                        currentChannel.OwnerGUID = curr.OwnerGUID;
                        currentChannel.CreatedUTC = curr.CreatedUTC;
                        currentChannel.UpdatedUTC = curr.UpdatedUTC;
                        currentChannel.Private = curr.Private;
                        currentChannel.Broadcast = curr.Broadcast;
                        currentChannel.Multicast = curr.Multicast;
                        currentChannel.Unicast = curr.Unicast;

                        if (String.Compare(currentChannel.OwnerGUID, currentClient.ClientGUID) == 0)
                        {
                            filtered.Add(currentChannel);
                            continue;
                        }

                        if (currentChannel.Private == 0)
                        {
                            filtered.Add(currentChannel);
                            continue;
                        }
                    }

                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelsMessage built response list after " + sw.Elapsed.TotalMilliseconds + "ms");
                }
                
                currentMessage = currentMessage.Redact();
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = ServerGUID;
                currentMessage.ChannelGUID = null;
                currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                currentMessage.Success = true;
                currentMessage.Data = SuccessData.ToBytes(null, filtered);
                return currentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelsMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListChannelMembersMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = new Message();
                List<Client> clients = new List<Client>();
                List<Client> ret = new List<Client>();

                if (currentChannel == null)
                {
                    Log("*** ProcessListChannelMembersMessage null channel after retrieval by GUID");
                    responseMessage = MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                    return responseMessage;
                }

                clients = ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
                if (clients == null || clients.Count < 1)
                {
                    Log("ProcessListChannelMembersMessage channel " + currentChannel.ChannelGUID + " has no members");
                    responseMessage = MsgBuilder.ChannelNoMembers(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelMembersMessage retrieved GetChannelMembers after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Client curr in clients)
                    {
                        Client temp = new Client();
                        temp.Password = null;
                        
                        temp.ClientTCPInterface = null;
                        temp.ClientHTTPContext = null;
                        temp.ClientWSContext = null;
                        temp.ClientWSInterface = null;

                        temp.Email = curr.Email;
                        temp.ClientGUID = curr.ClientGUID;
                        temp.CreatedUTC = curr.CreatedUTC;
                        temp.UpdatedUTC = curr.UpdatedUTC;
                        temp.SourceIP = curr.SourceIP;
                        temp.SourcePort = curr.SourcePort;
                        temp.IsTCP = curr.IsTCP;
                        temp.IsTCPSSL = curr.IsTCPSSL;
                        temp.IsWebsocket = curr.IsWebsocket;
                        temp.IsWebsocketSSL = curr.IsWebsocketSSL;

                        ret.Add(temp);
                    }

                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelMembersMessage built response list after " + sw.Elapsed.TotalMilliseconds + "ms");

                    currentMessage = currentMessage.Redact();
                    currentMessage.SyncResponse = currentMessage.SyncRequest;
                    currentMessage.SyncRequest = null;
                    currentMessage.RecipientGUID = currentMessage.SenderGUID;
                    currentMessage.SenderGUID = ServerGUID;
                    currentMessage.ChannelGUID = currentChannel.ChannelGUID;
                    currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    currentMessage.Success = true;
                    currentMessage.Data = SuccessData.ToBytes(null, ret);
                    return currentMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelMembersMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListChannelSubscribersMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel currentChannel = ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
                Message responseMessage = new Message();
                List<Client> clients = new List<Client>();
                List<Client> ret = new List<Client>();

                if (currentChannel == null)
                {
                    Log("*** ProcessListChannelSubscribersMessage null channel after retrieval by GUID");
                    responseMessage = MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                    return responseMessage;
                }

                if (currentChannel.Broadcast == 1)
                {
                    Log("ProcessListChannelSubscribersMessage channel is broadcast, calling ProcessListChannelMembers");
                    return ProcessListChannelMembersMessage(currentClient, currentMessage);
                }

                clients = ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
                if (clients == null || clients.Count < 1)
                {
                    Log("ProcessListChannelSubscribersMessage channel " + currentChannel.ChannelGUID + " has no subscribers");
                    responseMessage = MsgBuilder.ChannelNoSubscribers(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelSubscribersMessage retrieved GetChannelSubscribers after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Client curr in clients)
                    {
                        Client temp = new Client();
                        temp.Password = null;
                        
                        temp.ClientTCPInterface = null;
                        temp.ClientHTTPContext = null;
                        temp.ClientWSContext = null;
                        temp.ClientWSInterface = null;

                        temp.Email = curr.Email;
                        temp.ClientGUID = curr.ClientGUID;
                        temp.CreatedUTC = curr.CreatedUTC;
                        temp.UpdatedUTC = curr.UpdatedUTC;
                        temp.SourceIP = curr.SourceIP;
                        temp.SourcePort = curr.SourcePort;
                        temp.IsTCP = curr.IsTCP;
                        temp.IsTCPSSL = curr.IsTCPSSL;
                        temp.IsWebsocket = curr.IsWebsocket;
                        temp.IsWebsocketSSL = curr.IsWebsocketSSL;

                        ret.Add(temp);
                    }

                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelSubscribers built response list after " + sw.Elapsed.TotalMilliseconds + "ms");

                    currentMessage = currentMessage.Redact();
                    currentMessage.SyncResponse = currentMessage.SyncRequest;
                    currentMessage.SyncRequest = null;
                    currentMessage.RecipientGUID = currentMessage.SenderGUID;
                    currentMessage.SenderGUID = ServerGUID;
                    currentMessage.ChannelGUID = currentChannel.ChannelGUID;
                    currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    currentMessage.Success = true;
                    currentMessage.Data = Encoding.UTF8.GetBytes(Helper.SerializeJson(ret));
                    return currentMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelSubscribersMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListClientsMessage(Client currentClient, Message currentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                List<Client> clients = new List<Client>();
                List<Client> ret = new List<Client>();

                clients = ConnMgr.GetClients();
                if (clients == null || clients.Count < 1)
                {
                    Log("*** ProcessListClientsMessage no clients retrieved");
                    return null;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListClientsMessage retrieved GetAllClients after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Client curr in clients)
                    {
                        Client temp = new Client();
                        temp.SourceIP = curr.SourceIP;
                        temp.SourcePort = curr.SourcePort;
                        temp.IsTCP = curr.IsTCP;
                        temp.IsTCPSSL = curr.IsTCPSSL;
                        temp.IsWebsocket = curr.IsWebsocket;
                        temp.IsWebsocketSSL = curr.IsWebsocketSSL;

                        //
                        // contexts will not serialize
                        //
                        temp.ClientTCPInterface = null;
                        temp.ClientTCPSSLInterface = null;
                        temp.ClientSSLStream = null;
                        temp.ClientHTTPContext = null;
                        temp.ClientWSContext = null;
                        temp.ClientWSInterface = null;
                        temp.ClientWSSSLContext = null;
                        temp.ClientWSSSLInterface = null;
                        
                        temp.Email = curr.Email;
                        temp.Password = null;
                        temp.ClientGUID = curr.ClientGUID;
                        temp.CreatedUTC = curr.CreatedUTC;
                        temp.UpdatedUTC = curr.UpdatedUTC;

                        ret.Add(temp);
                    }

                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListClientsMessage built response list after " + sw.Elapsed.TotalMilliseconds + "ms");
                }

                currentMessage = currentMessage.Redact();
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = ServerGUID;
                currentMessage.ChannelGUID = null;
                currentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                currentMessage.Success = true;
                currentMessage.Data = SuccessData.ToBytes(null, ret);
                return currentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListClientsMessage " + currentClient.IpPort() + " " + currentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion
        
        #region Private-Logging-Methods

        private void Log(string message)
        {
            if (LogMessage != null) LogMessage(message);
            if (Config.Debug.Enable && Config.Debug.ConsoleLogging)
            {
                Console.WriteLine(message);
            }
        }

        private void LogException(string method, Exception e)
        {
            Log("================================================================================");
            Log(" = Method: " + method);
            Log(" = Exception Type: " + e.GetType().ToString());
            Log(" = Exception Data: " + e.Data);
            Log(" = Inner Exception: " + e.InnerException);
            Log(" = Exception Message: " + e.Message);
            Log(" = Exception Source: " + e.Source);
            Log(" = Exception StackTrace: " + e.StackTrace);
            Log("================================================================================");
        }

        private void PrintException(string method, Exception e)
        {
            Console.WriteLine("================================================================================");
            Console.WriteLine(" = Method: " + method);
            Console.WriteLine(" = Exception Type: " + e.GetType().ToString());
            Console.WriteLine(" = Exception Data: " + e.Data);
            Console.WriteLine(" = Inner Exception: " + e.InnerException);
            Console.WriteLine(" = Exception Message: " + e.Message);
            Console.WriteLine(" = Exception Source: " + e.Source);
            Console.WriteLine(" = Exception StackTrace: " + e.StackTrace);
            Console.WriteLine("================================================================================");
        }

        #endregion
    }
}
