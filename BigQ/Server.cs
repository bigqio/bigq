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
        #region Public-Class-Members

        /// <summary>
        /// Contains configuration-related variables for the server.  
        /// </summary>
        public ServerConfiguration Config;

        #endregion

        #region Private-Class-Members

        //
        // configuration
        //
        private DateTime CreatedUTC;
        private Random RNG;
        private string ServerGUID = "00000000-0000-0000-0000-000000000000";

        //
        // resources
        //
        private ConcurrentDictionary<string, Client> Clients;                   // IpPort(), Client
        private ConcurrentDictionary<string, string> ClientGUIDMap;             // Guid, IpPort()
        private ConcurrentDictionary<string, DateTime> ClientActiveSendMap;     // Receiver GUID, AddedUTC
        private ConcurrentDictionary<string, Channel> Channels;                 // Guid, Channel

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

        #region Public-Constructor

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

            Clients = new ConcurrentDictionary<string, Client>();
            ClientGUIDMap = new ConcurrentDictionary<string, string>();
            ClientActiveSendMap = new ConcurrentDictionary<string, DateTime>();
            Channels = new ConcurrentDictionary<string, Channel>();

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
            return GetAllChannels();
        }

        /// <summary>
        /// Retrieve list of members in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListChannelMembers(string guid)
        {
            return GetChannelMembers(guid);
        }

        /// <summary>
        /// Retrieve list of subscribers in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListChannelSubscribers(string guid)
        {
            return GetChannelSubscribers(guid);
        }

        /// <summary>
        /// Retrieve list of all clients on the server.
        /// </summary>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListClients()
        {
            return GetAllClients();
        }

        /// <summary>
        /// Retrieve list of all client GUID to IP:port maps.
        /// </summary>
        /// <returns>A dictionary containing client GUIDs (keys) and IP:port strings (values).</returns>
        public Dictionary<string, string> ListClientGUIDMaps()
        {
            return GetAllClientGUIDMaps();
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

        #region Private-Transport-and-Connection-Methods

        #region Agnostic
        
        private bool SendMessageImmediately(Client CurrentClient, Message CurrentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** SendMessageImmediately null client supplied");
                    return false;
                }

                if (
                    !CurrentClient.IsTCP 
                    && !CurrentClient.IsTCPSSL
                    && !CurrentClient.IsWebsocket
                    && !CurrentClient.IsWebsocketSSL
                    )
                {
                    Log("*** SendMessageImmediately unable to discern transport for client " + CurrentClient.IpPort());
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendMessageImmediately null message supplied");
                    return false;
                }

                #endregion

                #region Process

                if (CurrentClient.IsTCP)
                {
                    return TCPDataSender(CurrentClient, CurrentMessage);
                }
                if (CurrentClient.IsTCPSSL)
                {
                    return TCPSSLDataSender(CurrentClient, CurrentMessage);
                }
                else if (CurrentClient.IsWebsocket)
                {
                    return WSDataSender(CurrentClient, CurrentMessage);
                }
                else if (CurrentClient.IsWebsocketSSL)
                {
                    return WSDataSender(CurrentClient, CurrentMessage);
                }
                else
                {
                    Log("*** SendMessageImmediately unable to discern transport for client " + CurrentClient.IpPort());
                    return false;
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("SendMessageImmediately (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("SendMessageImmediately (null)", EOuter);
                }

                return false;
            }
        }

        private bool ChannelDataSender(Client CurrentClient, Channel CurrentChannel, Message CurrentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (CurrentChannel == null)
                {
                    Log("*** ChannelDataSender null channel supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** ChannelDataSender null message supplied");
                    return false;
                }

                #endregion

                #region Process-by-Channel-Type

                if (Helper.IsTrue(CurrentChannel.Broadcast))
                {
                    #region Broadcast-Channel

                    List<Client> CurrentChannelMembers = GetChannelMembers(CurrentChannel.Guid);
                    if (CurrentChannelMembers == null || CurrentChannelMembers.Count < 1)
                    {
                        Log("*** ChannelDataSender no members found in channel " + CurrentChannel.Guid);
                        return true;
                    }

                    CurrentMessage.SenderGUID = CurrentClient.ClientGUID;
                    foreach (Client curr in CurrentChannelMembers)
                    {
                        Task.Run(() =>
                        {
                            CurrentMessage.RecipientGUID = curr.ClientGUID;
                            bool ResponseSuccess = false;
                            ResponseSuccess = QueueClientMessage(curr, CurrentMessage);
                            if (!ResponseSuccess)
                            {
                                Log("*** ChannelDataSender error queuing channel message from " + CurrentMessage.SenderGUID + " to member " + CurrentMessage.RecipientGUID + " in channel " + CurrentMessage.ChannelGUID);
                            }
                        });
                    }

                    return true;

                    #endregion
                }
                else if (Helper.IsTrue(CurrentChannel.Multicast))
                {
                    #region Multicast-Channel-to-Subscribers

                    List<Client> CurrentChannelSubscribers = GetChannelSubscribers(CurrentChannel.Guid);
                    if (CurrentChannelSubscribers == null || CurrentChannelSubscribers.Count < 1)
                    {
                        Log("*** ChannelDataSender no subscribers found in channel " + CurrentChannel.Guid);
                        return true;
                    }

                    CurrentMessage.SenderGUID = CurrentClient.ClientGUID;
                    foreach (Client curr in CurrentChannelSubscribers)
                    {
                        Task.Run(() =>
                        {
                            CurrentMessage.RecipientGUID = curr.ClientGUID;
                            bool ResponseSuccess = false;
                            ResponseSuccess = QueueClientMessage(curr, CurrentMessage);
                            if (!ResponseSuccess)
                            {
                                Log("*** ChannelDataSender error queuing channel message from " + CurrentMessage.SenderGUID + " to subscriber " + CurrentMessage.RecipientGUID + " in channel " + CurrentMessage.ChannelGUID);
                            }
                        });
                    }

                    return true;

                    #endregion
                }
                else if (Helper.IsTrue(CurrentChannel.Unicast))
                {
                    #region Unicast-Channel-to-Subscriber

                    List<Client> CurrentChannelSubscribers = GetChannelSubscribers(CurrentChannel.Guid);
                    if (CurrentChannelSubscribers == null || CurrentChannelSubscribers.Count < 1)
                    {
                        Log("*** ChannelDataSender no subscribers found in channel " + CurrentChannel.Guid);
                        return true;
                    }

                    CurrentMessage.SenderGUID = CurrentClient.ClientGUID;
                    Client recipient = CurrentChannelSubscribers[RNG.Next(0, CurrentChannelSubscribers.Count)];
                    Task.Run(() =>
                    {
                        CurrentMessage.RecipientGUID = recipient.ClientGUID;
                        bool ResponseSuccess = false;
                        ResponseSuccess = QueueClientMessage(recipient, CurrentMessage);
                        if (!ResponseSuccess)
                        {
                            Log("*** ChannelDataSender error queuing channel message from " + CurrentMessage.SenderGUID + " to subscriber " + CurrentMessage.RecipientGUID + " in channel " + CurrentMessage.ChannelGUID);
                        }
                    });

                    return true;

                    #endregion
                }
                else
                {
                    #region Unknown

                    Log("*** ChannelDataSender channel is not designated as broadcast, multicast, or unicast, deleting");
                    return RemoveChannel(CurrentChannel);

                    #endregion
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("ChannelDataSender (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("ChannelDataSender (null)", EOuter);
                }

                return false;
            }
        }
        
        private bool QueueClientMessage(Client CurrentClient, Message CurrentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** QueueClientMessage null client supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** QueueClientMessage null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.SenderGUID))
                {
                    Log("*** QueueClientMessage null sender GUID supplied in message");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
                {
                    Log("*** QueueClientMessage null recipient GUID in supplied message");
                    return false;
                }
                
                if (CurrentClient.MessageQueue == null)
                {
                    CurrentClient.MessageQueue = new BlockingCollection<Message>();
                }

                #endregion

                #region Process

                Log("QueueClientMessage queued message for client " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " from " + CurrentMessage.SenderGUID);
                CurrentClient.MessageQueue.Add(CurrentMessage);
                return true;

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("QueueClientMessage (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("QueueClientMessage (null)", EOuter);
                }

                return false;
            }
        }

        private bool ProcessClientQueue(Client CurrentClient)
        {
            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** ProcessClientQueue null client supplied");
                    return false;
                }

                if (CurrentClient.MessageQueue == null)
                {
                    CurrentClient.MessageQueue = new BlockingCollection<Message>();
                }

                #endregion

                #region Process

                while (true)
                {
                    Message CurrentMessage = CurrentClient.MessageQueue.Take(CurrentClient.ProcessClientQueueToken);
                    if (CurrentMessage != null)
                    {
                        bool success = SendMessageImmediately(CurrentClient, CurrentMessage);
                        if (!success)
                        {
                            Log("*** ProcessClientQueue unable to deliver message from " + CurrentMessage.SenderGUID + " to " + CurrentMessage.RecipientGUID + ", requeuing");
                            CurrentClient.MessageQueue.Add(CurrentMessage);
                        }
                        else
                        {
                            Log("ProcessClientQueue successfully sent message from " + CurrentMessage.SenderGUID + " to " + CurrentMessage.RecipientGUID);
                        }
                    }
                    else
                    {
                        Log("*** ProcessClientQueue received null message from queue for client " + CurrentClient.ClientGUID);
                    }
                }

                #endregion
            }
            catch (OperationCanceledException)
            {
                Log("ProcessClientQueue canceled for client " + CurrentClient.IpPort() + " likely due to disconnect");
                return false;
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("ProcessClientQueue (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("ProcessClientQueue (null)", EOuter);
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

                    TcpClient Client = TCPListener.AcceptTcpClientAsync().Result;
                    Client.LingerState.Enabled = false;
                    
                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string ClientIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                        int ClientPort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;
                        Log("TCPAcceptConnections accepted connection from " + ClientIp + ":" + ClientPort);

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

                        Client CurrentClient = new Client();
                        CurrentClient.SourceIP = ClientIp;
                        CurrentClient.SourcePort = ClientPort;
                        CurrentClient.ClientTCPInterface = Client;
                        CurrentClient.ClientTCPSSLInterface = null;
                        CurrentClient.ClientSSLStream = null;
                        CurrentClient.ClientHTTPContext = null;
                        CurrentClient.ClientWSContext = null;
                        CurrentClient.ClientWSInterface = null;
                        CurrentClient.ClientWSSSLContext = null;
                        CurrentClient.ClientWSSSLInterface = null;
                        CurrentClient.MessageQueue = new BlockingCollection<Message>();

                        CurrentClient.IsTCP = true;
                        CurrentClient.IsTCPSSL = false;
                        CurrentClient.IsWebsocket = false;
                        CurrentClient.IsWebsocketSSL = false;
                        CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        CurrentClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        if (!AddClient(CurrentClient))
                        {
                            Log("*** TCPAcceptConnections unable to add client " + CurrentClient.IpPort());
                            Client.Close();
                            return;
                        }

                        #endregion

                        #region Start-Data-Receiver

                        CurrentClient.DataReceiverTokenSource = new CancellationTokenSource();
                        CurrentClient.DataReceiverToken = CurrentClient.DataReceiverTokenSource.Token;
                        Log("TCPAcceptConnections starting data receiver for " + CurrentClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)");
                        Task.Run(() => TCPDataReceiver(CurrentClient), CurrentClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        CurrentClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        CurrentClient.ProcessClientQueueToken = CurrentClient.ProcessClientQueueTokenSource.Token;
                        Log("TCPAcceptConnections starting queue processor for " + CurrentClient.IpPort());
                        Task.Run(() => ProcessClientQueue(CurrentClient), CurrentClient.ProcessClientQueueToken);

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

        private void TCPDataReceiver(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPDataReceiver null client supplied");
                    return;
                }

                if (CurrentClient.ClientTCPInterface == null)
                {
                    Log("*** TCPDataReceiver null TcpClient supplied within client");
                    return;
                }

                #endregion

                #region Wait-for-Data

                if (!CurrentClient.ClientTCPInterface.Connected)
                {
                    Log("*** TCPDataReceiver client " + CurrentClient.IpPort() + " is no longer connected");
                    return;
                }

                NetworkStream ClientStream = CurrentClient.ClientTCPInterface.GetStream();

                while (true)
                {
                    #region Check-if-Client-Connected

                    if (!CurrentClient.ClientTCPInterface.Connected || !Helper.IsTCPPeerConnected(CurrentClient.ClientTCPInterface))
                    {
                        Log("TCPDataReceiver client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** TCPDataReceiver unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** TCPDataReceiver unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        break;
                    }
                    else
                    {
                        // Log("TCPDataReceiver client " + CurrentClient.IpPort() + " is still connected");
                    }

                    #endregion

                    #region Retrieve-Message

                    Message CurrentMessage = Helper.TCPMessageRead(CurrentClient.ClientTCPInterface, (Config.Debug.Enable && (Config.Debug.Enable && Config.Debug.MsgResponseTime)));
                    if (CurrentMessage == null)
                    {
                        // Log("*** TCPDataReceiver unable to read from client " + CurrentClient.IpPort());
                        Thread.Sleep(30);
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPDataReceiver received message from " + CurrentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();
                    }

                    if (!CurrentMessage.IsValid())
                    {
                        Log("TCPDataReceiver invalid message received from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPDataReceiver verified message validity from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(CurrentClient, CurrentMessage);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPDataReceiver processed message from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(CurrentMessage));
                    // Log("TCPDataReceiver finished processing message from client " + CurrentClient.IpPort());

                    #endregion
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("TCPDataReceiver (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("TCPDataReceiver (null)", EOuter);
                }
            }
            finally
            {
                TCPActiveConnectionThreads--;
                Log("TCPDataReceiver closed data receiver for " + CurrentClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)");
                if (CurrentClient != null)
                {
                    try
                    {
                        CurrentClient.Close();
                    }
                    catch (Exception EClose)
                    {
                        LogException("TCPDataReceiver", EClose);
                    }
                }               
            }
        }

        private bool TCPDataSender(Client CurrentClient, Message CurrentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPDataSender null client supplied");
                    return false;
                }

                if (CurrentClient.ClientTCPInterface == null)
                {
                    Log("*** TCPDataSender null TcpClient supplied within client object for client " + CurrentClient.ClientGUID);
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** TCPDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.SenderGUID))
                {
                    Log("*** TCPDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
                {
                    Log("*** TCPDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (!Helper.IsTCPPeerConnected(CurrentClient.ClientTCPInterface))
                {
                    Log("TCPDataSender client " + CurrentClient.IpPort() + " not connected");
                    return false;
                }

                #endregion

                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(CurrentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Thread.Sleep(25);
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** TCPDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** TCPDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                if (!Helper.TCPMessageWrite(CurrentClient.ClientTCPInterface, CurrentMessage, (Config.Debug.Enable && Config.Debug.MsgResponseTime)))
                {
                    Log("TCPDataSender unable to send data to client " + CurrentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(CurrentMessage.Command))
                    {
                        Log("TCPDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command " + CurrentMessage.Command);
                    }
                    else
                    {
                        Log("TCPDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("TCPDataSender (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("TCPDataSender (null)", EOuter);
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
                    while (!ClientActiveSendMap.TryRemove(CurrentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Thread.Sleep(25);
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(CurrentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** TCPDataSender locked send map attempting to remove recipient GUID " + CurrentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void TCPHeartbeatManager(Client CurrentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Disable

                if (!Config.Heartbeat.Enable)
                {
                    Log("TCPHeartbeatManager disabled");
                    return;
                }

                if (Config.Heartbeat.IntervalMs == 0)
                {
                    Log("TCPHeartbeatManager disabled");
                    return;
                }

                #endregion

                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPHeartbeatManager null client supplied");
                    return;
                }

                if (CurrentClient.ClientTCPInterface == null)
                {
                    Log("*** TCPHeartbeatManager null TcpClient supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
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
                        Thread.Sleep(Config.Heartbeat.IntervalMs);
                    }

                    #endregion

                    #region Check-if-Client-Connected

                    if (!Helper.IsTCPPeerConnected(CurrentClient.ClientTCPInterface))
                    {
                        Log("TCPHeartbeatManager client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** TCPHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** TCPHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        return;
                    }

                    #endregion

                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message HeartbeatMessage = HeartbeatRequestMessage(CurrentClient);
                    if (!TCPDataSender(CurrentClient, HeartbeatMessage))
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** TCPHeartbeatManager failed to send heartbeat to client " + CurrentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** TCPHeartbeatManager maximum number of failed heartbeats reached, removing client " + CurrentClient.IpPort());

                            if (!RemoveClient(CurrentClient))
                            {
                                Log("*** TCPHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                            }

                            if (!RemoveClientChannels(CurrentClient))
                            {
                                Log("*** TCPHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                            }

                            if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));

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
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("TCPHeartbeatManager (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("TCPHeartbeatManager (null)", EOuter);
                }
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

                    TcpClient Client = TCPSSLListener.AcceptTcpClientAsync().Result;
                    Client.LingerState.Enabled = false;

                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string ClientIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                        int ClientPort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;
                        Log("TCPSSLAcceptConnections accepted connection from " + ClientIp + ":" + ClientPort);

                        #endregion

                        #region Initialize-and-Authenticate-as-Server

                        SslStream sslStream = null;
                        if (Config.AcceptInvalidSSLCerts)
                        {
                            sslStream = new SslStream(Client.GetStream(), false, new RemoteCertificateValidationCallback(TCPSSLValidateCert));
                        }
                        else
                        {
                            //
                            // do not accept invalid SSL certificates
                            //
                            sslStream = new SslStream(Client.GetStream(), false);
                        }

                        sslStream.AuthenticateAsServer(TCPSSLCertificate, true, SslProtocols.Tls, false);
                        Log("TCPSSLAcceptConnections SSL authentication complete with " + ClientIp + ":" + ClientPort);

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

                        Client CurrentClient = new Client();
                        CurrentClient.SourceIP = ClientIp;
                        CurrentClient.SourcePort = ClientPort;
                        CurrentClient.ClientTCPInterface = null;
                        CurrentClient.ClientTCPSSLInterface = Client;
                        CurrentClient.ClientSSLStream = sslStream;
                        CurrentClient.ClientHTTPContext = null;
                        CurrentClient.ClientWSContext = null;
                        CurrentClient.ClientWSInterface = null;
                        CurrentClient.ClientWSSSLContext = null;
                        CurrentClient.ClientWSSSLInterface = null;
                        CurrentClient.MessageQueue = new BlockingCollection<Message>();

                        CurrentClient.IsTCP = false;
                        CurrentClient.IsTCPSSL = true;
                        CurrentClient.IsWebsocket = false;
                        CurrentClient.IsWebsocketSSL = false;
                        CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        CurrentClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        if (!AddClient(CurrentClient))
                        {
                            Log("*** TCPSSLAcceptConnections unable to add client " + CurrentClient.IpPort());
                            Client.Close();
                            return;
                        }

                        #endregion

                        #region Start-Data-Receiver

                        Log("TCPSSLAcceptConnections starting data receiver for " + CurrentClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)");
                        CurrentClient.DataReceiverTokenSource = new CancellationTokenSource();
                        CurrentClient.DataReceiverToken = CurrentClient.DataReceiverTokenSource.Token;
                        Task.Run(() => TCPSSLDataReceiver(CurrentClient), CurrentClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        CurrentClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        CurrentClient.ProcessClientQueueToken = CurrentClient.ProcessClientQueueTokenSource.Token;
                        Log("TCPSSLAcceptConnections starting queue processor for " + CurrentClient.IpPort());
                        Task.Run(() => ProcessClientQueue(CurrentClient), CurrentClient.ProcessClientQueueToken);

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

        private void TCPSSLDataReceiver(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPSSLDataReceiver null client supplied");
                    return;
                }

                if (CurrentClient.ClientTCPSSLInterface == null)
                {
                    Log("*** TCPSSLDataReceiver null TcpClient supplied within client");
                    return;
                }

                if (CurrentClient.ClientSSLStream == null)
                {
                    Log("*** TCPSSLDataReceiver null SslStream supplied within client");
                    return;
                }

                if (!CurrentClient.ClientSSLStream.CanRead || !CurrentClient.ClientSSLStream.CanWrite)
                {
                    Log("*** TCPSSLDataReceiver supplied SslStream is either not readable or not writeable");
                    return;
                }

                #endregion

                #region Wait-for-Data

                if (!CurrentClient.ClientTCPSSLInterface.Connected)
                {
                    Log("*** TCPSSLDataReceiver client " + CurrentClient.IpPort() + " is no longer connected");
                    return;
                }

                NetworkStream ClientStream = CurrentClient.ClientTCPSSLInterface.GetStream();

                while (true)
                {
                    #region Check-if-Client-Connected

                    if (!CurrentClient.ClientTCPSSLInterface.Connected || !Helper.IsTCPPeerConnected(CurrentClient.ClientTCPSSLInterface))
                    {
                        Log("TCPSSLDataReceiver client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** TCPSSLDataReceiver unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** TCPSSLDataReceiver unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        break;
                    }
                    else
                    {
                        // Log("TCPSSLDataReceiver client " + CurrentClient.IpPort() + " is still connected");
                    }

                    #endregion

                    #region Retrieve-Message

                    Message CurrentMessage = Helper.TCPSSLMessageRead(CurrentClient.ClientTCPSSLInterface, CurrentClient.ClientSSLStream, (Config.Debug.Enable && (Config.Debug.Enable && Config.Debug.MsgResponseTime)));
                    if (CurrentMessage == null)
                    {
                        // Log("*** TCPSSLDataReceiver unable to read from client " + CurrentClient.IpPort());
                        Thread.Sleep(30);
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPSSLDataReceiver received message from " + CurrentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();
                    }

                    if (!CurrentMessage.IsValid())
                    {
                        Log("TCPSSLDataReceiver invalid message received from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPSSLDataReceiver verified message validity from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(CurrentClient, CurrentMessage);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("TCPSSLDataReceiver processed message from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(CurrentMessage));
                    // Log("TCPSSLDataReceiver finished processing message from client " + CurrentClient.IpPort());

                    #endregion
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("TCPSSLDataReceiver (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("TCPSSLDataReceiver (null)", EOuter);
                }
            }
            finally
            {
                TCPSSLActiveConnectionThreads--;
                Log("TCPSSLDataReceiver closed data receiver for " + CurrentClient.IpPort() + " (now " + TCPSSLActiveConnectionThreads + " connections active)");
                CurrentClient.Close();
            }
        }

        private bool TCPSSLDataSender(Client CurrentClient, Message CurrentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPSSLDataSender null client supplied");
                    return false;
                }

                if (CurrentClient.ClientTCPSSLInterface == null)
                {
                    Log("*** TCPSSLDataSender null TcpClient supplied within client object for client " + CurrentClient.ClientGUID);
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** TCPSSLDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.SenderGUID))
                {
                    Log("*** TCPSSLDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
                {
                    Log("*** TCPSSLDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (!Helper.IsTCPPeerConnected(CurrentClient.ClientTCPSSLInterface))
                {
                    Log("TCPSSLDataSender client " + CurrentClient.IpPort() + " not connected");
                    return false;
                }

                #endregion

                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(CurrentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Thread.Sleep(25);
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** TCPSSLDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** TCPSSLDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                if (!Helper.TCPSSLMessageWrite(CurrentClient.ClientTCPSSLInterface, CurrentClient.ClientSSLStream, CurrentMessage, (Config.Debug.Enable && Config.Debug.MsgResponseTime)))
                {
                    Log("TCPSSLDataSender unable to send data to client " + CurrentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(CurrentMessage.Command))
                    {
                        Log("TCPSSLDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command " + CurrentMessage.Command);
                    }
                    else
                    {
                        Log("TCPSSLDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("TCPSSLDataSender (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("TCPSSLDataSender (null)", EOuter);
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
                    while (!ClientActiveSendMap.TryRemove(CurrentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Thread.Sleep(25);
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(CurrentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** TCPSSLDataSender locked send map attempting to remove recipient GUID " + CurrentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void TCPSSLHeartbeatManager(Client CurrentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Disable

                if (!Config.Heartbeat.Enable)
                {
                    Log("TCPSSLHeartbeatManager disabled");
                    return;
                }

                if (Config.Heartbeat.IntervalMs == 0)
                {
                    Log("TCPSSLHeartbeatManager disabled");
                    return;
                }

                #endregion

                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** TCPSSLHeartbeatManager null client supplied");
                    return;
                }

                if (CurrentClient.ClientTCPSSLInterface == null)
                {
                    Log("*** TCPSSLHeartbeatManager null TcpClient supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
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
                        Thread.Sleep(Config.Heartbeat.IntervalMs);
                    }

                    #endregion

                    #region Check-if-Client-Connected

                    if (!Helper.IsTCPPeerConnected(CurrentClient.ClientTCPSSLInterface))
                    {
                        Log("TCPSSLHeartbeatManager client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** TCPSSLHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** TCPSSLHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        return;
                    }

                    #endregion

                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message HeartbeatMessage = HeartbeatRequestMessage(CurrentClient);
                    if (!TCPSSLDataSender(CurrentClient, HeartbeatMessage))
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** TCPSSLHeartbeatManager failed to send heartbeat to client " + CurrentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** TCPSSLHeartbeatManager maximum number of failed heartbeats reached, removing client " + CurrentClient.IpPort());

                            if (!RemoveClient(CurrentClient))
                            {
                                Log("*** TCPSSLHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                            }

                            if (!RemoveClientChannels(CurrentClient))
                            {
                                Log("*** TCPSSLHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                            }

                            if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));

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
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("TCPSSLHeartbeatManager (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("TCPSSLHeartbeatManager (null)", EOuter);
                }
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
                    HttpListenerContext Context = WSListener.GetContextAsync().Result;

                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string ClientIp = Context.Request.RemoteEndPoint.Address.ToString();
                        int ClientPort = Context.Request.RemoteEndPoint.Port;
                        Log("WSAcceptConnections accepted connection from " + ClientIp + ":" + ClientPort);

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
                            wsContext = Context.AcceptWebSocketAsync(subProtocol: null).Result;
                        }
                        catch (Exception)
                        {
                            Log("*** WSAcceptConnections exception while gathering websocket context for client " + ClientIp + ":" + ClientPort);
                            Context.Response.StatusCode = 500;
                            Context.Response.Close();
                            return;
                        }

                        WebSocket Client = wsContext.WebSocket;

                        #endregion

                        #region Add-to-Client-List

                        Client CurrentClient = new Client();
                        CurrentClient.SourceIP = ClientIp;
                        CurrentClient.SourcePort = ClientPort;
                        CurrentClient.ClientTCPInterface = null;
                        CurrentClient.ClientTCPSSLInterface = null;
                        CurrentClient.ClientHTTPContext = Context;
                        CurrentClient.ClientHTTPSSLContext = Context;
                        CurrentClient.ClientWSContext = wsContext;
                        CurrentClient.ClientWSInterface = Client;
                        CurrentClient.ClientWSSSLContext = wsContext;
                        CurrentClient.ClientWSSSLInterface = Client;
                        CurrentClient.MessageQueue = new BlockingCollection<Message>();

                        CurrentClient.IsTCP = false;
                        CurrentClient.IsTCPSSL = false;
                        CurrentClient.IsWebsocket = true;
                        CurrentClient.IsWebsocketSSL = false;
                        CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        CurrentClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        if (!AddClient(CurrentClient))
                        {
                            Log("*** WSAcceptConnections unable to add client " + CurrentClient.IpPort());
                            Context.Response.StatusCode = 500;
                            Context.Response.Close();
                            return;
                        }

                        #endregion

                        #region Start-Data-Receiver

                        Log("WSAcceptConnections starting data receiver for " + CurrentClient.IpPort() + " (now " + WSActiveConnectionThreads + " connections active)");
                        CurrentClient.DataReceiverTokenSource = new CancellationTokenSource();
                        CurrentClient.DataReceiverToken = CurrentClient.DataReceiverTokenSource.Token;
                        Task.Run(() => WSDataReceiver(CurrentClient), CurrentClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        CurrentClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        CurrentClient.ProcessClientQueueToken = CurrentClient.ProcessClientQueueTokenSource.Token;
                        Log("WSAcceptConnections starting queue processor for " + CurrentClient.IpPort());
                        Task.Run(() => ProcessClientQueue(CurrentClient), CurrentClient.ProcessClientQueueToken);

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

        private void WSDataReceiver(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** WSDataReceiver null client supplied");
                    return;
                }

                if (CurrentClient.ClientWSInterface == null)
                {
                    Log("*** WSDataReceiver null WebSocket supplied within client");
                    return;
                }

                #endregion

                #region Wait-for-Data

                while (true)
                {
                    #region Check-if-Client-Connected

                    if (!Helper.IsWSPeerConnected(CurrentClient.ClientWSInterface))
                    {
                        Log("WSDataReceiver client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** WSDataReceiver unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** WSDataReceiver unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        break;
                    }
                    else
                    {
                        // Log("TCPDataReceiver client " + CurrentClient.IpPort() + " is still connected");
                    }

                    #endregion

                    #region Retrieve-Message

                    Message CurrentMessage = Helper.WSMessageRead(CurrentClient.ClientHTTPContext, CurrentClient.ClientWSInterface, Config.Debug.MsgResponseTime).Result;
                    if (CurrentMessage == null)
                    {
                        Log("WSDataReceiver unable to read message from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSDataReceiver received message from " + CurrentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();

                        Task.Run(() => MessageReceived(CurrentMessage));
                    }

                    if (!CurrentMessage.IsValid())
                    {
                        Log("WSDataReceiver invalid message received from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSDataReceiver verified message validity from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(CurrentClient, CurrentMessage);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSDataReceiver processed message from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(CurrentMessage));

                    #endregion
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("WSDataReceiver (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("WSDataReceiver (null)", EOuter);
                }
            }
            finally
            {
                WSActiveConnectionThreads--;
                Log("WSDataReceiver closed data receiver for " + CurrentClient.IpPort() + " (now " + WSActiveConnectionThreads + " connections active)");
                CurrentClient.Close();
            }
        }

        private bool WSDataSender(Client CurrentClient, Message CurrentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** WSDataSender null client supplied");
                    return false;
                }

                if (CurrentClient.ClientWSInterface == null)
                {
                    Log("*** WSDataSender null websocket supplied within client object for client " + CurrentClient.ClientGUID);
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** WSDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.SenderGUID))
                {
                    Log("*** WSDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
                {
                    Log("*** WSDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (!Helper.IsWSPeerConnected(CurrentClient.ClientWSInterface))
                {
                    Log("WSDataSender client " + CurrentClient.IpPort() + " not connected");
                    return false;
                }

                #endregion

                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(CurrentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Thread.Sleep(25);
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** WSDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** WSDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                bool success = Helper.WSMessageWrite(CurrentClient.ClientHTTPContext, CurrentClient.ClientWSInterface, CurrentMessage, Config.Debug.MsgResponseTime).Result;
                if (!success)
                {
                    Log("WSDataSender unable to send data to client " + CurrentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(CurrentMessage.Command))
                    {
                        Log("WSDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command " + CurrentMessage.Command);
                    }
                    else
                    {
                        Log("WSDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("WSDataSender (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("WSDataSender (null)", EOuter);
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
                    while (!ClientActiveSendMap.TryRemove(CurrentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Thread.Sleep(25);
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(CurrentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** WSDataSender locked send map attempting to remove recipient GUID " + CurrentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void WSHeartbeatManager(Client CurrentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Disable

                if (!Config.Heartbeat.Enable)
                {
                    Log("WSHeartbeatManager disabled");
                    return;
                }

                if (Config.Heartbeat.IntervalMs == 0)
                {
                    Log("WSHeartbeatManager disabled");
                    return;
                }

                #endregion

                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** WSHeartbeatManager null client supplied");
                    return;
                }

                if (CurrentClient.ClientWSInterface == null)
                {
                    Log("*** WSHeartbeatManager null websocket supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
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
                        Thread.Sleep(Config.Heartbeat.IntervalMs);
                    }

                    #endregion

                    #region Check-if-Client-Connected

                    if (!Helper.IsWSPeerConnected(CurrentClient.ClientWSInterface))
                    {
                        Log("WSHeartbeatManager client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** WSHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** WSHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        return;
                    }

                    #endregion

                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message HeartbeatMessage = HeartbeatRequestMessage(CurrentClient);
                    bool success = Helper.WSMessageWrite(CurrentClient.ClientHTTPContext, CurrentClient.ClientWSInterface, HeartbeatMessage, Config.Debug.MsgResponseTime).Result;
                    if (!success)
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** WSHeartbeatManager failed to send heartbeat to client " + CurrentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** WSHeartbeatManager maximum number of failed heartbeats reached, removing client " + CurrentClient.IpPort());

                            if (!RemoveClient(CurrentClient))
                            {
                                Log("*** WSHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                            }

                            if (!RemoveClientChannels(CurrentClient))
                            {
                                Log("*** WSHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                            }

                            if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));

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
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("WSHeartbeatManager (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("WSHeartbeatManager (null)", EOuter);
                }
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
                    HttpListenerContext Context = WSSSLListener.GetContextAsync().Result;

                    Task.Run(() =>
                    {
                        #region Get-Tuple

                        string ClientIp = Context.Request.RemoteEndPoint.Address.ToString();
                        int ClientPort = Context.Request.RemoteEndPoint.Port;
                        Log("WSSSLAcceptConnections accepted connection from " + ClientIp + ":" + ClientPort);

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
                            wsContext = Context.AcceptWebSocketAsync(subProtocol: null).Result;
                        }
                        catch (Exception)
                        {
                            Log("*** WSSSLAcceptConnections exception while gathering websocket context for client " + ClientIp + ":" + ClientPort);
                            Context.Response.StatusCode = 500;
                            Context.Response.Close();
                            return;
                        }

                        WebSocket Client = wsContext.WebSocket;

                        #endregion

                        #region Add-to-Client-List

                        Client CurrentClient = new Client();
                        CurrentClient.SourceIP = ClientIp;
                        CurrentClient.SourcePort = ClientPort;
                        CurrentClient.ClientTCPInterface = null;
                        CurrentClient.ClientTCPSSLInterface = null;
                        CurrentClient.ClientHTTPContext = null;
                        CurrentClient.ClientWSContext = null;
                        CurrentClient.ClientWSInterface = null;
                        CurrentClient.ClientHTTPSSLContext = Context;
                        CurrentClient.ClientWSSSLContext = wsContext;
                        CurrentClient.ClientWSSSLInterface = Client;
                        CurrentClient.MessageQueue = new BlockingCollection<Message>();

                        CurrentClient.IsTCP = false;
                        CurrentClient.IsTCPSSL = false;
                        CurrentClient.IsWebsocket = false;
                        CurrentClient.IsWebsocketSSL = true;
                        CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        CurrentClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        if (!AddClient(CurrentClient))
                        {
                            Log("*** WSSSLAcceptConnections unable to add client " + CurrentClient.IpPort());
                            Context.Response.StatusCode = 500;
                            Context.Response.Close();
                            return;
                        }

                        #endregion

                        #region Start-Data-Receiver

                        Log("WSSSLAcceptConnections starting data receiver for " + CurrentClient.IpPort() + " (now " + WSSSLActiveConnectionThreads + " connections active)");
                        CurrentClient.DataReceiverTokenSource = new CancellationTokenSource();
                        CurrentClient.DataReceiverToken = CurrentClient.DataReceiverTokenSource.Token;
                        Task.Run(() => WSSSLDataReceiver(CurrentClient), CurrentClient.DataReceiverToken);

                        #endregion

                        #region Start-Queue-Processor

                        CurrentClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
                        CurrentClient.ProcessClientQueueToken = CurrentClient.ProcessClientQueueTokenSource.Token;
                        Log("WSSSLAcceptConnections starting queue processor for " + CurrentClient.IpPort());
                        Task.Run(() => ProcessClientQueue(CurrentClient), CurrentClient.ProcessClientQueueToken);

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

        private void WSSSLDataReceiver(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** WSSSLDataReceiver null client supplied");
                    return;
                }

                if (CurrentClient.ClientWSSSLInterface == null)
                {
                    Log("*** WSSSLDataReceiver null WebSocket supplied within client");
                    return;
                }

                #endregion

                #region Wait-for-Data

                while (true)
                {
                    #region Check-if-Client-Connected

                    if (!Helper.IsWSPeerConnected(CurrentClient.ClientWSSSLInterface))
                    {
                        Log("WSSSLDataReceiver client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** WSSSLDataReceiver unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** WSSSLDataReceiver unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        break;
                    }
                    else
                    {
                        // Log("WSSSLDataReceiver client " + CurrentClient.IpPort() + " is still connected");
                    }

                    #endregion

                    #region Retrieve-Message

                    Message CurrentMessage = Helper.WSMessageRead(CurrentClient.ClientHTTPSSLContext, CurrentClient.ClientWSSSLInterface, Config.Debug.MsgResponseTime).Result;
                    if (CurrentMessage == null)
                    {
                        Log("WSSSLDataReceiver unable to read message from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSSSLDataReceiver received message from " + CurrentClient.IpPort() + " after " + sw.Elapsed.TotalMilliseconds + "ms of inactivity, resetting stopwatch");
                        sw.Reset();
                        sw.Start();

                        Task.Run(() => MessageReceived(CurrentMessage));
                    }

                    if (!CurrentMessage.IsValid())
                    {
                        Log("WSSSLDataReceiver invalid message received from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSSSLDataReceiver verified message validity from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(CurrentClient, CurrentMessage);
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("WSSSLDataReceiver processed message from " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
                    sw.Reset();
                    sw.Start();

                    if (MessageReceived != null) Task.Run(() => MessageReceived(CurrentMessage));

                    #endregion
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("WSSSLDataReceiver (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("WSSSLDataReceiver (null)", EOuter);
                }
            }
            finally
            {
                WSSSLActiveConnectionThreads--;
                Log("WSSSLDataReceiver closed data receiver for " + CurrentClient.IpPort() + " (now " + WSSSLActiveConnectionThreads + " connections active)");
                CurrentClient.Close();
            }
        }

        private bool WSSSLDataSender(Client CurrentClient, Message CurrentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** WSSSLDataSender null client supplied");
                    return false;
                }

                if (CurrentClient.ClientWSSSLInterface == null)
                {
                    Log("*** WSSSLDataSender null websocket supplied within client object for client " + CurrentClient.ClientGUID);
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** WSSSLDataSender null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.SenderGUID))
                {
                    Log("*** WSSSLDataSender null sender GUID in supplied message");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
                {
                    Log("*** WSSSLDataSender null recipient GUID in supplied message");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (!Helper.IsWSPeerConnected(CurrentClient.ClientWSSSLInterface))
                {
                    Log("WSSSLDataSender client " + CurrentClient.IpPort() + " not connected");
                    return false;
                }

                #endregion

                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!ClientActiveSendMap.TryAdd(CurrentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Thread.Sleep(25);
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        Log("*** WSSSLDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        Log("*** WSSSLDataSender locked send map attempting to add recipient GUID " + CurrentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                bool success = Helper.WSMessageWrite(CurrentClient.ClientHTTPSSLContext, CurrentClient.ClientWSSSLInterface, CurrentMessage, Config.Debug.MsgResponseTime).Result;
                if (!success)
                {
                    Log("WSSSLDataSender unable to send data to client " + CurrentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(CurrentMessage.Command))
                    {
                        Log("WSSSLDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command " + CurrentMessage.Command);
                    }
                    else
                    {
                        Log("WSSSLDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command (null)");
                    }
                }

                #endregion

                return true;
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("WSSSLDataSender (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("WSSSLDataSender (null)", EOuter);
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
                    while (!ClientActiveSendMap.TryRemove(CurrentMessage.RecipientGUID, out removedVal))
                    {
                        //
                        // wait
                        //

                        Thread.Sleep(25);
                        removeLoopCount += 25;

                        if (!ClientActiveSendMap.ContainsKey(CurrentMessage.RecipientGUID))
                        {
                            //
                            // there was (temporarily) a conflict that has been resolved
                            //
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            Log("*** WSSSLDataSender locked send map attempting to remove recipient GUID " + CurrentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
            }
        }

        private void WSSSLHeartbeatManager(Client CurrentClient)
        {
            //
            //
            // Should only be called after client login
            //
            //

            try
            {
                #region Check-for-Disable

                if (!Config.Heartbeat.Enable)
                {
                    Log("WSSSLHeartbeatManager disabled");
                    return;
                }

                if (Config.Heartbeat.IntervalMs == 0)
                {
                    Log("WSSSLHeartbeatManager disabled");
                    return;
                }

                #endregion

                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** WSSSLHeartbeatManager null client supplied");
                    return;
                }

                if (CurrentClient.ClientWSSSLInterface == null)
                {
                    Log("*** WSSSLHeartbeatManager null websocket supplied within client");
                    return;
                }

                if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
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
                        Thread.Sleep(Config.Heartbeat.IntervalMs);
                    }

                    #endregion

                    #region Check-if-Client-Connected

                    if (!Helper.IsWSPeerConnected(CurrentClient.ClientWSSSLInterface))
                    {
                        Log("WSSSLHeartbeatManager client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** WSSSLHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** WSSSLHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        return;
                    }

                    #endregion

                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    Message HeartbeatMessage = HeartbeatRequestMessage(CurrentClient);
                    bool success = Helper.WSMessageWrite(CurrentClient.ClientHTTPSSLContext, CurrentClient.ClientWSSSLInterface, HeartbeatMessage, Config.Debug.MsgResponseTime).Result;
                    if (!success)
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** WSSSLHeartbeatManager failed to send heartbeat to client " + CurrentClient.IpPort() + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            Log("*** WSSSLHeartbeatManager maximum number of failed heartbeats reached, removing client " + CurrentClient.IpPort());

                            if (!RemoveClient(CurrentClient))
                            {
                                Log("*** WSSSLHeartbeatManager unable to remove client " + CurrentClient.IpPort());
                            }

                            if (!RemoveClientChannels(CurrentClient))
                            {
                                Log("*** WSSSLHeartbeatManager unable to remove channels associated with client " + CurrentClient.IpPort());
                            }

                            if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(CurrentClient));

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
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("WSSSLHeartbeatManager (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("WSSSLHeartbeatManager (null)", EOuter);
                }
            }
        }

        #endregion

        #endregion

        //
        // Methods below are transport agnostic
        //

        #region Private-Event-Methods

        private bool ServerJoinEvent(Client CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** ServerJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
            {
                Log("*** ServerJoinEvent null ClientGUID suplied within Client");
                return true;
            }

            Log("ServerJoinEvent sending server join notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGUID);

            List<Client> CurrentServerClients = GetAllClients(); 
            if (CurrentServerClients == null || CurrentServerClients.Count < 1)
            {
                Log("*** ServerJoinEvent no clients found on server");
                return true;
            }

            Message Message = ServerJoinEventMessage(CurrentClient);

            foreach (Client curr in CurrentServerClients)
            {
                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGUID = curr.ClientGUID;
                        bool ResponseSuccess = QueueClientMessage(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ServerJoinEvent error queuing server join event to " + Message.RecipientGUID + " (join by " + CurrentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ServerLeaveEvent(Client CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** ServerLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
            {
                Log("*** ServerLeaveEvent null ClientGUID suplied within Client");
                return true;
            }

            Log("ServerLeaveEvent sending server leave notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGUID);

            List<Client> CurrentServerClients = GetAllClients();
            if (CurrentServerClients == null || CurrentServerClients.Count < 1)
            {
                Log("*** ServerLeaveEvent no clients found on server");
                return true;
            }

            Message Message = ServerLeaveEventMessage(CurrentClient);

            foreach (Client curr in CurrentServerClients)
            {
                if (!String.IsNullOrEmpty(curr.ClientGUID))
                {
                    if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                    {
                        /*
                        Task.Run(() =>
                        {
                            Message.RecipientGUID = curr.ClientGUID;
                            bool ResponseSuccess = TCPDataSender(curr, Message);
                            if (!ResponseSuccess)
                            {
                                Log("*** ServerLeaveEvent error sending server leave event to " + Message.RecipientGUID + " (leave by " + CurrentClient.ClientGUID + ")");
                            }
                        });
                        */
                        Message.RecipientGUID = curr.ClientGUID;
                        bool ResponseSuccess = QueueClientMessage(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ServerLeaveEvent error queuing server leave event to " + Message.RecipientGUID + " (leave by " + CurrentClient.ClientGUID + ")");
                        }
                        else
                        {
                            Log("ServerLeaveEvent queued server leave event to " + Message.RecipientGUID + " (leave by " + CurrentClient.ClientGUID + ")");
                        }
                    }
                }
            }

            return true;
        }

        private bool ChannelJoinEvent(Client CurrentClient, Channel CurrentChannel)
        {
            if (CurrentClient == null)
            {
                Log("*** ChannelJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
            {
                Log("*** ChannelJoinEvent null ClientGUID supplied within Client");
                return true;
            }

            if (CurrentChannel == null)
            {
                Log("*** ChannelJoinEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentChannel.Guid))
            {
                Log("*** ChannelJoinEvent null GUID supplied within Channel");
                return true;
            }

            Log("ChannelJoinEvent sending channel join notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGUID + " channel " + CurrentChannel.Guid);

            List<Client> CurrentChannelClients = GetChannelMembers(CurrentChannel.Guid);
            if (CurrentChannelClients == null || CurrentChannelClients.Count < 1)
            {
                Log("*** ChannelJoinEvent no clients found in channel " + CurrentChannel.Guid);
                return true;
            }

            Message Message = ChannelJoinEventMessage(CurrentChannel, CurrentClient);

            foreach (Client curr in CurrentChannelClients)
            {
                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGUID = curr.ClientGUID;
                        bool ResponseSuccess = QueueClientMessage(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ChannelJoinEvent error queuing channel join event to " + Message.RecipientGUID + " for channel " + Message.ChannelGUID + " (join by " + CurrentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ChannelLeaveEvent(Client CurrentClient, Channel CurrentChannel)
        {
            if (CurrentClient == null)
            {
                Log("*** ChannelLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
            {
                Log("*** ChannelLeaveEvent null ClientGUID supplied within Client");
                return true;
            }

            if (CurrentChannel == null)
            {
                Log("*** ChannelLeaveEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentChannel.Guid))
            {
                Log("*** ChannelLeaveEvent null GUID supplied within Channel");
                return true;
            }

            Log("ChannelLeaveEvent sending channel leave notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGUID + " channel " + CurrentChannel.Guid);

            List<Client> CurrentChannelClients = GetChannelMembers(CurrentChannel.Guid);
            if (CurrentChannelClients == null || CurrentChannelClients.Count < 1)
            {
                Log("*** ChannelLeaveEvent no clients found in channel " + CurrentChannel.Guid);
                return true;
            }

            Message Message = ChannelLeaveEventMessage(CurrentChannel, CurrentClient);

            foreach (Client curr in CurrentChannelClients)
            {
                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGUID = curr.ClientGUID;
                        bool ResponseSuccess = QueueClientMessage(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ChannelLeaveEvent error queuing channel leave event to " + Message.RecipientGUID + " for channel " + Message.ChannelGUID + " (leave by " + CurrentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool SubscriberJoinEvent(Client CurrentClient, Channel CurrentChannel)
        {
            if (CurrentClient == null)
            {
                Log("*** SubscriberJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
            {
                Log("*** SubscriberJoinEvent null ClientGUID supplied within Client");
                return true;
            }

            if (CurrentChannel == null)
            {
                Log("*** SubscriberJoinEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentChannel.Guid))
            {
                Log("*** SubscriberJoinEvent null GUID supplied within Channel");
                return true;
            }

            Log("SubscriberJoinEvent sending subcriber join notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGUID + " channel " + CurrentChannel.Guid);

            List<Client> CurrentChannelClients = GetChannelSubscribers(CurrentChannel.Guid);
            if (CurrentChannelClients == null || CurrentChannelClients.Count < 1)
            {
                Log("*** SubscriberJoinEvent no clients found in channel " + CurrentChannel.Guid);
                return true;
            }

            Message Message = SubscriberJoinEventMessage(CurrentChannel, CurrentClient);

            foreach (Client curr in CurrentChannelClients)
            {
                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGUID = curr.ClientGUID;
                        bool ResponseSuccess = QueueClientMessage(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** SubscriberJoinEvent error queuing subscriber join event to " + Message.RecipientGUID + " for channel " + Message.ChannelGUID + " (join by " + CurrentClient.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool SubscriberLeaveEvent(Client CurrentClient, Channel CurrentChannel)
        {
            if (CurrentClient == null)
            {
                Log("*** SubscriberLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
            {
                Log("*** SubscriberLeaveEvent null ClientGUID supplied within Client");
                return true;
            }

            if (CurrentChannel == null)
            {
                Log("*** SubscriberLeaveEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentChannel.Guid))
            {
                Log("*** SubscriberLeaveEvent null GUID supplied within Channel");
                return true;
            }

            Log("SubscriberLeaveEvent sending subscriber leave notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGUID + " channel " + CurrentChannel.Guid);

            List<Client> CurrentChannelClients = GetChannelSubscribers(CurrentChannel.Guid);
            if (CurrentChannelClients == null || CurrentChannelClients.Count < 1)
            {
                Log("*** SubscriberLeaveEvent no clients found in channel " + CurrentChannel.Guid);
                return true;
            }

            Message Message = SubscriberLeaveEventMessage(CurrentChannel, CurrentClient);

            foreach (Client curr in CurrentChannelClients)
            {
                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGUID = curr.ClientGUID;
                        bool ResponseSuccess = QueueClientMessage(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** SubscriberLeaveEvent error queuing subscriber leave event to " + Message.RecipientGUID + " for channel " + Message.ChannelGUID + " (leave by " + CurrentClient.ClientGUID + ")");
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
                        Thread.Sleep(5000);
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
            catch (Exception EOuter)
            {
                LogException("MonitorUsersFile", EOuter);
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
                        Thread.Sleep(5000);
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
            catch (Exception EOuter)
            {
                LogException("MonitorPermissionsFile", EOuter);
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
            catch (Exception EOuter)
            {
                LogException("AllowConnection", EOuter);
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
            catch (Exception EOuter)
            {
                LogException("GetUser", EOuter);
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
            catch (Exception EOuter)
            {
                LogException("GetUserPermissions", EOuter);
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
            catch (Exception EOuter)
            {
                LogException("GetPermission", EOuter);
                return null;
            }
        }

        private bool AuthorizeMessage(Message CurrentMessage)
        {
            try
            {
                #region Check-for-Null-Values

                if (CurrentMessage == null)
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

                if (!String.IsNullOrEmpty(CurrentMessage.Email))
                {
                    #region Authenticate-Credentials

                    User currUser = GetUser(CurrentMessage.Email);
                    if (currUser == null)
                    {
                        Log("*** AuthenticateUser unable to find user " + CurrentMessage.Email);
                        return false;
                    }

                    if (!String.IsNullOrEmpty(currUser.Password))
                    {
                        if (String.Compare(currUser.Password, CurrentMessage.Password) != 0)
                        {
                            Log("*** AuthenticateUser invalid password supplied for user " + CurrentMessage.Email);
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

                    if (String.IsNullOrEmpty(CurrentMessage.Command))
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

                    if (currPermission.Permissions.Contains(CurrentMessage.Command))
                    {
                        // Log("AuthorizeMessage found permission for command " + CurrentMessage.Command + " in permission " + currUser.Permission + " for user " + currUser.Email);
                        return true;
                    }
                    else
                    {
                        Log("*** AuthorizeMessage permission " + currPermission.Name + " does not contain command " + CurrentMessage.Command + " for user " + currUser.Email);
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
            catch (Exception EOuter)
            {
                LogException("AuthorizeMessage", EOuter);
                return false;
            }
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
                        Thread.Sleep(5000);
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
                                        Thread.Sleep(1000);
                                        elapsed += 1000;
                                    }
                                }
                            });
                        }
                    }
                    
                    #endregion
                }
            }
            catch (Exception EOuter)
            {
                LogException("CleanupTask", EOuter);
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

        private Client GetClientByGuid(string guid)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid))
                {
                    Log("*** GetClientByGuid null GUID supplied");
                    return null;
                }

                if (Clients == null || Clients.Count < 1)
                {
                    Log("*** GetClientByGuid no clients");
                    return null;
                }

                if (ClientGUIDMap == null || ClientGUIDMap.Count < 1)
                {
                    Log("*** GetClientByGuid no GUID map entries");
                    return null;
                }

                string ipPort = null;
                if (ClientGUIDMap.TryGetValue(guid, out ipPort))
                {
                    if (!String.IsNullOrEmpty(ipPort))
                    {
                        Client existingClient = null;
                        if (Clients.TryGetValue(ipPort, out existingClient))
                        {
                            if (existingClient != null)
                            {
                                Log("GetClientByGuid returning client with GUID " + guid);
                                return existingClient;
                            }
                        }
                    }
                }

                Log("*** GetClientByGuid unable to find client by GUID " + guid);
                return null;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetClientByGuid " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<Client> GetAllClients()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (Clients == null || Clients.Count < 1)
                {
                    Log("*** GetAllClients no clients");
                    return null;
                }

                List<Client> ret = new List<Client>();
                foreach (KeyValuePair<string, Client> curr in Clients)
                {
                    if (!String.IsNullOrEmpty(curr.Value.ClientGUID))
                    {
                        /*
                        if (curr.Value.IsTCP) aConsole.WriteLine("GetAllClients adding TCP client " + curr.Value.IpPort() + " GUID " + curr.Value.ClientGUID + " to list");
                        else if (curr.Value.IsWebsocket) aConsole.WriteLine("GetAllClients adding websocket client " + curr.Value.IpPort() + " GUID " + curr.Value.ClientGUID + " to list");
                        else aConsole.WriteLine("GetAllClients adding unknown client " + curr.Value.IpPort() + " GUID " + curr.Value.ClientGUID + " to list");
                         */

                        ret.Add(curr.Value);
                    }
                }

                Log("GetAllClients returning " + ret.Count + " clients");
                return ret;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetAllClients " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }
        
        private Dictionary<string, string> GetAllClientGUIDMaps()
        {
            if (ClientGUIDMap == null || ClientGUIDMap.Count < 1) return new Dictionary<string, string>();
            Dictionary<string, string> ret = ClientGUIDMap.ToDictionary(entry => entry.Key, entry => entry.Value);
            return ret;
        }

        private Dictionary<string, DateTime> GetAllClientActiveSendMap()
        {
            if (ClientActiveSendMap == null || ClientActiveSendMap.Count < 1) return new Dictionary<string, DateTime>();
            Dictionary<string, DateTime> ret = ClientActiveSendMap.ToDictionary(entry => entry.Key, entry => entry.Value);
            return ret;
        }

        private Channel GetChannelByGuid(string guid)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid))
                {
                    Log("*** GetChannelByGuid null GUID supplied");
                    return null;
                }

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** GetChannelByGuid no channels found");
                    return null;
                }

                Channel ret = null;
                if (Channels.TryGetValue(guid, out ret))
                {
                    if (ret != null)
                    {
                        Log("GetChannelByGuid returning channel " + guid);
                        return ret;
                    }
                    else
                    {
                        Log("*** GetChannelByGuid unable to find channel with GUID " + guid);
                        return null;
                    }
                }

                Log("*** GetChannelByGuid unable to find channel with GUID " + guid);
                return null;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetChannelByGuid " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<Channel> GetAllChannels()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** GetAllChannels no Channels");
                    return null;
                }

                List<Channel> ret = new List<Channel>();
                foreach (KeyValuePair<string, Channel> curr in Channels)
                {
                    ret.Add(curr.Value);
                }

                Log("GetAllChannels returning " + ret.Count + " channels");
                return ret;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetAllChannels " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<Client> GetChannelMembers(string guid)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid))
                {
                    Log("*** GetChannelMembers null GUID supplied");
                    return null;
                }

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** GetChannelMembers no Channels");
                    return null;
                }

                List<Client> ret = new List<Client>();

                foreach (KeyValuePair<string, Channel> curr in Channels)
                {
                    if (String.Compare(curr.Value.Guid, guid) == 0)
                    {
                        foreach (Client CurrentClient in curr.Value.Members)
                        {
                            ret.Add(CurrentClient);
                        }
                    }
                }

                Log("GetChannelMembers returning " + ret.Count + " members");
                return ret;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetChannelMembers " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<Client> GetChannelSubscribers(string guid)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid))
                {
                    Log("*** GetChannelSubscribers null GUID supplied");
                    return null;
                }

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** GetChannelSubscribers no Channels");
                    return null;
                }

                List<Client> ret = new List<Client>();

                foreach (KeyValuePair<string, Channel> curr in Channels)
                {
                    if (String.Compare(curr.Value.Guid, guid) == 0)
                    {
                        foreach (Client CurrentClient in curr.Value.Subscribers)
                        {
                            ret.Add(CurrentClient);
                        }
                    }
                }

                Log("GetChannelSubscribers returning " + ret.Count + " subscribers");
                return ret;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetChannelSubscribers " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Channel GetChannelByName(string name)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(name))
                {
                    Log("*** GetChannelByName null name supplied");
                    return null;
                }

                Channel ret = null;
                
                foreach (KeyValuePair<string, Channel> curr in Channels)
                {
                    if (String.IsNullOrEmpty(curr.Value.ChannelName)) continue;

                    if (String.Compare(curr.Value.ChannelName.ToLower(), name.ToLower()) == 0)
                    {
                        ret = curr.Value;
                        break;
                    }
                }
                
                return ret;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetChannelByName " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool AddClient(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentClient == null)
                {
                    Log("*** AddClient null client supplied");
                    return false;
                }

                if (CurrentClient.IsTCP) Log("AddClient adding TCP client " + CurrentClient.IpPort() + " with " + Clients.Count + " entries in client list");
                else if (CurrentClient.IsTCPSSL) Log("AddClient adding TCP SSL client " + CurrentClient.IpPort() + " with " + Clients.Count + " entries in client list");
                else if (CurrentClient.IsWebsocket) Log("AddClient adding websocket client " + CurrentClient.IpPort() + " with " + Clients.Count + " entries in client list");
                else if (CurrentClient.IsWebsocketSSL) Log("AddClient adding websocket SSL client " + CurrentClient.IpPort() + " with " + Clients.Count + " entries in client list");
                else Log("AddClient adding UNKNOWN client " + CurrentClient.IpPort() + " with " + Clients.Count + " entries in client list");

                Client removedClient = null;
                if (Clients.TryRemove(CurrentClient.IpPort(), out removedClient))
                {
                    Log("AddClient removed previous existing client entry for " + CurrentClient.IpPort());
                }
                
                if (!Clients.TryAdd(CurrentClient.IpPort(), CurrentClient))
                {
                    Log("*** AddClient unable to add replacement client entry for " + CurrentClient.IpPort());
                    return false;
                }

                Log("AddClient " + CurrentClient.IpPort() + " exiting with " + Clients.Count + " entries in client list");
                if (ClientConnected != null) Task.Run(() => ClientConnected(CurrentClient));
                return true;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("AddClient " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveClient(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentClient == null)
                {
                    Log("*** RemoveClient null client supplied");
                    return false;
                }

                Log("RemoveClient removing client " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID);

                //
                // remove client entry
                //
                if (Clients.ContainsKey(CurrentClient.IpPort()))
                {
                    Client removedClient = null;
                    if (!Clients.TryRemove(CurrentClient.IpPort(), out removedClient))
                    {
                        Log("*** Unable to remove client " + CurrentClient.IpPort());
                        return false;
                    }
                }

                //
                // remove client GUID map entry
                //
                if (!String.IsNullOrEmpty(CurrentClient.ClientGUID))
                {
                    string removedMap = null;
                    if (ClientGUIDMap.TryRemove(CurrentClient.ClientGUID, out removedMap))
                    {
                        Log("RemoveClient removed client GUID map for GUID " + CurrentClient.ClientGUID + " and tuple " + CurrentClient.IpPort());
                    }
                }
                
                Log("RemoveClient exiting with " + Clients.Count + " client entries and " + ClientGUIDMap.Count + " GUID map entries");
                if (ClientDisconnected != null) Task.Run(() => ClientDisconnected(CurrentClient));
                return true;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("RemoveClient " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveClientChannels(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentClient == null)
                {
                    Log("*** RemoveClientChannels null client supplied");
                    return false;
                }
                
                if (Channels == null || Channels.Count < 1)
                {
                    Log("RemoveClientChannels no channels");
                    return true;
                }

                List<string> removeKeys = new List<string>();

                foreach (KeyValuePair<string, Channel> curr in Channels)
                {
                    if (String.Compare(curr.Value.OwnerGuid, CurrentClient.ClientGUID) == 0)
                    {
                        #region Match

                        if (curr.Value.Subscribers != null)
                        {
                            if (curr.Value.Subscribers.Count > 0)
                            {
                                //
                                // create another reference in case list is modified
                                //
                                Channel TempChannel = curr.Value;
                                List<Client> TempSubscribers = new List<Client>(curr.Value.Subscribers);

                                Task.Run(() =>
                                {
                                    foreach (Client Client in TempSubscribers)
                                    {
                                        if (String.Compare(Client.ClientGUID, TempChannel.OwnerGuid) != 0)
                                        {
                                            Log("RemoveClientChannels notifying channel " + TempChannel.Guid + " subscriber " + Client.ClientGUID + " of channel deletion");
                                            Task.Run(() =>
                                            {
                                                SendSystemMessage(ChannelDeletedByOwnerMessage(Client, TempChannel));
                                            });
                                        }
                                    }
                                });
                            }
                        }

                        removeKeys.Add(curr.Key);

                        #endregion
                    }
                }

                if (removeKeys.Count > 0)
                {
                    foreach (string curr in removeKeys)
                    {
                        Channel removeChannel = null;
                        if (!Channels.TryRemove(curr, out removeChannel))
                        {
                            Log("*** RemoveClientChannels unable to remove client channel " + curr + " for client " + CurrentClient.ClientGUID);
                        }
                    }
                }

                return true;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("RemoveClientChannels " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool UpdateClient(Client CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** UpdateClient null client supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
                {
                    Log("UpdateClient " + CurrentClient.IpPort() + " cannot update without a client GUID (login required)");
                    return false;
                }

                Log("UpdateClient " + CurrentClient.IpPort() + " entering with " + Clients.Count + " clients and " + ClientGUIDMap.Count + " GUID maps");

                #endregion

                #region Remove-Existing-Client-GUID-Map

                bool inMap = false;
                string inMapIPPort = "";
                string removedMap = "";

                if (ClientGUIDMap != null)
                {
                    if (ClientGUIDMap.TryGetValue(CurrentClient.ClientGUID, out inMapIPPort))
                    {
                        if (!String.IsNullOrEmpty(inMapIPPort))
                        {
                            Log("UpdateClient found existing client GUID map for GUID " + CurrentClient.ClientGUID + ": " + inMapIPPort);
                            inMap = true;
                        }
                    }
                }

                if (inMap)
                {
                    if (!ClientGUIDMap.TryRemove(CurrentClient.ClientGUID, out removedMap))
                    {
                        Log("*** UpdateClient unable to remove client GUID map for GUID " + CurrentClient.ClientGUID);
                    }
                }

                #endregion

                #region Remove-Existing-Client-Entry

                Client removedClient = null;
                if (inMap)
                {
                    if (!Clients.TryRemove(inMapIPPort, out removedClient))
                    {
                        Log("*** UpdateClient unable to remove client from clients list for GUID " + CurrentClient.ClientGUID);
                    }
                }

                #endregion

                #region Add-Updated-Client-Entry

                Client existingClient = null;
                if (Clients.TryGetValue(CurrentClient.IpPort(), out existingClient))
                {
                    if (existingClient == null)
                    {
                        #region New-Entry

                        Log("*** UpdateClient " + CurrentClient.IpPort() + " unable to retieve existing entry, adding new");
                        
                        if (!Clients.TryAdd(CurrentClient.IpPort(), CurrentClient))
                        {
                            Log("*** UpdateClient " + CurrentClient.IpPort() + " unable to add new entry");
                            return false;
                        }
                        
                        #endregion
                    }
                    else
                    {
                        #region Existing-Entry

                        existingClient.Email = CurrentClient.Email;
                        existingClient.Password = CurrentClient.Password;
                        existingClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        //
                        // preserve TCP and websocket context/interface
                        //
                        existingClient.ClientHTTPContext = CurrentClient.ClientHTTPContext;
                        existingClient.ClientWSContext = CurrentClient.ClientWSContext;
                        existingClient.ClientWSInterface = CurrentClient.ClientWSInterface;
                        existingClient.ClientTCPInterface = CurrentClient.ClientTCPInterface;
                        
                        if (!Clients.TryRemove(CurrentClient.IpPort(), out removedClient))
                        {
                            Log("*** UpdateClient " + CurrentClient.IpPort() + " unable to remove existing entry");
                            return false;
                        }
                        
                        if (!Clients.TryAdd(CurrentClient.IpPort(), existingClient))
                        {
                            Log("*** UpdateClient " + CurrentClient.IpPort() + " unable to add replacement entry");
                            return false;
                        }
                        
                        #endregion
                    }
                }
                else
                {
                    #region New-Entry

                    if (!Clients.TryAdd(CurrentClient.IpPort(), CurrentClient))
                    {
                        Log("*** UpdateClient " + CurrentClient.IpPort() + " unable to add new entry");
                        return false;
                    }
                    
                    #endregion
                }

                #endregion

                #region Add-Client-GUID-Map
                
                string existingMap = null;
                if (ClientGUIDMap.TryGetValue(CurrentClient.ClientGUID, out existingMap))
                {
                    if (String.IsNullOrEmpty(existingMap))
                    {
                        #region New-Entry

                        if (!ClientGUIDMap.TryAdd(CurrentClient.ClientGUID, CurrentClient.IpPort()))
                        {
                            Log("*** UpdateClient unable to add GUID map for client GUID " + CurrentClient.ClientGUID + " for tuple " + CurrentClient.IpPort());
                            return false;
                        }
                        
                        #endregion
                    }
                    else
                    {
                        #region Existing-Entry

                        string deletedMap = null;
                        if (!ClientGUIDMap.TryRemove(CurrentClient.ClientGUID, out deletedMap))
                        {
                            Log("*** UpdateClient unable to remove client GUID map for GUID " + CurrentClient.ClientGUID + " for replacement");
                            return false;
                        }
                        
                        if (!ClientGUIDMap.TryAdd(CurrentClient.ClientGUID, CurrentClient.IpPort()))
                        {
                            Log("*** UpdateClient unable to add GUID map for client GUID " + CurrentClient.ClientGUID + " for tuple " + CurrentClient.IpPort());
                            return false;
                        }
                        
                        #endregion
                    }
                }
                else
                {
                    #region New-Entry

                    if (!ClientGUIDMap.TryAdd(CurrentClient.ClientGUID, CurrentClient.IpPort()))
                    {
                        Log("*** UpdateClient unable to add GUID map for client GUID " + CurrentClient.ClientGUID + " for tuple " + CurrentClient.IpPort());
                        return false;
                    }
                    
                    #endregion
                }

                #endregion

                Log("UpdateClient " + CurrentClient.IpPort() + " exiting with " + Clients.Count + " clients and " + ClientGUIDMap.Count + " GUID maps");
                return true;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("UpdateClient " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool AddChannel(Client CurrentClient, Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentClient == null)
                {
                    Log("*** AddChannel null client supplied");
                    return false;
                }

                if (CurrentChannel == null)
                {
                    Log("*** AddChannel null channel supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentChannel.Guid))
                {
                    CurrentChannel.Guid = Guid.NewGuid().ToString();
                    return false;
                }

                DateTime timestamp = DateTime.Now.ToUniversalTime();
                if (CurrentChannel.CreatedUTC == null) CurrentChannel.CreatedUTC = timestamp;
                if (CurrentChannel.UpdatedUTC == null) CurrentChannel.UpdatedUTC = timestamp;

                Channel existingChannel = null;
                if (Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    if (existingChannel != null)
                    {
                        Log("AddChannel channel with GUID " + CurrentChannel.Guid + " already exists");
                        return true;
                    }
                }

                Log("AddChannel adding channel " + CurrentChannel.ChannelName + " GUID " + CurrentChannel.Guid);
                if (String.IsNullOrEmpty(CurrentChannel.ChannelName)) CurrentChannel.ChannelName = CurrentChannel.Guid;
                CurrentChannel.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentChannel.UpdatedUTC = CurrentClient.CreatedUTC;
                CurrentChannel.Members = new List<Client>();
                CurrentChannel.Members.Add(CurrentClient);
                CurrentChannel.Subscribers = new List<Client>();
                CurrentChannel.OwnerGuid = CurrentClient.ClientGUID;

                if (!Channels.TryAdd(CurrentChannel.Guid, CurrentChannel))
                {
                    Log("*** AddChannel unable to add channel with GUID " + CurrentChannel.Guid + " for client " + CurrentChannel.OwnerGuid);
                    return false;
                }

                Log("AddChannel successfully added channel with GUID " + CurrentChannel.Guid + " for client " + CurrentChannel.OwnerGuid);
                return true;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("AddChannel " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveChannel(Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentChannel == null)
                {
                    Log("*** RemoveChannel null channel supplied");
                    return false;
                }

                if (Channels == null || Channels.Count < 1)
                {
                    Log("RemoveChannel no channels");
                    return true;
                }

                if (String.Compare(CurrentChannel.OwnerGuid, ServerGUID) == 0)
                {
                    Log("RemoveChannel skipping removal of channel " + CurrentChannel.Guid + " (server channel)");
                    return true;
                }

                Channel removeChannel = null;
                if (Channels.TryRemove(CurrentChannel.Guid, out removeChannel))
                {
                    if (removeChannel != null)
                    {
                        #region Match

                        Log("RemoveChannel notifying channel members of channel removal");

                        if (removeChannel.Members != null)
                        {
                            if (removeChannel.Members.Count > 0)
                            {
                                //
                                // create another reference in case list is modified
                                //
                                Channel TempChannel = removeChannel;
                                List<Client> TempSubscribers = new List<Client>(removeChannel.Members);

                                Task.Run(() =>
                                {
                                    foreach (Client Client in TempSubscribers)
                                    {
                                        if (String.Compare(Client.ClientGUID, CurrentChannel.OwnerGuid) != 0)
                                        {
                                            Log("RemoveChannel notifying channel " + TempChannel.Guid + " member " + Client.ClientGUID + " of channel deletion by owner");
                                            SendSystemMessage(ChannelDeletedByOwnerMessage(Client, TempChannel));
                                        }
                                    }
                                }
                                );
                            }
                        }

                        Log("RemoveChannel removed channel " + removeChannel.Guid + " successfully");
                        return true;

                        #endregion
                    }
                    else
                    {
                        Log("*** RemoveChannel channel " + CurrentChannel.Guid + " not found");
                        return false;
                    }
                }
                else
                {
                    Log("*** RemoveChannel channel " + CurrentChannel.Guid + " not found");
                    return false;
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("RemoveChannel " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool AddChannelMember(Client CurrentClient, Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** AddChannelMember null client supplied");
                    return false;
                }

                if (CurrentChannel == null)
                {
                    Log("*** AddChannelMember null channel supplied");
                    return false;
                }

                #endregion

                #region Process

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** AddChannelMember no channels");
                    return false;
                }

                Channel existingChannel = null;

                if (Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    if (existingChannel != null)
                    {
                        #region Channel-Exists

                        #region Add-to-Members

                        bool clientExists = false;

                        if (existingChannel.Members != null && existingChannel.Members.Count > 0)
                        {
                            foreach (Client curr in existingChannel.Members)
                            {
                                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) == 0)
                                {
                                    clientExists = true;
                                    break;
                                }
                            }
                        }

                        if (!clientExists)
                        {
                            existingChannel.Members.Add(CurrentClient);
                            if (!Channels.TryUpdate(existingChannel.Guid, existingChannel, CurrentChannel))
                            {
                                Log("** AddChannelMember unable to replace channel entry for GUID " + existingChannel.Guid);
                                return false;
                            }

                            //
                            // notify existing members
                            //
                            if (Config.Notification.ChannelJoinNotification)
                            {
                                foreach (Client curr in existingChannel.Members)
                                {
                                    if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                                    {
                                        //
                                        // create another reference in case list is modified
                                        //
                                        Channel TempChannel = existingChannel;
                                        Task.Run(() =>
                                        {
                                            Log("AddChannelMember notifying channel " + TempChannel.Guid + " subscriber " + curr.ClientGUID + " of channel join by client " + CurrentClient.ClientGUID);
                                            SendSystemMessage(ChannelJoinEventMessage(TempChannel, CurrentClient));
                                        }
                                        );
                                    }
                                }
                            }
                        }

                        #endregion
                        
                        #endregion
                    }
                    else
                    {
                        Log("*** AddChannelMember channel with GUID " + CurrentChannel.Guid + " not found");
                        return false;
                    }
                }
                else
                {
                    Log("*** AddChannelMember channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }

                #endregion

                return true;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("AddChannelMember " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool AddChannelSubscriber(Client CurrentClient, Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** AddChannelSubscriber null client supplied");
                    return false;
                }

                if (CurrentChannel == null)
                {
                    Log("*** AddChannelSubscriber null channel supplied");
                    return false;
                }

                #endregion

                #region Process

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** AddChannelSubscriber no channels");
                    return false;
                }

                Channel existingChannel = null;

                if (Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    if (existingChannel != null)
                    {
                        #region Channel-Exists

                        #region Add-to-Members

                        bool clientExists = false;

                        if (existingChannel.Members != null && existingChannel.Members.Count > 0)
                        {
                            foreach (Client curr in existingChannel.Members)
                            {
                                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) == 0)
                                {
                                    clientExists = true;
                                    break;
                                }
                            }
                        }

                        if (!clientExists)
                        {
                            existingChannel.Members.Add(CurrentClient);
                            if (!Channels.TryUpdate(existingChannel.Guid, existingChannel, CurrentChannel))
                            {
                                Log("** AddChannelSubscriber unable to replace channel entry for GUID " + existingChannel.Guid);
                                return false;
                            }

                            //
                            // notify existing members
                            //
                            if (Config.Notification.ChannelJoinNotification)
                            {
                                foreach (Client curr in existingChannel.Members)
                                {
                                    if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                                    {
                                        //
                                        // create another reference in case list is modified
                                        //
                                        Channel TempChannel = existingChannel;
                                        Task.Run(() =>
                                        {
                                            Log("AddChannelSubscriber notifying channel " + TempChannel.Guid + " subscriber " + curr.ClientGUID + " of channel join by client " + CurrentClient.ClientGUID);
                                            SendSystemMessage(ChannelJoinEventMessage(TempChannel, CurrentClient));
                                        }
                                        );
                                    }
                                }
                            }
                        }

                        #endregion

                        #region Add-to-Subscribers

                        clientExists = false;

                        if (existingChannel.Subscribers != null && existingChannel.Subscribers.Count > 0)
                        {
                            foreach (Client curr in existingChannel.Subscribers)
                            {
                                if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) == 0)
                                {
                                    clientExists = true;
                                    break;
                                }
                            }
                        }

                        if (!clientExists)
                        {
                            existingChannel.Subscribers.Add(CurrentClient);
                            if (!Channels.TryUpdate(existingChannel.Guid, existingChannel, CurrentChannel))
                            {
                                Log("** AddChannelSubscriber unable to replace channel entry for GUID " + existingChannel.Guid);
                                return false;
                            }

                            //
                            // notify existing members
                            //
                            if (Config.Notification.ChannelJoinNotification)
                            {
                                foreach (Client curr in existingChannel.Members)
                                {
                                    if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) != 0)
                                    {
                                        //
                                        // create another reference in case list is modified
                                        //
                                        Channel TempChannel = existingChannel;
                                        Task.Run(() =>
                                        {
                                            Log("AddChannelSubscriber notifying channel " + TempChannel.Guid + " subscriber " + curr.ClientGUID + " of channel join by client " + CurrentClient.ClientGUID);
                                            SendSystemMessage(SubscriberJoinEventMessage(TempChannel, CurrentClient));
                                        }
                                        );
                                    }
                                }
                            }
                        }

                        #endregion

                        #endregion
                    }
                    else
                    {
                        Log("*** AddChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                        return false;
                    }
                }
                else
                {
                    Log("*** AddChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }

                #endregion

                return true;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("AddChannelSubscriber " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveChannelMember(Client CurrentClient, Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentClient == null)
                {
                    Log("*** RemoveChannelMember null client supplied");
                    return false;
                }

                if (CurrentChannel == null)
                {
                    Log("*** RemoveChannelMember null channel supplied");
                    return false;
                }

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** RemoveChannelMember no channels");
                    return false;
                }

                Channel existingChannel = null;
                if (!Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    Log("*** RemoveChannelMember channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }

                if (existingChannel != null)
                {
                    #region Channel-Found

                    #region Update-Members-and-Subscribers-List

                    List<Client> updatedMembers = new List<Client>();
                    List<Client> updatedSubscribers = new List<Client>();

                    if (existingChannel.Members == null || existingChannel.Members.Count < 1)
                    {
                        Log("RemoveChannelMember channel " + CurrentChannel.Guid + " has no members, removing channel");
                        return RemoveChannel(CurrentChannel);
                    }

                    foreach (Client curr in existingChannel.Members)
                    {
                        if (String.Compare(CurrentClient.ClientGUID, curr.ClientGUID) != 0)
                        {
                            updatedMembers.Add(curr);
                        }
                    }

                    foreach (Client curr in existingChannel.Subscribers)
                    {
                        if (String.Compare(CurrentClient.ClientGUID, curr.ClientGUID) != 0)
                        {
                            updatedSubscribers.Add(curr);
                        }
                    }

                    existingChannel.Members = updatedMembers;
                    existingChannel.Subscribers = updatedSubscribers;
                    Log("RemoveChannelMember successfully replaced channel member and subscriber list for channel GUID " + CurrentChannel.Guid);

                    #endregion

                    #region Update-Channel

                    Channel removedChannel = null;
                    if (!Channels.TryRemove(CurrentChannel.Guid, out removedChannel))
                    {
                        Log("*** RemoveChannelMember unable to remove channel with GUID " + CurrentChannel.Guid + " for replacement");
                        return false;
                    }

                    if (!Channels.TryAdd(CurrentChannel.Guid, existingChannel))
                    {
                        Log("*** RemoveChannelMember unable to re-add channel with GUID " + CurrentChannel.Guid);
                        return false;
                    }

                    Log("RemoveChannelMember successfully replaced channel object channel GUID " + CurrentChannel.Guid);

                    #endregion

                    #region Send-Notifications

                    if (Config.Notification.ChannelJoinNotification)
                    {
                        foreach (Client curr in existingChannel.Members)
                        {
                            //
                            // create another reference in case list is modified
                            //
                            Channel TempChannel = existingChannel;
                            Task.Run(() =>
                            {
                                Log("RemoveChannelMember notifying channel " + TempChannel.Guid + " member " + curr.ClientGUID + " of channel leave by member " + CurrentClient.ClientGUID);
                                SendSystemMessage(ChannelLeaveEventMessage(TempChannel, CurrentClient));
                            }
                            );
                         }
                    }

                    Log("RemoveChannelSubscriber successfully replaced channel member and subscriber lists for channel GUID " + CurrentChannel.Guid);
                    return true;

                    #endregion

                    #endregion
                }
                else
                {
                    #region Channel-Not-found

                    Log("*** RemoveChannelMember channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;

                    #endregion
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("RemoveChannelSubscriber " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveChannelSubscriber(Client CurrentClient, Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentClient == null)
                {
                    Log("*** RemoveChannelSubscriber null client supplied");
                    return false;
                }

                if (CurrentChannel == null)
                {
                    Log("*** RemoveChannelSubscriber null channel supplied");
                    return false;
                }

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** RemoveChannelSubscriber no channels");
                    return false;
                }

                Channel existingChannel = null;
                if (!Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    Log("*** RemoveChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }

                if (existingChannel != null)
                {
                    #region Channel-Found

                    #region Update-Members-and-Subscribers-List
                    
                    if (existingChannel.Members == null || existingChannel.Members.Count < 1)
                    {
                        Log("RemoveChannelSubscriber channel " + CurrentChannel.Guid + " has no members, removing channel");
                        return RemoveChannel(CurrentChannel);
                    }

                    List<Client> updatedMembers = new List<Client>();
                    foreach (Client curr in existingChannel.Members)
                    {
                        if (String.Compare(CurrentClient.ClientGUID, curr.ClientGUID) != 0)
                        {
                            updatedMembers.Add(curr);
                        }
                    }

                    List<Client> updatedSubscribers = new List<Client>();
                    foreach (Client curr in existingChannel.Subscribers)
                    {
                        if (String.Compare(CurrentClient.ClientGUID, curr.ClientGUID) != 0)
                        {
                            updatedSubscribers.Add(curr);
                        }
                    }

                    existingChannel.Members = updatedMembers;
                    existingChannel.Subscribers = updatedSubscribers;
                    Log("RemoveChannelSubscriber successfully replaced channel member and subscriber list for channel GUID " + CurrentChannel.Guid);

                    #endregion

                    #region Update-Channel

                    Channel removedChannel = null;
                    if (!Channels.TryRemove(CurrentChannel.Guid, out removedChannel))
                    {
                        Log("*** RemoveChannelSubscriber unable to remove channel with GUID " + CurrentChannel.Guid + " for replacement");
                        return false;
                    }

                    if (!Channels.TryAdd(CurrentChannel.Guid, existingChannel))
                    {
                        Log("*** RemoveChannelSubscriber unable to re-add channel with GUID " + CurrentChannel.Guid);
                        return false;
                    }

                    Log("RemoveChannelSubscriber successfully replaced channel object channel GUID " + CurrentChannel.Guid);

                    #endregion

                    #region Send-Notifications

                    if (Config.Notification.ChannelJoinNotification)
                    {
                        foreach (Client curr in existingChannel.Members)
                        {
                            Channel TempChannel = existingChannel;
                            Task.Run(() =>
                            {
                                Log("RemoveChannelSubscriber notifying channel " + TempChannel.Guid + " member " + curr.ClientGUID + " of channel leave by subscriber " + CurrentClient.ClientGUID);
                                SendSystemMessage(SubscriberLeaveEventMessage(TempChannel, CurrentClient));
                            });
                        }
                    }

                    return true;

                    #endregion

                    #endregion
                }
                else
                {
                    #region Channel-Not-Found

                    Log("*** RemoveChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;

                    #endregion
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("RemoveChannelSubscriber " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool IsChannelMember(Client CurrentClient, Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** IsChannelMember no channels found");
                    return false;
                }

                Channel existingChannel = null;
                if (!Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    Log("*** IsChannelMember channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }
                else
                {
                    if (existingChannel != null)
                    {
                        #region Channel-Found

                        foreach (Client curr in existingChannel.Members)
                        {
                            if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) == 0)
                            {
                                Log("IsChannelMember client GUID " + CurrentClient.ClientGUID + " is a member in channel GUID " + CurrentChannel.Guid);
                                return true;
                            }
                        }

                        Log("*** IsChannelMember client GUID " + CurrentClient.ClientGUID + " is not a member in channel GUID " + CurrentChannel.Guid);
                        return false;

                        #endregion
                    }
                    else
                    {
                        Log("*** IsChannelMember channel with GUID " + CurrentChannel.Guid + " not found");
                        return false;
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("IsChannelMember " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool IsChannelSubscriber(Client CurrentClient, Channel CurrentChannel)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** IsChannelSubscriber no channels found");
                    return false;
                }

                Channel existingChannel = null;
                if (!Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    Log("*** IsChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }
                else
                {
                    if (existingChannel != null)
                    {
                        #region Channel-Found

                        foreach (Client curr in existingChannel.Subscribers)
                        {
                            if (String.Compare(curr.ClientGUID, CurrentClient.ClientGUID) == 0)
                            {
                                Log("IsChannelSubscriber client GUID " + CurrentClient.ClientGUID + " is a subscriber to channel GUID " + CurrentChannel.Guid);
                                return true;
                            }
                        }

                        Log("*** IsChannelSubscriber client GUID " + CurrentClient.ClientGUID + " is not a subscriber to channel GUID " + CurrentChannel.Guid);
                        return false;

                        #endregion
                    }
                    else
                    {
                        Log("*** IsChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                        return false;
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("IsChannelSubscriber " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool IsClientConnected(string guid)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid))
                {
                    Log("*** IsClientConnected null GUID supplied");
                    return false;
                }

                string ipPort = null;
                if (ClientGUIDMap.TryGetValue(guid, out ipPort))
                {
                    Client existingClient = null;
                    if (Clients.TryGetValue(ipPort, out existingClient))
                    {
                        if (existingClient != null)
                        {
                            Log("IsClientConnected client " + guid + " exists");
                            return true;
                        }
                    }
                }
                
                Log("IsClientConnected client " + guid + " is not connected");
                return false;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("IsClientConnected " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<User> GetCurrentUsersFile()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
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
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetCurrentUsersFile " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<Permission> GetCurrentPermissionsFile()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
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
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.LockMethodResponseTime) Log("GetCurrentPermissionsFile " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion

        #region Private-Message-Processing-Methods

        private Message RedactMessage(Message msg)
        {
            if (msg == null) return null;
            msg.Email = null;
            msg.Password = null;
            return msg;
        }

        private Channel BuildChannelFromMessageData(Client CurrentClient, Message CurrentMessage)
        {
            if (CurrentClient == null)
            {
                Log("*** BuildChannelFromMessageData null client supplied");
                return null;
            }

            if (CurrentMessage == null)
            {
                Log("*** BuildChannelFromMessageData null channel supplied");
                return null;
            }

            if (CurrentMessage.Data == null)
            {
                Log("*** BuildChannelFromMessageData null data supplied in message");
                return null;
            }

            Channel ret = null;
            try
            {
                ret = Helper.DeserializeJson<Channel>(CurrentMessage.Data, false);
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

            if (String.IsNullOrEmpty(ret.Guid)) ret.Guid = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(ret.ChannelName)) ret.ChannelName = ret.Guid;
            ret.CreatedUTC = DateTime.Now.ToUniversalTime();
            ret.UpdatedUTC = ret.CreatedUTC;
            ret.OwnerGuid = CurrentClient.ClientGUID;
            ret.Members = new List<Client>();
            ret.Members.Add(CurrentClient);
            ret.Subscribers = new List<Client>();
            return ret;
        }

        private bool MessageProcessor(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** MessageProcessor null client supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** MessageProcessor null message supplied");
                    return false;
                }

                #endregion

                #region Variables-and-Initialization

                Client CurrentRecipient = null;
                Channel CurrentChannel = null;
                Message ResponseMessage = new Message();
                bool ResponseSuccess = false;
                CurrentMessage.Success = null;

                #endregion

                #region Verify-Client-GUID-Present

                if (String.IsNullOrEmpty(CurrentClient.ClientGUID))
                {
                    if (!String.IsNullOrEmpty(CurrentMessage.Command))
                    {
                        if (String.Compare(CurrentMessage.Command.ToLower(), "login") != 0)
                        {
                            #region Null-GUID-and-Not-Login

                            Log("*** MessageProcessor received message from client with no GUID");
                            ResponseSuccess = QueueClientMessage(CurrentClient, LoginRequiredMessage());
                            if (!ResponseSuccess)
                            {
                                Log("*** MessageProcessor unable to queue login required message to client " + CurrentClient.IpPort());
                            }
                            return ResponseSuccess;

                            #endregion
                        }
                    }
                }
                else
                {
                    #region Ensure-GUID-Exists

                    if (String.Compare(CurrentClient.ClientGUID, ServerGUID) != 0)
                    {
                        //
                        // All zeros is the BigQ server
                        //
                        Client VerifyClient = GetClientByGuid(CurrentClient.ClientGUID);
                        if (VerifyClient == null)
                        {
                            Log("*** MessageProcessor received message from unknown client GUID " + CurrentClient.ClientGUID + " from " + CurrentClient.IpPort());
                            ResponseSuccess = QueueClientMessage(CurrentClient, LoginRequiredMessage());
                            if (!ResponseSuccess)
                            {
                                Log("*** MessageProcessor unable to queue login required message to client " + CurrentClient.IpPort());
                            }
                            return ResponseSuccess;
                        }
                    }

                    #endregion
                }

                #endregion

                #region Verify-Transport-Objects-Present

                if (String.Compare(CurrentClient.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (CurrentClient.IsTCP)
                    {
                        #region TCP

                        if (CurrentClient.ClientTCPInterface == null)
                        {
                            Log("*** MessageProcessor null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (CurrentClient.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (CurrentClient.ClientTCPSSLInterface == null)
                        {
                            Log("*** MessageProcessor null TCP SSL client within supplied client");
                            return false;
                        }

                        if (CurrentClient.ClientSSLStream == null)
                        {
                            Log("*** MessageProcessor null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (CurrentClient.IsWebsocket)
                    {
                        #region Websocket

                        if (CurrentClient.ClientHTTPContext == null)
                        {
                            Log("*** MessageProcessor null HTTP context within supplied client");
                            return false;
                        }

                        if (CurrentClient.ClientWSContext == null)
                        {
                            Log("*** MessageProcessor null websocket context witin supplied client");
                            return false;
                        }

                        if (CurrentClient.ClientWSInterface == null)
                        {
                            Log("*** MessageProcessor null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (CurrentClient.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (CurrentClient.ClientHTTPSSLContext == null)
                        {
                            Log("*** MessageProcessor null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (CurrentClient.ClientWSSSLContext == null)
                        {
                            Log("*** MessageProcessor null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (CurrentClient.ClientWSSSLInterface == null)
                        {
                            Log("*** MessageProcessor null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** MessageProcessor unknown transport for supplied client " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                #endregion

                #region Authorize-Message

                if (!AuthorizeMessage(CurrentMessage))
                {
                    if (String.IsNullOrEmpty(CurrentMessage.Command))
                    {
                        Log("*** MessageProcessor unable to authenticate or authorize message from " + CurrentMessage.Email + " " + CurrentMessage.SenderGUID);
                    }
                    else
                    {
                        Log("*** MessageProcessor unable to authenticate or authorize message of type " + CurrentMessage.Command + " from " + CurrentMessage.Email + " " + CurrentMessage.SenderGUID);
                    }

                    ResponseMessage = AuthorizationFailedMessage(CurrentMessage);
                    ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue authorization failed message to client " + CurrentClient.IpPort());
                    }
                    return ResponseSuccess;
                }

                #endregion

                #region Process-Administrative-Messages

                if (!String.IsNullOrEmpty(CurrentMessage.Command))
                {
                    Log("MessageProcessor processing administrative message of type " + CurrentMessage.Command + " from client " + CurrentClient.IpPort());

                    switch (CurrentMessage.Command.ToLower())
                    {
                        case "echo":
                            ResponseMessage = ProcessEchoMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "login":
                            ResponseMessage = ProcessLoginMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "heartbeatrequest":
                            // no need to send response
                            return true;

                        case "joinchannel":
                            ResponseMessage = ProcessJoinChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "leavechannel":
                            ResponseMessage = ProcessLeaveChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "subscribechannel":
                            ResponseMessage = ProcessSubscribeChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "unsubscribechannel":
                            ResponseMessage = ProcessUnsubscribeChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "createchannel":
                            ResponseMessage = ProcessCreateChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "deletechannel":
                            ResponseMessage = ProcessDeleteChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "listchannels":
                            ResponseMessage = ProcessListChannelsMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "listchannelmembers":
                            ResponseMessage = ProcessListChannelMembersMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "listchannelsubscribers":
                            ResponseMessage = ProcessListChannelSubscribersMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "listclients":
                            ResponseMessage = ProcessListClientsMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "isclientconnected":
                            ResponseMessage = ProcessIsClientConnectedMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        default:
                            ResponseMessage = UnknownCommandMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                            return ResponseSuccess;
                    }
                }

                #endregion

                #region Get-Recipient-or-Channel

                if (!String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
                {
                    CurrentRecipient = GetClientByGuid(CurrentMessage.RecipientGUID);
                }
                else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGUID))
                {
                    CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                }
                else
                {
                    #region Recipient-Not-Supplied

                    Log("MessageProcessor no recipient specified either by RecipientGUID or ChannelGUID");
                    ResponseMessage = RecipientNotFoundMessage(CurrentClient, CurrentMessage);
                    ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue recipient not found message to " + CurrentClient.IpPort());
                    }
                    return false;

                    #endregion
                }

                #endregion

                #region Process-Recipient-Messages

                if (CurrentRecipient != null)
                {
                    #region Send-to-Recipient

                    ResponseSuccess = QueueClientMessage(CurrentRecipient, CurrentMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue to recipient " + CurrentRecipient.ClientGUID + ", sent failure notification to sender");
                    }

                    return ResponseSuccess;

                    #endregion
                }
                else if (CurrentChannel != null)
                {
                    #region Send-to-Channel

                    if (Helper.IsTrue(CurrentChannel.Broadcast))
                    {
                        #region Broadcast-Message

                        ResponseSuccess = SendChannelMembersMessage(CurrentClient, CurrentChannel, CurrentMessage);
                        if (!ResponseSuccess)
                        {
                            Log("*** MessageProcessor unable to send to members in channel " + CurrentChannel.Guid + ", sent failure notification to sender");
                        }

                        return ResponseSuccess;

                        #endregion
                    }
                    else if (Helper.IsTrue(CurrentChannel.Multicast))
                    {
                        #region Multicast-Message-to-Subscribers

                        ResponseSuccess = SendChannelSubscribersMessage(CurrentClient, CurrentChannel, CurrentMessage);
                        if (!ResponseSuccess)
                        {
                            Log("*** MessageProcessor unable to send to subscribers in channel " + CurrentChannel.Guid + ", sent failure notification to sender");
                        }

                        return ResponseSuccess;

                        #endregion
                    }
                    else if (Helper.IsTrue(CurrentChannel.Unicast))
                    {
                        #region Unicast-Message-to-One-Subscriber

                        ResponseSuccess = SendChannelSubscriberMessage(CurrentClient, CurrentChannel, CurrentMessage);
                        if (!ResponseSuccess)
                        {
                            Log("*** MessageProcessor unable to send to subscriber in channel " + CurrentChannel.Guid + ", sent failure notification to sender");
                        }

                        return ResponseSuccess;

                        #endregion
                    }
                    else
                    {
                        #region Unknown-Channel-Type

                        Log("*** MessageProcessor channel " + CurrentChannel.Guid + " not marked as broadcast, multicast, or unicast, deleting");
                        if (!RemoveChannel(CurrentChannel))
                        {
                            Log("*** MessageProcessor unable to remove channel " + CurrentChannel.Guid);
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
                    ResponseMessage = RecipientNotFoundMessage(CurrentClient, CurrentMessage);
                    ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** MessageProcessor unable to queue recipient not found message to client " + CurrentClient.IpPort());
                    }
                    return false;

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("MessageProcessor " + CurrentMessage.Command + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendPrivateMessage(Client Sender, Client Recipient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Sender == null)
                {
                    Log("*** SendPrivateMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(Sender.ClientGUID, ServerGUID) != 0)
                {
                    // all zeros is the server
                    if (Sender.IsTCP)
                    {
                        if (Sender.ClientTCPInterface == null)
                        {
                            Log("*** SendPrivateMessage null TCP client within supplied Sender");
                            return false;
                        }
                    }

                    if (Sender.IsWebsocket)
                    {
                        if (Sender.ClientHTTPContext == null)
                        {
                            Log("*** SendPrivateMessage null HTTP context within supplied Sender");
                            return false;
                        }

                        if (Sender.ClientWSContext == null)
                        {
                            Log("*** SendPrivateMessage null websocket context within supplied Sender");
                            return false;
                        }
                        if (Sender.ClientWSInterface == null)
                        {
                            Log("*** SendPrivateMessage null websocket object within supplied Sender");
                            return false;
                        }
                    }
                }

                if (Recipient == null)
                {
                    Log("*** SendPrivateMessage null Recipient supplied");
                    return false;
                }

                if (Recipient.IsTCP)
                {
                    if (Recipient.ClientTCPInterface == null)
                    {
                        Log("*** SendPrivateMessage null TCP client within supplied Recipient");
                        return false;
                    }
                }

                if (Recipient.IsWebsocket)
                {
                    if (Recipient.ClientHTTPContext == null)
                    {
                        Log("*** SendPrivateMessage null HTTP context within supplied Recipient");
                        return false;
                    }

                    if (Recipient.ClientWSContext == null)
                    {
                        Log("*** SendPrivateMessage null websocket context within supplied Recipient");
                        return false;
                    }

                    if (Recipient.ClientWSInterface == null)
                    {
                        Log("*** SendPrivateMessage null websocket object within supplied Recipient");
                        return false;
                    }
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendPrivateMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool ResponseSuccess = false;
                Message ResponseMessage = new Message();

                #endregion

                #region Send-to-Recipient

                ResponseSuccess = QueueClientMessage(Recipient, RedactMessage(CurrentMessage));

                #endregion

                #region Send-Success-or-Failure-to-Sender

                if (CurrentMessage.SyncRequest != null && Convert.ToBoolean(CurrentMessage.SyncRequest))
                {
                    #region Sync-Request

                    //
                    // do not send notifications for success/fail on a sync message
                    //

                    return true;

                    #endregion
                }
                else if (CurrentMessage.SyncRequest != null && Convert.ToBoolean(CurrentMessage.SyncResponse))
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

                    if (ResponseSuccess)
                    {
                        if (Config.Notification.MsgAcknowledgement)
                        {
                            ResponseMessage = MessageQueueSuccess(Sender, CurrentMessage);
                            ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                        }
                        return true;
                    }
                    else
                    {
                        ResponseMessage = MessageQueueFailure(Sender, CurrentMessage);
                        ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                        return false;
                    }

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendPrivateMessage " + CurrentMessage.SenderGUID + " -> " + CurrentMessage.RecipientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendChannelMembersMessage(Client Sender, Channel CurrentChannel, Message CurrentMessage)
        {
            //
            // broadcast channel
            //
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Sender == null)
                {
                    Log("*** SendChannelMembersMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(Sender.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (Sender.IsTCP)
                    {
                        #region TCP

                        if (Sender.ClientTCPInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (Sender.ClientTCPSSLInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null TCP SSL client within supplied client");
                            return false;
                        }

                        if (Sender.ClientSSLStream == null)
                        {
                            Log("*** SendChannelMembersMessage null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsWebsocket)
                    {
                        #region Websocket

                        if (Sender.ClientHTTPContext == null)
                        {
                            Log("*** SendChannelMembersMessage null HTTP context within supplied client");
                            return false;
                        }

                        if (Sender.ClientWSContext == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket context witin supplied client");
                            return false;
                        }

                        if (Sender.ClientWSInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (Sender.ClientHTTPSSLContext == null)
                        {
                            Log("*** SendChannelMembersMessage null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (Sender.ClientWSSSLContext == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (Sender.ClientWSSSLInterface == null)
                        {
                            Log("*** SendChannelMembersMessage null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** SendChannelMembersMessage unknown transport for supplied client " + Sender.IpPort() + " " + Sender.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                if (CurrentChannel == null)
                {
                    Log("*** SendChannelMembersMessage null channel supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendChannelMembersMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool ResponseSuccess = false;
                Message ResponseMessage = new Message();

                #endregion

                #region Verify-Channel-Membership

                if (!IsChannelMember(Sender, CurrentChannel))
                {
                    ResponseMessage = NotChannelMemberMessage(Sender, CurrentMessage, CurrentChannel);
                    ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** SendChannelMembersMessage unable to queue not channel member message to " + Sender.IpPort());
                    }
                    return false;
                }

                #endregion

                #region Send-to-Channel-and-Return-Success

                Task.Run(() =>
                {
                    ResponseSuccess = ChannelDataSender(Sender, CurrentChannel, RedactMessage(CurrentMessage));
                });

                if (Config.Notification.MsgAcknowledgement)
                {
                    ResponseMessage = MessageQueueSuccess(Sender, CurrentMessage);
                    ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** SendChannelMembersMessage unable to queue message queue success notification to " + Sender.IpPort());
                    }
                }
                return true;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendChannelMembersMessage " + CurrentMessage.SenderGUID + " -> " + CurrentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendChannelSubscribersMessage(Client Sender, Channel CurrentChannel, Message CurrentMessage)
        {
            //
            // multicast channel
            //
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Sender == null)
                {
                    Log("*** SendChannelSubscribersMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(Sender.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (Sender.IsTCP)
                    {
                        #region TCP

                        if (Sender.ClientTCPInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (Sender.ClientTCPSSLInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null TCP SSL client within supplied client");
                            return false;
                        }

                        if (Sender.ClientSSLStream == null)
                        {
                            Log("*** SendChannelSubscribersMessage null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsWebsocket)
                    {
                        #region Websocket

                        if (Sender.ClientHTTPContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null HTTP context within supplied client");
                            return false;
                        }

                        if (Sender.ClientWSContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket context witin supplied client");
                            return false;
                        }

                        if (Sender.ClientWSInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (Sender.ClientHTTPSSLContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (Sender.ClientWSSSLContext == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (Sender.ClientWSSSLInterface == null)
                        {
                            Log("*** SendChannelSubscribersMessage null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** SendChannelSubscribersMessage unknown transport for supplied client " + Sender.IpPort() + " " + Sender.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                if (CurrentChannel == null)
                {
                    Log("*** SendChannelSubscribersMessage null channel supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendChannelSubscribersMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool ResponseSuccess = false;
                Message ResponseMessage = new Message();

                #endregion

                #region Verify-Channel-Membership

                if (!IsChannelMember(Sender, CurrentChannel))
                {
                    ResponseMessage = NotChannelMemberMessage(Sender, CurrentMessage, CurrentChannel);
                    ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** SendChannelSubscribersMessage unable to queue not channel member message to " + Sender.IpPort());
                    }
                    return false;
                }

                #endregion

                #region Send-to-Channel-Subscribers-and-Return-Success

                Task.Run(() =>
                {
                    ResponseSuccess = ChannelDataSender(Sender, CurrentChannel, RedactMessage(CurrentMessage));
                });

                if (Config.Notification.MsgAcknowledgement)
                {
                    ResponseMessage = MessageQueueSuccess(Sender, CurrentMessage);
                    ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** SendChannelSubscribersMessage unable to queue message queue success mesage to " + Sender.IpPort());
                    }
                }
                return true;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendChannelSubscribersMessage " + CurrentMessage.SenderGUID + " -> " + CurrentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }
        
        private bool SendChannelSubscriberMessage(Client Sender, Channel CurrentChannel, Message CurrentMessage)
        {
            //
            // unicast within a multicast channel
            //
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Sender == null)
                {
                    Log("*** SendChannelSubscriberMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(Sender.ClientGUID, ServerGUID) != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (Sender.IsTCP)
                    {
                        #region TCP

                        if (Sender.ClientTCPInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null TCP client within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsTCPSSL)
                    {
                        #region TCP-SSL

                        if (Sender.ClientTCPSSLInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null TCP SSL client within supplied client");
                            return false;
                        }

                        if (Sender.ClientSSLStream == null)
                        {
                            Log("*** SendChannelSubscriberMessage null SSL stream within supplied client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsWebsocket)
                    {
                        #region Websocket

                        if (Sender.ClientHTTPContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null HTTP context within supplied client");
                            return false;
                        }

                        if (Sender.ClientWSContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket context witin supplied client");
                            return false;
                        }

                        if (Sender.ClientWSInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else if (Sender.IsWebsocketSSL)
                    {
                        #region Websocket-SSL

                        if (Sender.ClientHTTPSSLContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null HTTP SSL context within supplied client");
                            return false;
                        }

                        if (Sender.ClientWSSSLContext == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket SSL context witin supplied client");
                            return false;
                        }

                        if (Sender.ClientWSSSLInterface == null)
                        {
                            Log("*** SendChannelSubscriberMessage null websocket SSL object within supplied websocket client");
                            return false;
                        }

                        #endregion
                    }
                    else
                    {
                        #region Unknown

                        Log("*** SendChannelSubscriberMessage unknown transport for supplied client " + Sender.IpPort() + " " + Sender.ClientGUID);
                        return false;

                        #endregion
                    }
                }

                if (CurrentChannel == null)
                {
                    Log("*** SendChannelSubscriberMessage null channel supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendChannelSubscriberMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool ResponseSuccess = false;
                Message ResponseMessage = new Message();

                #endregion

                #region Verify-Channel-Membership

                if (!IsChannelMember(Sender, CurrentChannel))
                {
                    ResponseMessage = NotChannelMemberMessage(Sender, CurrentMessage, CurrentChannel);
                    ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** SendChannelSubscriberMessage unable to queue not channel member message to " + Sender.IpPort());
                    }
                    return false;
                }

                #endregion

                #region Send-to-Channel-Subscriber-and-Return-Success

                Task.Run(() =>
                {
                    ResponseSuccess = ChannelDataSender(Sender, CurrentChannel, RedactMessage(CurrentMessage));
                });

                if (Config.Notification.MsgAcknowledgement)
                {
                    ResponseMessage = MessageQueueSuccess(Sender, CurrentMessage);
                    ResponseSuccess = QueueClientMessage(Sender, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** SendChannelSubscriberMessage unable to queue message queue success mesage to " + Sender.IpPort());
                    }
                }
                return true;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendChannelSubscriberMessage " + CurrentMessage.SenderGUID + " -> " + CurrentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemMessage(Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentMessage == null)
                {
                    Log("*** SendSystemMessage null message supplied");
                    return false;
                }

                #endregion

                #region Create-System-Client-Object

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

                #endregion

                #region Variables

                Client CurrentRecipient = new Client();
                Channel CurrentChannel = new Channel();
                Message ResponseMessage = new Message();
                bool ResponseSuccess = false;

                #endregion

                #region Get-Recipient-or-Channel

                if (!String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
                {
                    CurrentRecipient = GetClientByGuid(CurrentMessage.RecipientGUID);
                }
                else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGUID))
                {
                    CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
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

                if (CurrentRecipient != null)
                {
                    #region Send-to-Recipient

                    ResponseSuccess = QueueClientMessage(CurrentRecipient, RedactMessage(CurrentMessage));
                    if (ResponseSuccess)
                    {
                        Log("SendSystemMessage successfully queued message to recipient " + CurrentRecipient.ClientGUID);
                        return true;
                    }
                    else
                    {
                        Log("*** SendSystemMessage unable to queue message to recipient " + CurrentRecipient.ClientGUID);
                        return false;
                    }

                    #endregion
                }
                else if (CurrentChannel != null)
                {
                    #region Send-to-Channel-and-Return-Success

                    ResponseSuccess = ChannelDataSender(CurrentClient, CurrentChannel, RedactMessage(CurrentMessage));
                    if (ResponseSuccess)
                    {
                        Log("SendSystemMessage successfully sent message to channel " + CurrentChannel.Guid);
                        return true;
                    }
                    else
                    {
                        Log("*** SendSystemMessage unable to send message to channel " + CurrentChannel.Guid);
                        return false;
                    }

                    #endregion
                }
                else
                {
                    #region Recipient-Not-Found

                    Log("Unable to find either recipient or channel");
                    ResponseMessage = RecipientNotFoundMessage(CurrentClient, CurrentMessage);
                    ResponseSuccess = QueueClientMessage(CurrentClient, ResponseMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** SendSystemMessage unable to queue recipient not found message to " + CurrentClient.IpPort());
                    }
                    return false;

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendSystemMessage " + CurrentMessage.SenderGUID + " -> " + CurrentMessage.RecipientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemPrivateMessage(Client Recipient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Recipient == null)
                {
                    Log("*** SendSystemPrivateMessage null recipient supplied");
                    return false;
                }

                if (Recipient.IsTCP)
                {
                    if (Recipient.ClientTCPInterface == null)
                    {
                        Log("*** SendSystemPrivateMessage null TCP client found within supplied recipient");
                        return false;
                    }
                }

                if (Recipient.IsWebsocket)
                {
                    if (Recipient.ClientHTTPContext == null)
                    {
                        Log("*** SendSystemPrivateMessage null HTTP context found within supplied recipient");
                        return false;
                    }

                    if (Recipient.ClientWSContext == null)
                    {
                        Log("*** SendSystemPrivateMessage null websocket context found within supplied recipient");
                        return false;
                    }

                    if (Recipient.ClientWSInterface == null)
                    {
                        Log("*** SendSystemPrivateMessage null websocket object found within supplied recipient");
                        return false;
                    }
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendSystemPrivateMessage null message supplied");
                    return false;
                }

                #endregion

                #region Create-System-Client-Object

                Client CurrentClient = new Client();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGUID = ServerGUID;

                if (!String.IsNullOrEmpty(Config.TcpServer.IP)) CurrentClient.SourceIP = Config.TcpServer.IP;
                else CurrentClient.SourceIP = "127.0.0.1";

                CurrentClient.SourcePort = Config.TcpServer.Port;
                CurrentClient.ServerIP = CurrentClient.SourceIP;
                CurrentClient.ServerPort = CurrentClient.SourcePort;
                CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUTC = CurrentClient.CreatedUTC;

                #endregion

                #region Variables

                Channel CurrentChannel = new Channel();
                bool ResponseSuccess = false;

                #endregion

                #region Process-Recipient-Messages

                ResponseSuccess = QueueClientMessage(Recipient, RedactMessage(CurrentMessage));
                if (!ResponseSuccess)
                {
                    Log("*** SendSystemPrivateMessage unable to queue message to " + Recipient.IpPort());
                }
                return ResponseSuccess;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendSystemPrivateMessage " + CurrentMessage.SenderGUID + " -> " + CurrentMessage.RecipientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemChannelMessage(Channel CurrentChannel, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (CurrentChannel == null)
                {
                    Log("*** SendSystemChannelMessage null channel supplied");
                    return false;
                }

                if (CurrentChannel.Subscribers == null || CurrentChannel.Subscribers.Count < 1)
                {
                    Log("SendSystemChannelMessage no subscribers in channel " + CurrentChannel.Guid);
                    return true;
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendSystemPrivateMessage null message supplied");
                    return false;
                }

                #endregion

                #region Create-System-Client-Object

                Client CurrentClient = new Client();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGUID = ServerGUID;

                if (!String.IsNullOrEmpty(Config.TcpServer.IP)) CurrentClient.SourceIP = Config.TcpServer.IP;
                else CurrentClient.SourceIP = "127.0.0.1";

                CurrentClient.SourcePort = Config.TcpServer.Port;
                CurrentClient.ServerIP = CurrentClient.SourceIP;
                CurrentClient.ServerPort = CurrentClient.SourcePort;
                CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUTC = CurrentClient.CreatedUTC;

                #endregion

                #region Override-Channel-Variables

                //
                // This is necessary so the message goes to members instead of subscribers
                // in case the channel is configured as a multicast channel
                //
                CurrentChannel.Broadcast = 1;
                CurrentChannel.Multicast = 0;                

                #endregion

                #region Variables

                bool ResponseSuccess = false;

                #endregion

                #region Send-to-Channel

                ResponseSuccess = ChannelDataSender(CurrentClient, CurrentChannel, CurrentMessage);
                return ResponseSuccess;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("SendSystemChannelMessage " + CurrentMessage.SenderGUID + " -> " + CurrentMessage.ChannelGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion

        #region Private-Message-Handlers

        private Message ProcessEchoMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                CurrentMessage.SenderGUID = ServerGUID;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.Success = true;
                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessEchoMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessLoginMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            bool runClientLoginTask = false;
            bool runServerJoinNotification = false;

            try
            {
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                CurrentMessage.SenderGUID = ServerGUID;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.ClientGUID = CurrentMessage.RecipientGUID;
                CurrentClient.Email = CurrentMessage.Email;
                if (String.IsNullOrEmpty(CurrentClient.Email)) CurrentClient.Email = CurrentClient.ClientGUID;

                if (!UpdateClient(CurrentClient))
                {
                    Log("*** ProcessLoginMessage unable to update client " + CurrentClient.ClientGUID + " " + CurrentClient.IpPort());
                    CurrentMessage.Success = false;
                    CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.ServerError, "Unable to update client object", null);
                }
                else
                {
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = SuccessData.ToBytes("Login successful", null);
                    runClientLoginTask = true;
                    runServerJoinNotification = true;
                    
                    #region Start-Heartbeat-Manager

                    if (Config.Heartbeat.IntervalMs > 0)
                    {
                        Log("ProcessLoginMessage starting heartbeat manager for " + CurrentClient.IpPort());
                        if (CurrentClient.IsTCP) Task.Run(() => TCPHeartbeatManager(CurrentClient), TCPCancellationToken);
                        else if (CurrentClient.IsTCPSSL) Task.Run(() => TCPSSLHeartbeatManager(CurrentClient), TCPSSLCancellationToken);
                        else if (CurrentClient.IsWebsocket) Task.Run(() => WSHeartbeatManager(CurrentClient), WSCancellationToken);
                        else if (CurrentClient.IsWebsocketSSL) Task.Run(() => WSSSLHeartbeatManager(CurrentClient), WSSSLCancellationToken);
                        else
                        {
                            Log("*** ProcessLoginMessage unable to start heartbeat manager for " + CurrentClient.IpPort() + ", cannot determine transport from object");
                        }

                    }

                    #endregion
                }

                CurrentMessage = RedactMessage(CurrentMessage);
                return CurrentMessage;
            }
            finally
            {
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessLoginMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.ElapsedMilliseconds + "ms (before tasks)");

                if (runClientLoginTask)
                {
                    if (ClientLogin != null)
                    {
                        Task.Run(() => ClientLogin(CurrentClient));
                    }
                }

                if (runServerJoinNotification)
                {
                    if (Config.Notification.ServerJoinNotification)
                    {
                        Task.Run(() => ServerJoinEvent(CurrentClient));
                    }
                }

                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessLoginMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.ElapsedMilliseconds + "ms (after tasks)");
            }
        }

        private Message ProcessIsClientConnectedMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                CurrentMessage.SenderGUID = ServerGUID;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();

                if (CurrentMessage.Data == null)
                {
                    CurrentMessage.Success = false;
                    CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.BadRequest, "Data does not include client GUID", null);
                }
                else
                {
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = SuccessData.ToBytes(null, IsClientConnected(CurrentMessage.Data.ToString()));
                }

                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessIsClientConnectedMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessJoinChannelMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = null;

                if (CurrentChannel == null)
                {
                    Log("*** ProcessJoinChannelMessage unable to find channel " + CurrentChannel.Guid);
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }
                else
                {
                    Log("ProcessJoinChannelMessage adding client " + CurrentClient.IpPort() + " as member to channel " + CurrentChannel.Guid);
                    if (!AddChannelMember(CurrentClient, CurrentChannel))
                    {
                        Log("*** ProcessJoinChannelMessage error while adding " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " as member of channel " + CurrentChannel.Guid);
                        ResponseMessage = ChannelJoinFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        return ResponseMessage;
                    }
                    else
                    {
                        if (Config.Notification.ChannelJoinNotification) ChannelJoinEvent(CurrentClient, CurrentChannel);
                        ResponseMessage = ChannelJoinSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        return ResponseMessage;
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessJoinChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessSubscribeChannelMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = null;

                if (CurrentChannel == null)
                {
                    Log("*** ProcessSubscribeChannelMessage unable to find channel " + CurrentChannel.Guid);
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }

                if (CurrentChannel.Broadcast == 1)
                {
                    Log("ProcessSubscribeChannelMessage channel marked as broadcast, calling ProcessJoinChannelMessage");
                    return ProcessJoinChannelMessage(CurrentClient, CurrentMessage);
                }
                
                #region Add-Member

                Log("ProcessSubscribeChannelMessage adding client " + CurrentClient.IpPort() + " as subscriber to channel " + CurrentChannel.Guid);
                if (!AddChannelMember(CurrentClient, CurrentChannel))
                {
                    Log("*** ProcessSubscribeChannelMessage error while adding " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " as member of channel " + CurrentChannel.Guid);
                    ResponseMessage = ChannelJoinFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    return ResponseMessage;
                }

                #endregion

                #region Add-Subscriber

                if (!AddChannelSubscriber(CurrentClient, CurrentChannel))
                {
                    Log("*** ProcessSubscribeChannelMessage error while adding " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " as subscriber to channel " + CurrentChannel.Guid);
                    ResponseMessage = ChannelSubscribeFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    return ResponseMessage;
                }

                #endregion

                #region Send-Notification

                if (Config.Notification.ChannelJoinNotification) ChannelJoinEvent(CurrentClient, CurrentChannel);
                ResponseMessage = ChannelSubscribeSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                return ResponseMessage;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessJoinChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessLeaveChannelMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = new Message();

                if (CurrentChannel == null)
                {
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }
                else
                {
                    if (String.Compare(CurrentClient.ClientGUID, CurrentChannel.OwnerGuid) == 0)
                    {
                        #region Owner-Abandoning-Channel

                        if (!RemoveChannel(CurrentChannel))
                        {
                            Log("*** ProcessLeaveChannelMessage unable to remove owner " + CurrentClient.IpPort() + " from channel " + CurrentMessage.ChannelGUID);
                            return ChannelLeaveFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        }
                        else
                        {
                            return ChannelDeleteSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        }

                        #endregion
                    }
                    else
                    {
                        #region Member-Leaving-Channel

                        if (!RemoveChannelMember(CurrentClient, CurrentChannel))
                        {
                            Log("*** ProcessLeaveChannelMessage unable to remove member " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " from channel " + CurrentMessage.ChannelGUID);
                            return ChannelLeaveFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        }
                        else
                        {
                            if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(CurrentClient, CurrentChannel);
                            return ChannelLeaveSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessLeaveChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessUnsubscribeChannelMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = new Message();

                if (CurrentChannel == null)
                {
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }
                
                if (CurrentChannel.Broadcast == 1)
                {
                    Log("ProcessUnsubscribeChannelMessage channel marked as broadcast, calling ProcessLeaveChannelMessage");
                    return ProcessLeaveChannelMessage(CurrentClient, CurrentMessage);
                }
                
                if (String.Compare(CurrentClient.ClientGUID, CurrentChannel.OwnerGuid) == 0)
                {
                    #region Owner-Abandoning-Channel

                    if (!RemoveChannel(CurrentChannel))
                    {
                        Log("*** ProcessUnsubscribeChannelMessage unable to remove owner " + CurrentClient.IpPort() + " from channel " + CurrentMessage.ChannelGUID);
                        return ChannelUnsubscribeFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    }
                    else
                    {
                        return ChannelDeleteSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    }

                    #endregion
                }
                else
                {
                    #region Subscriber-Leaving-Channel

                    if (!RemoveChannelSubscriber(CurrentClient, CurrentChannel))
                    {
                        Log("*** ProcessUnsubscribeChannelMessage unable to remove subscrber " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " from channel " + CurrentMessage.ChannelGUID);
                        return ChannelUnsubscribeFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    }
                    else
                    {
                        if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(CurrentClient, CurrentChannel);
                        return ChannelUnsubscribeSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    }

                    #endregion
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessUnsubscribeChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessCreateChannelMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = new Message();

                if (CurrentChannel == null)
                {
                    Channel RequestChannel = BuildChannelFromMessageData(CurrentClient, CurrentMessage);
                    if (RequestChannel == null)
                    {
                        Log("*** ProcessCreateChannelMessage unable to build Channel from Message data");
                        ResponseMessage = DataErrorMessage(CurrentClient, CurrentMessage, "unable to create Channel from supplied message data");
                        return ResponseMessage;
                    }
                    else
                    {
                        CurrentChannel = GetChannelByName(RequestChannel.ChannelName);
                        if (CurrentChannel != null)
                        {
                            ResponseMessage = ChannelAlreadyExistsMessage(CurrentClient, CurrentMessage, CurrentChannel);
                            return ResponseMessage;
                        }
                        else
                        {
                            if (String.IsNullOrEmpty(RequestChannel.Guid))
                            {
                                RequestChannel.Guid = Guid.NewGuid().ToString();
                                Log("ProcessCreateChannelMessage adding GUID " + RequestChannel.Guid + " to request (not supplied by requestor)");
                            }

                            RequestChannel.OwnerGuid = CurrentClient.ClientGUID;

                            if (!AddChannel(CurrentClient, RequestChannel))
                            {
                                Log("*** ProcessCreateChannelMessage error while adding channel " + CurrentChannel.Guid);
                                ResponseMessage = ChannelCreateFailureMessage(CurrentClient, CurrentMessage);
                                return ResponseMessage;
                            }

                            if (!AddChannelSubscriber(CurrentClient, RequestChannel))
                            {
                                Log("*** ProcessCreateChannelMessage error while adding channel member " + CurrentClient.IpPort() + " to channel " + CurrentChannel.Guid);
                                ResponseMessage = ChannelJoinFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                                return ResponseMessage;
                            }

                            ResponseMessage = ChannelCreateSuccessMessage(CurrentClient, CurrentMessage, RequestChannel);
                            return ResponseMessage;
                        }
                    }
                }
                else
                {
                    ResponseMessage = ChannelAlreadyExistsMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    return ResponseMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessCreateChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessDeleteChannelMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = new Message();

                if (CurrentChannel == null)
                {
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }

                if (String.Compare(CurrentChannel.OwnerGuid, CurrentClient.ClientGUID) != 0)
                {
                    ResponseMessage = ChannelDeleteFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    return ResponseMessage;
                }

                if (!RemoveChannel(CurrentChannel))
                {
                    Log("*** ProcessDeleteChannelMessage unable to remove channel " + CurrentChannel.Guid);
                    ResponseMessage = ChannelDeleteFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                }
                else
                {
                    ResponseMessage = ChannelDeleteSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                }

                return ResponseMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessDeleteChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListChannelsMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                List<Channel> ret = new List<Channel>();
                List<Channel> filtered = new List<Channel>();
                Channel CurrentChannel = new Channel();

                ret = GetAllChannels();
                if (ret == null || ret.Count < 1)
                {
                    Log("*** ProcessListChannelsMessage no channels retrieved");

                    CurrentMessage = RedactMessage(CurrentMessage);
                    CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                    CurrentMessage.SyncRequest = null;
                    CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                    CurrentMessage.SenderGUID = ServerGUID;
                    CurrentMessage.ChannelGUID = null;
                    CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = SuccessData.ToBytes(null, new List<Channel>());
                    return CurrentMessage;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelsMessage retrieved GetAllChannels after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Channel curr in ret)
                    {
                        CurrentChannel = new Channel();
                        CurrentChannel.Subscribers = null;
                        CurrentChannel.Guid = curr.Guid;
                        CurrentChannel.ChannelName = curr.ChannelName;
                        CurrentChannel.OwnerGuid = curr.OwnerGuid;
                        CurrentChannel.CreatedUTC = curr.CreatedUTC;
                        CurrentChannel.UpdatedUTC = curr.UpdatedUTC;
                        CurrentChannel.Private = curr.Private;
                        CurrentChannel.Broadcast = curr.Broadcast;
                        CurrentChannel.Multicast = curr.Multicast;
                        CurrentChannel.Unicast = curr.Unicast;

                        if (String.Compare(CurrentChannel.OwnerGuid, CurrentClient.ClientGUID) == 0)
                        {
                            filtered.Add(CurrentChannel);
                            continue;
                        }

                        if (CurrentChannel.Private == 0)
                        {
                            filtered.Add(CurrentChannel);
                            continue;
                        }
                    }

                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelsMessage built response list after " + sw.Elapsed.TotalMilliseconds + "ms");
                }
                
                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                CurrentMessage.SenderGUID = ServerGUID;
                CurrentMessage.ChannelGUID = null;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.Success = true;
                CurrentMessage.Data = SuccessData.ToBytes(null, filtered);
                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelsMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListChannelMembersMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = new Message();
                List<Client> Clients = new List<Client>();
                List<Client> ret = new List<Client>();

                if (CurrentChannel == null)
                {
                    Log("*** ProcessListChannelMembersMessage null channel after retrieval by GUID");
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }

                Clients = GetChannelMembers(CurrentChannel.Guid);
                if (Clients == null || Clients.Count < 1)
                {
                    Log("ProcessListChannelMembersMessage channel " + CurrentChannel.Guid + " has no members");
                    ResponseMessage = ChannelNoMembersMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    return ResponseMessage;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelMembersMessage retrieved GetChannelMembers after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Client curr in Clients)
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

                    CurrentMessage = RedactMessage(CurrentMessage);
                    CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                    CurrentMessage.SyncRequest = null;
                    CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                    CurrentMessage.SenderGUID = ServerGUID;
                    CurrentMessage.ChannelGUID = CurrentChannel.Guid;
                    CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = SuccessData.ToBytes(null, ret);
                    return CurrentMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelMembersMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListChannelSubscribersMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Channel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGUID);
                Message ResponseMessage = new Message();
                List<Client> Clients = new List<Client>();
                List<Client> ret = new List<Client>();

                if (CurrentChannel == null)
                {
                    Log("*** ProcessListChannelSubscribersMessage null channel after retrieval by GUID");
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }

                if (CurrentChannel.Broadcast == 1)
                {
                    Log("ProcessListChannelSubscribersMessage channel is broadcast, calling ProcessListChannelMembers");
                    return ProcessListChannelMembersMessage(CurrentClient, CurrentMessage);
                }

                Clients = GetChannelSubscribers(CurrentChannel.Guid);
                if (Clients == null || Clients.Count < 1)
                {
                    Log("ProcessListChannelSubscribersMessage channel " + CurrentChannel.Guid + " has no subscribers");
                    ResponseMessage = ChannelNoSubscribersMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    return ResponseMessage;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelSubscribersMessage retrieved GetChannelSubscribers after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Client curr in Clients)
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

                    CurrentMessage = RedactMessage(CurrentMessage);
                    CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                    CurrentMessage.SyncRequest = null;
                    CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                    CurrentMessage.SenderGUID = ServerGUID;
                    CurrentMessage.ChannelGUID = CurrentChannel.Guid;
                    CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = Encoding.UTF8.GetBytes(Helper.SerializeJson(ret));
                    return CurrentMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListChannelSubscribersMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private Message ProcessListClientsMessage(Client CurrentClient, Message CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                List<Client> Clients = new List<Client>();
                List<Client> ret = new List<Client>();

                Clients = GetAllClients();
                if (Clients == null || Clients.Count < 1)
                {
                    Log("*** ProcessListClientsMessage no clients retrieved");
                    return null;
                }
                else
                {
                    if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListClientsMessage retrieved GetAllClients after " + sw.Elapsed.TotalMilliseconds + "ms");

                    foreach (Client curr in Clients)
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

                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
                CurrentMessage.SenderGUID = ServerGUID;
                CurrentMessage.ChannelGUID = null;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.Success = true;
                CurrentMessage.Data = SuccessData.ToBytes(null, ret);
                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Log("ProcessListClientsMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGUID + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion

        #region Private-Message-Builders

        private Message AuthorizationFailedMessage(Message CurrentMessage)
        {
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.SyncResponse = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.AuthorizationFailed, "Authorization failed", null);
            return CurrentMessage;
        }

        private Message LoginRequiredMessage()
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;
            ResponseMessage.Data = FailureData.ToBytes(ErrorTypes.LoginRequired, "Login required", null);
            return ResponseMessage;
        }

        private Message HeartbeatRequestMessage(Client CurrentClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.MessageID = Guid.NewGuid().ToString();
            ResponseMessage.RecipientGUID = CurrentClient.ClientGUID; 
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.Command = "HeartbeatRequest";
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Data = null;
            return ResponseMessage;
        }

        private Message UnknownCommandMessage(Client CurrentClient, Message CurrentMessage)
        {
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnknownCommand, "Unknown command", CurrentMessage.Command);
            return CurrentMessage;
        }

        private Message RecipientNotFoundMessage(Client CurrentClient, Message CurrentMessage)
        {
            string originalRecipientGUID = CurrentMessage.RecipientGUID;
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;

            if (!String.IsNullOrEmpty(originalRecipientGUID))
            {
                CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.RecipientNotFound, "Unknown recipient", originalRecipientGUID);
            }
            else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGUID))
            {
                CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.ChannelNotFound, "Unknown channel", CurrentMessage.ChannelGUID);
            }
            else
            {
                CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.BadRequest, "No recipient or channel supplied", null);
            }
            return CurrentMessage;
        }

        private Message NotChannelMemberMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.NotAChannelMember, "You are not a member of this channel", CurrentChannel.Guid);
            return CurrentMessage;
        }
        
        private Message MessageQueueSuccess(Client CurrentClient, Message CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;

            if (!String.IsNullOrEmpty(CurrentMessage.RecipientGUID))
            {
                #region Individual-Recipient

                CurrentMessage.Data = SuccessData.ToBytes("Message queued to recipient", CurrentMessage.RecipientGUID);
                return CurrentMessage;

                #endregion
            }
            else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGUID))
            {
                #region Channel-Recipient

                CurrentMessage.Data = SuccessData.ToBytes("Message queued to channel", CurrentMessage.ChannelGUID);
                return CurrentMessage;

                #endregion
            }
            else
            {
                #region Unknown-Recipient

                return RecipientNotFoundMessage(CurrentClient, CurrentMessage);

                #endregion
            }
        }

        private Message MessageQueueFailure(Client CurrentClient, Message CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToQueue, "Unable to queue message", null);
            return CurrentMessage;
        }

        private Message ChannelNotFoundMessage(Client CurrentClient, Message CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.ChannelNotFound, "Channel not found", CurrentMessage.ChannelGUID);
            return CurrentMessage;
        }

        private Message ChannelNoMembersMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.NoChannelMembers, "No members in channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelNoSubscribersMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.NoChannelSubscribers, "No subscribers in channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelAlreadyExistsMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.ChannelAlreadyExists, "Channel already exists", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelCreateSuccessMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = SuccessData.ToBytes("Channel created successfully", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelCreateFailureMessage(Client CurrentClient, Message CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToCreateChannel, "Unable to create channel", null);
            return CurrentMessage;
        }

        private Message ChannelJoinSuccessMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = SuccessData.ToBytes("Successfully joined channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelLeaveSuccessMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = SuccessData.ToBytes("Successfully left channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelSubscribeSuccessMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = SuccessData.ToBytes("Successfully subscribed to channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelUnsubscribeSuccessMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = SuccessData.ToBytes("Successfully unsubscribed from channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelJoinFailureMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToJoinChannel, "Unable to join channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelLeaveFailureMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToLeaveChannel, "Unable to leave channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelSubscribeFailureMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToSubscribeChannel, "Unable to subscribe to channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelUnsubscribeFailureMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToUnsubscribeChannel, "Unable to unsubscribe from channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelDeletedByOwnerMessage(Client CurrentClient, Channel CurrentChannel)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = CurrentClient.ClientGUID;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = SuccessData.ToBytes("Channel deleted by owner", CurrentChannel.Guid);
            return ResponseMessage;
        }

        private Message ChannelDeleteSuccessMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = SuccessData.ToBytes("Successfully deleted channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message ChannelDeleteFailureMessage(Client CurrentClient, Message CurrentMessage, Channel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.ChannelGUID = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToDeleteChannel, "Unable to delete channel", CurrentChannel.Guid);
            return CurrentMessage;
        }

        private Message DataErrorMessage(Client CurrentClient, Message CurrentMessage, string message)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGUID = CurrentMessage.SenderGUID;
            CurrentMessage.SenderGUID = ServerGUID;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = FailureData.ToBytes(ErrorTypes.DataError, "Data error encountered", message);
            return CurrentMessage;
        }

        private Message ServerJoinEventMessage(Client NewClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;
            ResponseMessage.Command = "Event";
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientJoinedServer, NewClient.ClientGUID);
            return ResponseMessage;
        }

        private Message ServerLeaveEventMessage(Client LeavingClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;
            ResponseMessage.Command = "Event";
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientLeftServer, LeavingClient.ClientGUID);
            return ResponseMessage;
        }

        private Message ChannelJoinEventMessage(Channel CurrentChannel, Client NewClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;
            ResponseMessage.Command = "Event";
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientJoinedChannel, NewClient.ClientGUID);
            return ResponseMessage;
        }

        private Message ChannelLeaveEventMessage(Channel CurrentChannel, Client LeavingClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.Command = "Event";
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientLeftChannel, LeavingClient.ClientGUID);
            return ResponseMessage;
        }

        private Message SubscriberJoinEventMessage(Channel CurrentChannel, Client NewClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;
            ResponseMessage.Command = "Event";
            ResponseMessage.Data = EventData.ToBytes(EventTypes.SubscriberJoinedChannel, NewClient.ClientGUID);
            return ResponseMessage;
        }

        private Message SubscriberLeaveEventMessage(Channel CurrentChannel, Client LeavingClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.Command = "Event";
            ResponseMessage.Data = EventData.ToBytes(EventTypes.SubscriberLeftChannel, LeavingClient.ClientGUID);
            return ResponseMessage;
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
