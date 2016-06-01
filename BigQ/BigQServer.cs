using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ
{
    public class BigQServer
    {
        #region Private-Class-Members

        //
        // configuration
        //
        private DateTime Created;
        private bool SendAcknowledgements;
        private bool SendServerJoinNotifications;
        private bool SendChannelJoinNotifications;
        private bool ConsoleDebug;
        private int HeartbeatIntervalMsec;
        private int MaxHeartbeatFailures;
        private bool LogLockMethodResponseTime = false;
        private bool LogMessageResponseTime = false;

        //
        // resources
        //
        private ConcurrentDictionary<string, BigQClient> Clients;       // IpPort(), Client
        private ConcurrentDictionary<string, string> ClientGuidMap;     // Guid, IpPort()
        private ConcurrentDictionary<string, BigQChannel> Channels;     // Guid, Channel
        
        //
        // TCP networking
        //
        private string TCPListenerIP;
        private IPAddress TCPListenerIPAddress;
        private int TCPListenerPort;
        private TcpListener TCPListener;
        private int TCPActiveConnectionThreads;
        private CancellationTokenSource TCPCancellationTokenSource;
        private CancellationToken TCPCancellationToken;

        //
        // websocket networking
        //
        private string WSListenerIP;
        private IPAddress WSListenerIPAddress;
        private int WSListenerPort;
        private HttpListener WSListener;
        private int WSActiveConnectionThreads;
        private CancellationTokenSource WSCancellationTokenSource;
        private CancellationToken WSCancellationToken;

        #endregion

        #region Public-Delegates

        /// <summary>
        /// This function is called when the server receives a message from a connected client.
        /// </summary>
        public Func<BigQMessage, bool> MessageReceived;

        /// <summary>
        /// This function is called when the server stops.
        /// </summary>
        public Func<bool> ServerStopped;

        /// <summary>
        /// This function is called when a client connects to the server.
        /// </summary>
        public Func<BigQClient, bool> ClientConnected;

        /// <summary>
        /// This function is called when a client issues the login command.
        /// </summary>
        public Func<BigQClient, bool> ClientLogin;

        /// <summary>
        /// This function is called when a client disconnects from the server.
        /// </summary>
        public Func<BigQClient, bool> ClientDisconnected;

        /// <summary>
        /// This function is called when a log message needs to be sent from the server.
        /// </summary>
        public Func<string, bool> LogMessage;

        #endregion

        #region Public-Constructor

        /// <summary>
        /// Start an instance of the BigQ server process.
        /// </summary>
        /// <param name="ipAddressTcp">IP address used to listen for TCP connections.  Use '+' to represent any interface.</param>
        /// <param name="portTcp">TCP port used to listen for TCP connections.</param>
        /// <param name="ipAddressWebsocket">IP address used to listen for websocket connections.  Use '+' to represent any interface.</param>
        /// <param name="portWebsocket">TCP port used to listen for websocket connections.</param>
        /// <param name="debug">Specify whether debugging to console is enabled or not.</param>
        /// <param name="sendAck">Specify whether the server should send acknowledgements to clients that send messages.</param>
        /// <param name="sendServerJoinNotifications">Specify whether the server should send notifications to existing clients when a new client joins the server.</param>
        /// <param name="sendChannelJoinNotifications">Specify whether the server should send notifications to existing channel members when a new client joins the channel.</param>
        /// <param name="heartbeatIntervalMsec">Specifythe interval, in milliseconds, at which the server will send heartbeat messages.</param>
        public BigQServer(
            string ipAddressTcp, 
            int portTcp,
            string ipAddressWebsocket,
            int portWebsocket, 
            bool debug, 
            bool sendAck, 
            bool sendServerJoinNotifications, 
            bool sendChannelJoinNotifications,
            int heartbeatIntervalMsec)
        {
            #region Check-for-Invalid-Values

            if (portTcp < 1) throw new ArgumentOutOfRangeException("portTcp");
            if (portWebsocket < 1) throw new ArgumentOutOfRangeException("portWebsocket");
            if (heartbeatIntervalMsec < 100 && heartbeatIntervalMsec != 0) throw new ArgumentOutOfRangeException("heartbeatIntervalMsec");
            
            #endregion

            #region Set-Class-Variables

            Created = DateTime.Now.ToUniversalTime();
            SendAcknowledgements = sendAck;
            SendServerJoinNotifications = sendServerJoinNotifications;
            SendChannelJoinNotifications = sendChannelJoinNotifications;
            ConsoleDebug = debug;

            Clients = new ConcurrentDictionary<string, BigQClient>();
            Channels = new ConcurrentDictionary<string, BigQChannel>();
            ClientGuidMap = new ConcurrentDictionary<string, string>();

            TCPListenerIP = ipAddressTcp;
            TCPListenerPort = portTcp;
            WSListenerIP = ipAddressWebsocket;
            WSListenerPort = portWebsocket;

            TCPActiveConnectionThreads = 0;
            WSActiveConnectionThreads = 0;
            HeartbeatIntervalMsec = heartbeatIntervalMsec;
            MaxHeartbeatFailures = 5;
            
            #endregion

            #region Set-Delegates-to-Null

            MessageReceived = null;
            ServerStopped = null;
            ClientConnected = null;
            ClientLogin = null;
            ClientDisconnected = null;
            LogMessage = null;

            #endregion

            #region Stop-Existing-Tasks

            if (TCPCancellationTokenSource != null) TCPCancellationTokenSource.Cancel();
            if (WSCancellationTokenSource != null) WSCancellationTokenSource.Cancel();
            
            #endregion

            #region Start-TCP-Server

            if (String.IsNullOrEmpty(TCPListenerIP))
            {
                TCPListenerIPAddress = System.Net.IPAddress.Any;
                TCPListenerIP = TCPListenerIPAddress.ToString();
            }
            else
            {
                TCPListenerIPAddress = IPAddress.Parse(TCPListenerIP);
            }

            TCPListener = new TcpListener(TCPListenerIPAddress, TCPListenerPort);
            Log("Starting TCP server at: tcp://" + TCPListenerIP + ":" + TCPListenerPort);

            TCPCancellationTokenSource = new CancellationTokenSource();
            TCPCancellationToken = TCPCancellationTokenSource.Token;
            Task.Run(() => TCPAcceptConnections(), TCPCancellationToken);

            #endregion

            #region Start-Websocket-Server

            if (String.IsNullOrEmpty(WSListenerIP))
            {
                WSListenerIPAddress = System.Net.IPAddress.Any;
                WSListenerIP = "+";
            }

            string prefix = "http://" + WSListenerIP + ":" + WSListenerPort + "/";
            WSListener = new HttpListener();
            WSListener.Prefixes.Add(prefix);
            Log("Starting Websocket server at: " + prefix);

            WSCancellationTokenSource = new CancellationTokenSource();
            WSCancellationToken = WSCancellationTokenSource.Token;
            Task.Run(() => WSAcceptConnections(), WSCancellationToken);

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
            if (WSCancellationTokenSource != null) WSCancellationTokenSource.Cancel();
            return;
        }
        
        /// <summary>
        /// Enumerate all channels.
        /// </summary>
        /// <returns>List of BigQChannel objects.</returns>
        public List<BigQChannel> ListChannels()
        {
            return GetAllChannels();
        }

        /// <summary>
        /// Enumerate all subscribers in a given channel.
        /// </summary>
        /// <returns>List of BigQClient objects.</returns>
        public List<BigQClient> ListChannelSubscribers(string guid)
        {
            return GetChannelSubscribers(guid);
        }

        /// <summary>
        /// Enumerate all clients.
        /// </summary>
        /// <returns>List of BigQClient objects.</returns>
        public List<BigQClient> ListClients()
        {
            return GetAllClients();
        }

        /// <summary>
        /// Enumerate all client GUID to IP:port maps.
        /// </summary>
        /// <returns>A dictionary containing client GUIDs (keys) and IP:port strings (values).</returns>
        public Dictionary<string, string> ListClientGuidMaps()
        {
            return GetAllClientGuidMaps();
        }

        /// <summary>
        /// Retrieve the connection count.
        /// </summary>
        /// <returns>An int containing the number of active connections (sum of websocket and TCP).</returns>
        public int ConnectionCount()
        {
            return TCPActiveConnectionThreads + WSActiveConnectionThreads;
        }
        
        #endregion
        
        #region Private-Transport-and-Connection-Methods

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
                        #region Increment-Counters

                        TCPActiveConnectionThreads++;

                        //
                        //
                        // Do not decrement in this block, decrement is done by the connection reader
                        //
                        //

                        #endregion

                        #region Get-Tuple

                        string ClientIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                        int ClientPort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;
                        Log("TCPAcceptConnections accepted connection from " + ClientIp + ":" + ClientPort);

                        #endregion

                        #region Add-to-Client-List

                        BigQClient CurrentClient = new BigQClient();
                        CurrentClient.SourceIp = ClientIp;
                        CurrentClient.SourcePort = ClientPort;
                        CurrentClient.ClientTCPInterface = Client;
                        CurrentClient.ClientHTTPContext = null;
                        CurrentClient.ClientWSContext = null;
                        CurrentClient.ClientWSInterface = null;

                        CurrentClient.IsTCP = true;
                        CurrentClient.IsWebsocket = false;
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

                        Log("TCPAcceptConnections starting data receiver for " + CurrentClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)");
                        Task.Run(() => TCPDataReceiver(CurrentClient), TCPCancellationToken);

                        #endregion

                        #region Start-Heartbeat-Manager

                        if (HeartbeatIntervalMsec > 0)
                        {
                            Log("TCPAcceptConnections starting heartbeat manager for " + CurrentClient.IpPort());
                            Task.Run(() => TCPHeartbeatManager(CurrentClient), TCPCancellationToken);
                        }

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
                        #region Increment-Counters

                        WSActiveConnectionThreads++;

                        //
                        //
                        // Do not decrement in this block, decrement is done by the connection reader
                        //
                        //

                        #endregion

                        #region Get-Tuple

                        string ClientIp = Context.Request.RemoteEndPoint.Address.ToString();
                        int ClientPort = Context.Request.RemoteEndPoint.Port;
                        Log("WSAcceptConnections accepted connection from " + ClientIp + ":" + ClientPort);

                        #endregion

                        #region Get-Websocket-Context

                        WebSocketContext wsContext = null;
                        try
                        {
                            wsContext = Context.AcceptWebSocketAsync(subProtocol: null).Result;
                        }
                        catch (Exception)
                        {
                            Log("*** WSSetupConnection exception while gathering websocket context for client " + ClientIp + ":" + ClientPort);
                            Context.Response.StatusCode = 500;
                            Context.Response.Close();
                            return;
                        }

                        WebSocket Client = wsContext.WebSocket;

                        #endregion

                        #region Add-to-Client-List

                        BigQClient CurrentClient = new BigQClient();
                        CurrentClient.SourceIp = ClientIp;
                        CurrentClient.SourcePort = ClientPort;
                        CurrentClient.ClientTCPInterface = null;
                        CurrentClient.ClientHTTPContext = Context;
                        CurrentClient.ClientWSContext = wsContext;
                        CurrentClient.ClientWSInterface = Client;

                        CurrentClient.IsTCP = false;
                        CurrentClient.IsWebsocket = true;
                        CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                        CurrentClient.UpdatedUTC = DateTime.Now.ToUniversalTime();

                        if (!AddClient(CurrentClient))
                        {
                            Log("*** WSSetupConnection unable to add client " + CurrentClient.IpPort());
                            Context.Response.StatusCode = 500;
                            Context.Response.Close();
                            return;
                        }

                        #endregion

                        #region Start-Data-Receiver

                        Log("WSSetupConnection starting data receiver for " + CurrentClient.IpPort() + " (now " + WSActiveConnectionThreads + " connections active)");
                        Task.Run(() => WSDataReceiver(CurrentClient), WSCancellationToken);

                        #endregion

                        #region Start-Heartbeat-Manager

                        if (HeartbeatIntervalMsec > 0)
                        {
                            Log("WSSetupConnection starting heartbeat manager for " + CurrentClient.IpPort());
                            Task.Run(() => WSHeartbeatManager(CurrentClient), WSCancellationToken);
                        }

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
        
        private void TCPDataReceiver(BigQClient CurrentClient)
        {
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

                    if (!CurrentClient.ClientTCPInterface.Connected || !BigQHelper.IsTCPPeerConnected(CurrentClient.ClientTCPInterface))
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

                        if (SendServerJoinNotifications) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        break;
                    }
                    else
                    {
                        // Log("TCPDataReceiver client " + CurrentClient.IpPort() + " is still connected");
                    }

                    #endregion
                    
                    #region Retrieve-Message

                    BigQMessage CurrentMessage = BigQHelper.TCPMessageRead(CurrentClient.ClientTCPInterface);
                    if (CurrentMessage == null)
                    {
                        Log("*** TCPDataReceiver unable to read from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        Log("TCPDataReceiver successfully received message from client " + CurrentClient.IpPort());   
                    }

                    if (!CurrentMessage.IsValid())
                    {
                        Log("TCPDataReceiver invalid message received from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        Log("TCPDataReceiver valid message received from client " + CurrentClient.IpPort());
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(CurrentClient, CurrentMessage);
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
                Log("TCPDataReceiver closing data receiver for " + CurrentClient.IpPort() + " (now " + TCPActiveConnectionThreads + " connections active)"); 
                TCPActiveConnectionThreads--;
            }
        }

        private void WSDataReceiver(BigQClient CurrentClient)
        {
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

                    if (!BigQHelper.IsWSPeerConnected(CurrentClient.ClientWSInterface))
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

                        if (SendServerJoinNotifications) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        break;
                    }
                    else
                    {
                        // Log("TCPDataReceiver client " + CurrentClient.IpPort() + " is still connected");
                    }

                    #endregion

                    #region Retrieve-Message

                    Task<BigQMessage> MessageTask = BigQHelper.WSMessageRead(CurrentClient.ClientHTTPContext, CurrentClient.ClientWSInterface);
                    if (MessageTask == null)
                    {
                        Log("*** WSDataReceiver unable to read from client " + CurrentClient.IpPort() + " (message read task failed)");
                        continue;
                    }

                    BigQMessage CurrentMessage = MessageTask.Result;
                    if (CurrentMessage == null)
                    {
                        Log("WSDataReceiver unable to read message from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        Log("WSDataReceiver successfully received message from client " + CurrentClient.IpPort());
                        Task.Run(() => MessageReceived(CurrentMessage));
                    }

                    if (!CurrentMessage.IsValid())
                    {
                        Log("WSDataReceiver invalid message received from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        Log("WSDataReceiver valid message received from client " + CurrentClient.IpPort());
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(CurrentClient, CurrentMessage);
                    if (MessageReceived != null) Task.Run(() => MessageReceived(CurrentMessage));
                    // Log("WSDataReceiver finished processing message from client " + CurrentClient.IpPort());

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
                Log("WSDataReceiver closing data receiver for " + CurrentClient.IpPort() + " (now " + WSActiveConnectionThreads + " connections active)");
                WSActiveConnectionThreads--;
            }
        }

        private bool TCPDataSender(BigQClient CurrentClient, BigQMessage Message)
        {
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
                    Log("*** TCPDataSender null TcpClient supplied within client object for client " + CurrentClient.ClientGuid);
                    return false;
                }

                if (Message == null)
                {
                    Log("*** TCPDataSender null message supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (!BigQHelper.IsTCPPeerConnected(CurrentClient.ClientTCPInterface))
                {
                    Log("TCPDataSender client " + CurrentClient.IpPort() + " not connected");
                    return false;
                }

                #endregion

                #region Send-Message

                if (!BigQHelper.TCPMessageWrite(CurrentClient.ClientTCPInterface, Message))
                {
                    Log("TCPDataSender unable to send data to client " + CurrentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(Message.Command))
                    {
                        Log("TCPDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command " + Message.Command);
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
        }

        private bool WSDataSender(BigQClient CurrentClient, BigQMessage Message)
        {
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
                    Log("*** WSDataSender null websocket supplied within client object for client " + CurrentClient.ClientGuid);
                    return false;
                }

                if (Message == null)
                {
                    Log("*** WSDataSender null message supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (!BigQHelper.IsWSPeerConnected(CurrentClient.ClientWSInterface))
                {
                    Log("WSDataSender client " + CurrentClient.IpPort() + " not connected");
                    return false;
                }

                #endregion

                #region Send-Message

                Task<bool> MessageTask = BigQHelper.WSMessageWrite(CurrentClient.ClientHTTPContext, CurrentClient.ClientWSInterface, Message);
                if (MessageTask == null)
                {
                    Log("*** WSDataSender unable to send to client " + CurrentClient.IpPort() + " (message read task failed)");
                    return false;
                }

                bool success = MessageTask.Result;
                if (!success)
                {
                    Log("WSDataSender unable to send data to client " + CurrentClient.IpPort());
                    return false;
                }
                else
                {
                    if (!String.IsNullOrEmpty(Message.Command))
                    {
                        Log("WSDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command " + Message.Command);
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
        }

        private void TCPHeartbeatManager(BigQClient CurrentClient)
        {
            try
            {
                #region Check-for-Disable

                if (HeartbeatIntervalMsec == 0)
                {
                    Log("*** TCPHeartbeatManager disabled");
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
                        Thread.Sleep(HeartbeatIntervalMsec);
                    }

                    #endregion
                    
                    #region Check-if-Client-Connected
                    
                    if (!BigQHelper.IsTCPPeerConnected(CurrentClient.ClientTCPInterface))
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

                        if (SendServerJoinNotifications) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        return;
                    }

                    #endregion

                    #region Send-Heartbeat-Message
                    
                    lastHeartbeatAttempt = DateTime.Now;

                    BigQMessage HeartbeatMessage = HeartbeatRequestMessage(CurrentClient);
                    if (!TCPDataSender(CurrentClient, HeartbeatMessage))
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** TCPHeartbeatManager failed to send heartbeat to client " + CurrentClient.IpPort() + " (" + numConsecutiveFailures + "/" + MaxHeartbeatFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= MaxHeartbeatFailures)
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

                            if (SendServerJoinNotifications) Task.Run(() => ServerLeaveEvent(CurrentClient));

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

        private void WSHeartbeatManager(BigQClient CurrentClient)
        {
            try
            {
                #region Check-for-Disable

                if (HeartbeatIntervalMsec == 0)
                {
                    Log("*** WSHeartbeatManager disabled");
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
                        Thread.Sleep(HeartbeatIntervalMsec);
                    }

                    #endregion

                    #region Check-if-Client-Connected

                    if (!BigQHelper.IsWSPeerConnected(CurrentClient.ClientWSInterface))
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

                        if (SendServerJoinNotifications) Task.Run(() => ServerLeaveEvent(CurrentClient));
                        return;
                    }

                    #endregion

                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;

                    bool success = false;
                    BigQMessage HeartbeatMessage = HeartbeatRequestMessage(CurrentClient);
                    Task<bool> MessageTask = BigQHelper.WSMessageWrite(CurrentClient.ClientHTTPContext, CurrentClient.ClientWSInterface, HeartbeatMessage);
                    if (MessageTask != null) success = MessageTask.Result;

                    if (!success)
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** WSHeartbeatManager failed to send heartbeat to client " + CurrentClient.IpPort() + " (" + numConsecutiveFailures + "/" + MaxHeartbeatFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= MaxHeartbeatFailures)
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

                            if (SendServerJoinNotifications) Task.Run(() => ServerLeaveEvent(CurrentClient));

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

        private bool DataSender(BigQClient CurrentClient, BigQMessage Message)
        {
            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** DataSender null client supplied");
                    return false;
                }

                if (!CurrentClient.IsTCP && !CurrentClient.IsWebsocket)
                {
                    Log("*** DataSender unable to discern transport for client " + CurrentClient.IpPort());
                    return false;
                }

                if (Message == null)
                {
                    Log("*** DataSender null message supplied");
                    return false;
                }

                #endregion

                #region Process

                if (CurrentClient.IsTCP) return TCPDataSender(CurrentClient, Message);
                else if (CurrentClient.IsWebsocket) return WSDataSender(CurrentClient, Message);
                else
                {
                    Log("*** DataSender unable to discern transport for client " + CurrentClient.IpPort());
                    return false;
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("DataSender (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("DataSender (null)", EOuter);
                }

                return false;
            }
        }

        private bool ChannelDataSender(BigQClient CurrentClient, BigQChannel CurrentChannel, BigQMessage Message)
        {
            List<BigQClient> CurrentChannelClients = GetChannelSubscribers(CurrentChannel.Guid);
            if (CurrentChannelClients == null || CurrentChannelClients.Count < 1)
            {
                Log("*** ChannelDataSender no clients found in channel " + CurrentChannel.Guid);
                return true;
            }

            Message.SenderGuid = CurrentClient.ClientGuid;
            foreach (BigQClient curr in CurrentChannelClients)
            {
                Task.Run(() =>
                {
                    Message.RecipientGuid = curr.ClientGuid;
                    bool ResponseSuccess = false;
                    ResponseSuccess = DataSender(curr, Message);
                    if (!ResponseSuccess)
                    {
                        Log("*** ChannelDataSender error sending channel message from " + Message.SenderGuid + " to client " + Message.RecipientGuid + " in channel " + Message.ChannelGuid);
                    }
                });
            }

            return true;
        }

        #endregion

        //
        // Methods below are transport agnostic
        //

        #region Private-Event-Methods

        private bool ServerJoinEvent(BigQClient CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** ServerJoinEvent null BigQClient supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGuid))
            {
                Log("*** ServerJoinEvent null ClientGuid suplied within BigQClient");
                return true;
            }

            Log("ServerJoinEvent sending server join notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGuid);

            List<BigQClient> CurrentServerClients = GetAllClients(); 
            if (CurrentServerClients == null || CurrentServerClients.Count < 1)
            {
                Log("*** ServerJoinEvent no clients found on server");
                return true;
            }

            BigQMessage Message = ServerJoinEventMessage(CurrentClient);

            foreach (BigQClient curr in CurrentServerClients)
            {
                if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = DataSender(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ServerJoinEvent error sending server join event to " + Message.RecipientGuid + " (join by " + CurrentClient.ClientGuid + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ServerLeaveEvent(BigQClient CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** ServerLeaveEvent null BigQClient supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGuid))
            {
                Log("*** ServerLeaveEvent null ClientGuid suplied within BigQClient");
                return true;
            }

            Log("ServerLeaveEvent sending server leave notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGuid);

            List<BigQClient> CurrentServerClients = GetAllClients();
            if (CurrentServerClients == null || CurrentServerClients.Count < 1)
            {
                Log("*** ServerLeaveEvent no clients found on server");
                return true;
            }

            BigQMessage Message = ServerLeaveEventMessage(CurrentClient);

            foreach (BigQClient curr in CurrentServerClients)
            {
                if (!String.IsNullOrEmpty(curr.ClientGuid))
                {
                    if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) != 0)
                    {
                        /*
                        Task.Run(() =>
                        {
                            Message.RecipientGuid = curr.ClientGuid;
                            bool ResponseSuccess = TCPDataSender(curr, Message);
                            if (!ResponseSuccess)
                            {
                                Log("*** ServerLeaveEvent error sending server leave event to " + Message.RecipientGuid + " (leave by " + CurrentClient.ClientGuid + ")");
                            }
                        });
                        */
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = DataSender(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ServerLeaveEvent error sending server leave event to " + Message.RecipientGuid + " (leave by " + CurrentClient.ClientGuid + ")");
                        }
                        else
                        {
                            Log("ServerLeaveEvent sent server leave event to " + Message.RecipientGuid + " (leave by " + CurrentClient.ClientGuid + ")");
                        }
                    }
                }
            }

            return true;
        }

        private bool ChannelJoinEvent(BigQClient CurrentClient, BigQChannel CurrentChannel)
        {
            if (CurrentClient == null)
            {
                Log("*** ChannelJoinEvent null BigQClient supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGuid))
            {
                Log("*** ChannelJoinEvent null ClientGuid supplied within BigQClient");
                return true;
            }

            if (CurrentChannel == null)
            {
                Log("*** ChannelJoinEvent null BigQChannel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentChannel.Guid))
            {
                Log("*** ChannelJoinEvent null GUID supplied within BigQChannel");
                return true;
            }

            Log("ChannelJoinEvent sending channel join notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGuid + " channel " + CurrentChannel.Guid);

            List<BigQClient> CurrentChannelClients = GetChannelSubscribers(CurrentChannel.Guid);
            if (CurrentChannelClients == null || CurrentChannelClients.Count < 1)
            {
                Log("*** ChannelJoinEvent no clients found in channel " + CurrentChannel.Guid);
                return true;
            }

            BigQMessage Message = ChannelJoinEventMessage(CurrentChannel, CurrentClient);

            foreach (BigQClient curr in CurrentChannelClients)
            {
                if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = DataSender(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ChannelJoinEvent error sending channel join event to " + Message.RecipientGuid + " for channel " + Message.ChannelGuid + " (join by " + CurrentClient.ClientGuid + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ChannelLeaveEvent(BigQClient CurrentClient, BigQChannel CurrentChannel)
        {
            if (CurrentClient == null)
            {
                Log("*** ChannelLeaveEvent null BigQClient supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentClient.ClientGuid))
            {
                Log("*** ChannelLeaveEvent null ClientGuid supplied within BigQClient");
                return true;
            }

            if (CurrentChannel == null)
            {
                Log("*** ChannelLeaveEvent null BigQChannel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(CurrentChannel.Guid))
            {
                Log("*** ChannelLeaveEvent null GUID supplied within BigQChannel");
                return true;
            }

            Log("ChannelLeaveEvent sending channel leave notification for " + CurrentClient.IpPort() + " GUID " + CurrentClient.ClientGuid + " channel " + CurrentChannel.Guid);

            List<BigQClient> CurrentChannelClients = GetChannelSubscribers(CurrentChannel.Guid);
            if (CurrentChannelClients == null || CurrentChannelClients.Count < 1)
            {
                Log("*** ChannelLeaveEvent no clients found in channel " + CurrentChannel.Guid);
                return true;
            }

            BigQMessage Message = ChannelLeaveEventMessage(CurrentChannel, CurrentClient);

            foreach (BigQClient curr in CurrentChannelClients)
            {
                if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) != 0)
                {
                    Task.Run(() =>
                    {
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = DataSender(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ChannelLeaveEvent error sending channel leave event to " + Message.RecipientGuid + " for channel " + Message.ChannelGuid + " (leave by " + CurrentClient.ClientGuid + ")");
                        }
                    });
                }
            }

            return true;
        }

        #endregion
        
        #region Private-Locked-Methods

        //
        // Ensure that none of these methods call another method within this region
        // otherwise you have a lock within a lock!  There should be NO methods
        // outside of this region that have a lock statement
        //

        private BigQClient GetClientByGuid(string guid)
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

                if (ClientGuidMap == null || ClientGuidMap.Count < 1)
                {
                    Log("*** GetClientByGuid no GUID map entries");
                    return null;
                }

                string ipPort = null;
                if (ClientGuidMap.TryGetValue(guid, out ipPort))
                {
                    if (!String.IsNullOrEmpty(ipPort))
                    {
                        BigQClient existingClient = null;
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
                if (LogLockMethodResponseTime) Log("GetClientByGuid " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<BigQClient> GetAllClients()
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

                List<BigQClient> ret = new List<BigQClient>();
                foreach (KeyValuePair<string, BigQClient> curr in Clients)
                {
                    if (!String.IsNullOrEmpty(curr.Value.ClientGuid))
                    {
                        /*
                        if (curr.Value.IsTCP) Console.WriteLine("GetAllClients adding TCP client " + curr.Value.IpPort() + " GUID " + curr.Value.ClientGuid + " to list");
                        else if (curr.Value.IsWebsocket) Console.WriteLine("GetAllClients adding websocket client " + curr.Value.IpPort() + " GUID " + curr.Value.ClientGuid + " to list");
                        else Console.WriteLine("GetAllClients adding unknown client " + curr.Value.IpPort() + " GUID " + curr.Value.ClientGuid + " to list");
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
                if (LogLockMethodResponseTime) Log("GetAllClients " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }
        
        private Dictionary<string, string> GetAllClientGuidMaps()
        {
            if (ClientGuidMap == null || ClientGuidMap.Count < 1) return new Dictionary<string, string>();
            Dictionary<string, string> ret = ClientGuidMap.ToDictionary(entry => entry.Key, entry => entry.Value);
            return ret;
        }

        private BigQChannel GetChannelByGuid(string guid)
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

                BigQChannel ret = null;
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
                if (LogLockMethodResponseTime) Log("GetChannelByGuid " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<BigQChannel> GetAllChannels()
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

                List<BigQChannel> ret = new List<BigQChannel>();
                foreach (KeyValuePair<string, BigQChannel> curr in Channels)
                {
                    ret.Add(curr.Value);
                }

                Log("GetAllChannels returning " + ret.Count + " channels");
                return ret;
            }
            finally
            {
                sw.Stop();
                if (LogLockMethodResponseTime) Log("GetAllChannels " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private List<BigQClient> GetChannelSubscribers(string guid)
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

                List<BigQClient> ret = new List<BigQClient>();
                
                foreach (KeyValuePair<string, BigQChannel> curr in Channels)
                {
                    if (String.Compare(curr.Value.Guid, guid) == 0)
                    {
                        foreach (BigQClient CurrentClient in curr.Value.Subscribers)
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
                if (LogLockMethodResponseTime) Log("GetChannelSubscribers " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQChannel GetChannelByName(string name)
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

                BigQChannel ret = null;
                
                foreach (KeyValuePair<string, BigQChannel> curr in Channels)
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
                if (LogLockMethodResponseTime) Log("GetChannelByName " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool AddClient(BigQClient CurrentClient)
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
                else if (CurrentClient.IsWebsocket) Log("AddClient adding websocket client " + CurrentClient.IpPort() + " with " + Clients.Count + " entries in client list");
                else Log("AddClient adding UNKNOWN client " + CurrentClient.IpPort() + " with " + Clients.Count + " entries in client list");

                BigQClient removedClient = null;
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
                if (LogLockMethodResponseTime) Log("AddClient " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveClient(BigQClient CurrentClient)
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

                Log("RemoveClient removing client " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid);

                //
                // remove client entry
                //
                if (Clients.ContainsKey(CurrentClient.IpPort()))
                {
                    BigQClient removedClient = null;
                    if (!Clients.TryRemove(CurrentClient.IpPort(), out removedClient))
                    {
                        Log("*** Unable to remove client " + CurrentClient.IpPort());
                        return false;
                    }
                }

                //
                // remove client GUID map entry
                //
                if (!String.IsNullOrEmpty(CurrentClient.ClientGuid))
                {
                    string removedMap = null;
                    if (ClientGuidMap.TryRemove(CurrentClient.ClientGuid, out removedMap))
                    {
                        Log("RemoveClient removed client GUID map for GUID " + CurrentClient.ClientGuid + " and tuple " + CurrentClient.IpPort());
                    }
                }
                
                Log("RemoveClient exiting with " + Clients.Count + " client entries and " + ClientGuidMap.Count + " GUID map entries");
                if (ClientDisconnected != null) Task.Run(() => ClientDisconnected(CurrentClient));
                return true;
            }
            finally
            {
                sw.Stop();
                if (LogLockMethodResponseTime) Log("RemoveClient " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveClientChannels(BigQClient CurrentClient)
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

                foreach (KeyValuePair<string, BigQChannel> curr in Channels)
                {
                    if (String.Compare(curr.Value.OwnerGuid, CurrentClient.ClientGuid) != 0)
                    {
                        #region Match

                        if (curr.Value.Subscribers != null)
                        {
                            if (curr.Value.Subscribers.Count > 0)
                            {
                                //
                                // create another reference in case list is modified
                                //
                                BigQChannel TempChannel = curr.Value;
                                List<BigQClient> TempSubscribers = new List<BigQClient>(curr.Value.Subscribers);

                                Task.Run(() =>
                                {
                                    foreach (BigQClient Client in TempSubscribers)
                                    {
                                        if (String.Compare(Client.ClientGuid, TempChannel.OwnerGuid) != 0)
                                        {
                                            Log("RemoveClientChannels notifying channel " + TempChannel.Guid + " subscriber " + Client.ClientGuid + " of channel deletion");
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
                        BigQChannel removeChannel = null;
                        if (!Channels.TryRemove(curr, out removeChannel))
                        {
                            Log("*** RemoveClientChannels unable to remove client channel " + curr + " for client " + CurrentClient.ClientGuid);
                        }
                    }
                }

                return true;
            }
            finally
            {
                sw.Stop();
                if (LogLockMethodResponseTime) Log("RemoveClientChannels " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool UpdateClient(BigQClient CurrentClient)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (CurrentClient == null)
                {
                    Log("*** UpdateClient null client supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(CurrentClient.ClientGuid))
                {
                    Log("UpdateClient " + CurrentClient.IpPort() + " cannot update without a client GUID (login required)");
                    return false;
                }

                Log("UpdateClient " + CurrentClient.IpPort() + " entering with " + Clients.Count + " entries in client list");

                //
                // update Clients dictionary
                //
                BigQClient existingClient = null;
                BigQClient removedClient = null;

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

                //
                // update ClientGuidMap dictionary
                //
                string existingMap = null;
                if (ClientGuidMap.TryGetValue(CurrentClient.ClientGuid, out existingMap))
                {
                    if (String.IsNullOrEmpty(existingMap))
                    {
                        #region New-Entry

                        if (!ClientGuidMap.TryAdd(CurrentClient.ClientGuid, CurrentClient.IpPort()))
                        {
                            Log("*** UpdateClient unable to add GUID map for client GUID " + CurrentClient.ClientGuid + " for tuple " + CurrentClient.IpPort());
                            return false;
                        }
                        
                        #endregion
                    }
                    else
                    {
                        #region Existing-Entry

                        string deletedMap = null;
                        if (!ClientGuidMap.TryRemove(CurrentClient.ClientGuid, out deletedMap))
                        {
                            Log("*** UpdateClient unable to remove client GUID map for GUID " + CurrentClient.ClientGuid + " for replacement");
                            return false;
                        }
                        
                        if (!ClientGuidMap.TryAdd(CurrentClient.ClientGuid, CurrentClient.IpPort()))
                        {
                            Log("*** UpdateClient unable to add GUID map for client GUID " + CurrentClient.ClientGuid + " for tuple " + CurrentClient.IpPort());
                            return false;
                        }
                        
                        #endregion
                    }
                }
                else
                {
                    #region New-Entry

                    if (!ClientGuidMap.TryAdd(CurrentClient.ClientGuid, CurrentClient.IpPort()))
                    {
                        Log("*** UpdateClient unable to add GUID map for client GUID " + CurrentClient.ClientGuid + " for tuple " + CurrentClient.IpPort());
                        return false;
                    }
                    
                    #endregion
                }

                Log("UpdateClient " + CurrentClient.IpPort() + " exiting with " + Clients.Count + " entries in client list");
                return true;
            }
            finally
            {
                sw.Stop();
                if (LogLockMethodResponseTime) Log("UpdateClient " + CurrentClient.IpPort() + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool AddChannel(BigQClient CurrentClient, BigQChannel CurrentChannel)
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
                    Log("*** AddChannel null channel GUID supplied");
                    return false;
                }

                BigQChannel existingChannel = null;
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
                CurrentChannel.Subscribers = new List<BigQClient>();
                CurrentChannel.Subscribers.Add(CurrentClient);
                CurrentChannel.OwnerGuid = CurrentClient.ClientGuid;

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
                if (LogLockMethodResponseTime) Log("AddChannel " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveChannel(BigQChannel CurrentChannel)
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

                BigQChannel removeChannel = null;
                if (Channels.TryRemove(CurrentChannel.Guid, out removeChannel))
                {
                    if (removeChannel != null)
                    {
                        #region Match

                        Log("RemoveChannel notifying channel members of channel removal");

                        if (removeChannel.Subscribers != null)
                        {
                            if (removeChannel.Subscribers.Count > 0)
                            {
                                //
                                // create another reference in case list is modified
                                //
                                BigQChannel TempChannel = removeChannel;
                                List<BigQClient> TempSubscribers = new List<BigQClient>(removeChannel.Subscribers);

                                Task.Run(() =>
                                {
                                    foreach (BigQClient Client in TempSubscribers)
                                    {
                                        if (String.Compare(Client.ClientGuid, CurrentChannel.OwnerGuid) != 0)
                                        {
                                            Log("RemoveChannel notifying channel " + TempChannel.Guid + " subscriber " + Client.ClientGuid + " of channel deletion by owner");
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
                if (LogLockMethodResponseTime) Log("RemoveChannel " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool AddChannelSubscriber(BigQClient CurrentClient, BigQChannel CurrentChannel)
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

                BigQChannel existingChannel = null;

                if (Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    if (existingChannel != null)
                    {
                        #region Match

                        bool clientExists = false;
                        if (existingChannel.Subscribers != null && existingChannel.Subscribers.Count > 0)
                        {
                            foreach (BigQClient curr in existingChannel.Subscribers)
                            {
                                if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) == 0)
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
                            // notify existing clients
                            //
                            if (SendChannelJoinNotifications)
                            {
                                foreach (BigQClient curr in existingChannel.Subscribers)
                                {
                                    if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) != 0)
                                    {
                                        //
                                        // create another reference in case list is modified
                                        //
                                        BigQChannel TempChannel = existingChannel;
                                        Task.Run(() =>
                                        {
                                            Log("AddChannelSubscriber notifying channel " + TempChannel.Guid + " subscriber " + curr.ClientGuid + " of channel join by client " + CurrentClient.ClientGuid);
                                            SendSystemMessage(ChannelJoinEventMessage(TempChannel, CurrentClient));
                                        }
                                        );
                                    }
                                }
                            }
                        }

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
                if (LogLockMethodResponseTime) Log("AddChannelSubscriber " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool RemoveChannelSubscriber(BigQClient CurrentClient, BigQChannel CurrentChannel)
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

                BigQChannel existingChannel = null;
                if (!Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    Log("*** RemoveChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }
                else
                {
                    if (existingChannel != null)
                    {
                        #region Match

                        if (existingChannel.Subscribers == null || existingChannel.Subscribers.Count < 1)
                        {
                            Log("RemoveChannelSubscriber channel " + CurrentChannel.Guid + " has no subscribers, removing channel");
                            return RemoveChannel(CurrentChannel);
                        }

                        List<BigQClient> updatedSubscribers = new List<BigQClient>();
                        foreach (BigQClient curr in existingChannel.Subscribers)
                        {
                            if (String.Compare(CurrentClient.ClientGuid, curr.ClientGuid) != 0)
                            {
                                updatedSubscribers.Add(curr);
                            }
                        }

                        existingChannel.Subscribers = updatedSubscribers;

                        BigQChannel removedChannel = null;
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

                        if (SendChannelJoinNotifications)
                        {
                            foreach (BigQClient curr in existingChannel.Subscribers)
                            {
                                if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) != 0)
                                {
                                    //
                                    // create another reference in case list is modified
                                    //
                                    BigQChannel TempChannel = existingChannel;
                                    Task.Run(() =>
                                    {
                                        Log("RemoveChannelSubscriber notifying channel " + TempChannel.Guid + " subscriber " + curr.ClientGuid + " of channel leave by client " + CurrentClient.ClientGuid);
                                        SendSystemMessage(ChannelLeaveEventMessage(TempChannel, CurrentClient));
                                    }
                                    );
                                }
                            }
                        }

                        Log("RemoveChannelSubscriber successfully replaced channel subscriber list for channel GUID " + CurrentChannel.Guid);
                        return true;

                        #endregion
                    }
                    else
                    {
                        Log("*** RemoveChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                        return false;
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (LogLockMethodResponseTime) Log("RemoveChannelSubscriber " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool IsChannelSubscriber(BigQClient CurrentClient, BigQChannel CurrentChannel)
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

                BigQChannel existingChannel = null;
                if (!Channels.TryGetValue(CurrentChannel.Guid, out existingChannel))
                {
                    Log("*** IsChannelSubscriber channel with GUID " + CurrentChannel.Guid + " not found");
                    return false;
                }
                else
                {
                    if (existingChannel != null)
                    {
                        #region Match

                        foreach (BigQClient curr in existingChannel.Subscribers)
                        {
                            if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) == 0)
                            {
                                Log("IsChannelSubscriber client GUID " + CurrentClient.ClientGuid + " is a member of channel GUID " + CurrentChannel.Guid);
                                return true;
                            }
                        }

                        Log("*** IsChannelSubscriber client GUID " + CurrentClient.ClientGuid + " is not a member of channel GUID " + CurrentChannel.Guid);
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
                if (LogLockMethodResponseTime) Log("IsChannelSubscriber " + sw.Elapsed.TotalMilliseconds + "ms");
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
                if (ClientGuidMap.TryGetValue(guid, out ipPort))
                {
                    BigQClient existingClient = null;
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
                if (LogLockMethodResponseTime) Log("IsClientConnected " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion

        #region Private-Message-Processing-Methods

        private BigQMessage RedactMessage(BigQMessage msg)
        {
            if (msg == null) return null;
            msg.Email = null;
            msg.Password = null;
            return msg;
        }

        private BigQChannel BuildChannelFromMessageData(BigQClient CurrentClient, BigQMessage CurrentMessage)
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

            BigQChannel ret = null;
            try
            {
                ret = BigQHelper.DeserializeJson<BigQChannel>(CurrentMessage.Data, false);
            }
            catch (Exception e)
            {
                LogException("BuildChannelFromMessageData", e);
                ret = null;
            }

            if (ret == null)
            {
                Log("*** BuildChannelFromMessageData unable to convert message body to BigQChannel object");
                return null;
            }

            // assume ret.Private is set in the request
            if (ret.Private == default(int)) ret.Private = 0;

            if (String.IsNullOrEmpty(ret.Guid)) ret.Guid = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(ret.ChannelName)) ret.ChannelName = ret.Guid;
            ret.CreatedUTC = DateTime.Now.ToUniversalTime();
            ret.UpdatedUTC = ret.CreatedUTC;
            ret.OwnerGuid = CurrentClient.ClientGuid;
            ret.Subscribers = new List<BigQClient>();
            ret.Subscribers.Add(CurrentClient);
            return ret;
        }

        private bool MessageProcessor(BigQClient CurrentClient, BigQMessage CurrentMessage)
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

                #region Variables

                BigQClient CurrentRecipient = null;
                BigQChannel CurrentChannel = null;
                BigQMessage ResponseMessage = new BigQMessage();
                bool ResponseSuccess = false;

                #endregion

                #region Preset-Values

                CurrentMessage.Success = null;

                #endregion

                #region Verify-Client-GUID-Present

                if (String.IsNullOrEmpty(CurrentClient.ClientGuid))
                {
                    if (!String.IsNullOrEmpty(CurrentMessage.Command))
                    {
                        if (String.Compare(CurrentMessage.Command.ToLower(), "login") != 0)
                        {
                            #region Null-GUID-and-Not-Login

                            Log("*** MessageProcessor received message from client with no GUID");
                            ResponseSuccess = DataSender(CurrentClient, LoginRequiredMessage());
                            return ResponseSuccess;

                            #endregion
                        }
                    }
                }
                else
                {
                    #region Ensure-GUID-Exists

                    if (String.Compare(CurrentClient.ClientGuid, "00000000-0000-0000-0000-000000000000") != 0)
                    {
                        // all zeros is the server
                        BigQClient VerifyClient = GetClientByGuid(CurrentClient.ClientGuid);
                        if (VerifyClient == null)
                        {
                            Log("*** MessageProcessor received message from unknown client GUID " + CurrentClient.ClientGuid + " from " + CurrentClient.IpPort());
                            ResponseSuccess = DataSender(CurrentClient, LoginRequiredMessage());
                            return ResponseSuccess;
                        }
                    }

                    #endregion
                }

                #endregion

                #region Verify-Transport-Objects-Present

                if (String.Compare(CurrentClient.ClientGuid, "00000000-0000-0000-0000-000000000000") != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (CurrentClient.IsTCP)
                    {
                        if (CurrentClient.ClientTCPInterface == null)
                        {
                            Log("*** MessageProcessor null TCP client within supplied TCP client");
                            return false;
                        }
                    }

                    if (CurrentClient.IsWebsocket)
                    {
                        if (CurrentClient.ClientHTTPContext == null)
                        {
                            Log("*** MessageProcessor null HTTP context within supplied websocket client");
                            return false;
                        }

                        if (CurrentClient.ClientWSContext == null)
                        {
                            Log("*** MessageProcessor null websocket context witin supplied websocket client");
                            return false;
                        }

                        if (CurrentClient.ClientWSInterface == null)
                        {
                            Log("*** MessageProcessor null websocket object within supplied websocket client");
                            return false;
                        }
                    }
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
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "login":
                            ResponseMessage = ProcessLoginMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "heartbeatrequest":
                            // no need to send response
                            return true;

                        case "joinchannel":
                            ResponseMessage = ProcessJoinChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "leavechannel":
                            ResponseMessage = ProcessLeaveChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "createchannel":
                            ResponseMessage = ProcessCreateChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "deletechannel":
                            ResponseMessage = ProcessDeleteChannelMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "listchannels":
                            ResponseMessage = ProcessListChannelsMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "listchannelsubscribers":
                            ResponseMessage = ProcessListChannelSubscribersMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "listclients":
                            ResponseMessage = ProcessListClientsMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        case "isclientconnected":
                            ResponseMessage = ProcessIsClientConnectedMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;

                        default:
                            ResponseMessage = UnknownCommandMessage(CurrentClient, CurrentMessage);
                            ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                            return ResponseSuccess;
                    }
                }

                #endregion

                #region Get-Recipient-or-Channel

                if (!String.IsNullOrEmpty(CurrentMessage.RecipientGuid))
                {
                    CurrentRecipient = GetClientByGuid(CurrentMessage.RecipientGuid);
                }
                else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGuid))
                {
                    CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGuid);
                }
                else
                {
                    #region Recipient-Not-Supplied

                    Log("MessageProcessor no recipient specified either by RecipientGuid or ChannelGuid");
                    ResponseMessage = RecipientNotFoundMessage(CurrentClient, CurrentMessage);
                    ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                    return false;

                    #endregion
                }

                #endregion

                #region Process-Recipient-Messages

                if (CurrentRecipient != null)
                {
                    #region Send-to-Recipient

                    ResponseSuccess = SendPrivateMessage(CurrentClient, CurrentRecipient, CurrentMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** MessageProcessor unable to send to recipient " + CurrentRecipient.ClientGuid + ", sent failure notification to sender");
                    }

                    return ResponseSuccess;

                    #endregion
                }
                else if (CurrentChannel != null)
                {
                    #region Send-to-Channel

                    ResponseSuccess = SendChannelMessage(CurrentClient, CurrentChannel, CurrentMessage);
                    if (!ResponseSuccess)
                    {
                        Log("*** MessageProcessor unable to send to channel " + CurrentChannel.Guid + ", sent failure notification to sender");
                    }

                    return ResponseSuccess;

                    #endregion
                }
                else
                {
                    #region Recipient-Not-Found

                    Log("MessageProcessor unable to find either recipient or channel");
                    ResponseMessage = RecipientNotFoundMessage(CurrentClient, CurrentMessage);
                    ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                    return false;

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("MessageProcessor " + CurrentMessage.Command + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendPrivateMessage(BigQClient Sender, BigQClient Recipient, BigQMessage CurrentMessage)
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

                if (String.Compare(Sender.ClientGuid, "00000000-0000-0000-0000-000000000000") != 0)
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
                BigQMessage ResponseMessage = new BigQMessage();

                #endregion

                #region Send-to-Recipient

                ResponseSuccess = DataSender(Recipient, RedactMessage(CurrentMessage));

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
                        if (SendAcknowledgements)
                        {
                            ResponseMessage = MessageSendSuccess(Sender, CurrentMessage);
                            ResponseSuccess = DataSender(Sender, ResponseMessage);
                        }
                        return true;
                    }
                    else
                    {
                        ResponseMessage = MessageSendFailure(Sender, CurrentMessage);
                        ResponseSuccess = DataSender(Sender, ResponseMessage);
                        return false;
                    }

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendPrivateMessage " + CurrentMessage.SenderGuid + " -> " + CurrentMessage.RecipientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendChannelMessage(BigQClient Sender, BigQChannel CurrentChannel, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Sender == null)
                {
                    Log("*** SendChannelMessage null Sender supplied");
                    return false;
                }

                if (String.Compare(Sender.ClientGuid, "00000000-0000-0000-0000-000000000000") != 0)
                {
                    //
                    // all zeros is the server
                    //
                    if (Sender.IsTCP)
                    {
                        if (Sender.ClientTCPInterface == null)
                        {
                            Log("*** SendChannelMessage null TCP client within supplied Sender");
                            return false;
                        }
                    }

                    if (Sender.IsWebsocket)
                    {
                        if (Sender.ClientHTTPContext == null)
                        {
                            Log("*** SendChannelMessage null HTTP context within supplied Sender");
                            return false;
                        }


                        if (Sender.ClientWSContext == null)
                        {
                            Log("*** SendChannelMessage null websocket context within supplied Sender");
                            return false;
                        }

                        if (Sender.ClientWSInterface == null)
                        {
                            Log("*** SendChannelMessage null websocket object within supplied Sender");
                            return false;
                        }
                    }
                }

                if (CurrentChannel == null)
                {
                    Log("*** SendChannelMessage null channel supplied");
                    return false;
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendChannelMessage null message supplied");
                    return false;
                }

                #endregion

                #region Variables

                bool ResponseSuccess = false;
                BigQMessage ResponseMessage = new BigQMessage();

                #endregion

                #region Verify-Channel-Membership

                if (!IsChannelSubscriber(Sender, CurrentChannel))
                {
                    ResponseMessage = NotChannelMemberMessage(Sender, CurrentMessage, CurrentChannel);
                    ResponseSuccess = DataSender(Sender, ResponseMessage);
                    return false;
                }

                #endregion

                #region Send-to-Channel-and-Return-Success

                Task.Run(() =>
                {
                    ResponseSuccess = ChannelDataSender(Sender, CurrentChannel, RedactMessage(CurrentMessage));
                });

                if (SendAcknowledgements)
                {
                    ResponseMessage = MessageSendSuccess(Sender, CurrentMessage);
                    ResponseSuccess = DataSender(Sender, ResponseMessage);
                }
                return true;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendChannelMessage " + CurrentMessage.SenderGuid + " -> " + CurrentMessage.ChannelGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemMessage(BigQMessage CurrentMessage)
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

                BigQClient CurrentClient = new BigQClient();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGuid = "00000000-0000-0000-0000-000000000000";

                if (!String.IsNullOrEmpty(TCPListenerIP)) CurrentClient.SourceIp = TCPListenerIP;
                else CurrentClient.SourceIp = "127.0.0.1";

                CurrentClient.SourcePort = TCPListenerPort;
                CurrentClient.ServerIp = CurrentClient.SourceIp;
                CurrentClient.ServerPort = CurrentClient.SourcePort;
                CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUTC = CurrentClient.CreatedUTC;

                #endregion

                #region Variables

                BigQClient CurrentRecipient = new BigQClient();
                BigQChannel CurrentChannel = new BigQChannel();
                BigQMessage ResponseMessage = new BigQMessage();
                bool ResponseSuccess = false;

                #endregion

                #region Get-Recipient-or-Channel

                if (!String.IsNullOrEmpty(CurrentMessage.RecipientGuid))
                {
                    CurrentRecipient = GetClientByGuid(CurrentMessage.RecipientGuid);
                }
                else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGuid))
                {
                    CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGuid);
                }
                else
                {
                    #region Recipient-Not-Supplied

                    Log("SendSystemMessage no recipient specified either by RecipientGuid or ChannelGuid");
                    return false;

                    #endregion
                }

                #endregion

                #region Process-Recipient-Messages

                if (CurrentRecipient != null)
                {
                    #region Send-to-Recipient

                    ResponseSuccess = DataSender(CurrentRecipient, RedactMessage(CurrentMessage));
                    if (ResponseSuccess)
                    {
                        Log("SendSystemMessage successfully sent message to recipient " + CurrentRecipient.ClientGuid);
                        return true;
                    }
                    else
                    {
                        Log("*** SendSystemMessage unable to send message to recipient " + CurrentRecipient.ClientGuid);
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
                    ResponseSuccess = DataSender(CurrentClient, ResponseMessage);
                    return false;

                    #endregion
                }

                #endregion
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendSystemMessage " + CurrentMessage.SenderGuid + " -> " + CurrentMessage.RecipientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemPrivateMessage(BigQClient Recipient, BigQMessage CurrentMessage)
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

                BigQClient CurrentClient = new BigQClient();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGuid = "00000000-0000-0000-0000-000000000000";

                if (!String.IsNullOrEmpty(TCPListenerIP)) CurrentClient.SourceIp = TCPListenerIP;
                else CurrentClient.SourceIp = "127.0.0.1";

                CurrentClient.SourcePort = TCPListenerPort;
                CurrentClient.ServerIp = CurrentClient.SourceIp;
                CurrentClient.ServerPort = CurrentClient.SourcePort;
                CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUTC = CurrentClient.CreatedUTC;

                #endregion

                #region Variables

                BigQChannel CurrentChannel = new BigQChannel();
                bool ResponseSuccess = false;

                #endregion

                #region Process-Recipient-Messages

                ResponseSuccess = DataSender(Recipient, RedactMessage(CurrentMessage));
                return ResponseSuccess;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendSystemPrivateMessage " + CurrentMessage.SenderGuid + " -> " + CurrentMessage.RecipientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private bool SendSystemChannelMessage(BigQChannel Channel, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Channel == null)
                {
                    Log("*** SendSystemChannelMessage null channel supplied");
                    return false;
                }

                if (Channel.Subscribers == null || Channel.Subscribers.Count < 1)
                {
                    Log("SendSystemChannelMessage no subscribers in channel " + Channel.Guid);
                    return true;
                }

                if (CurrentMessage == null)
                {
                    Log("*** SendSystemPrivateMessage null message supplied");
                    return false;
                }

                #endregion

                #region Create-System-Client-Object

                BigQClient CurrentClient = new BigQClient();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGuid = "00000000-0000-0000-0000-000000000000";

                if (!String.IsNullOrEmpty(TCPListenerIP)) CurrentClient.SourceIp = TCPListenerIP;
                else CurrentClient.SourceIp = "127.0.0.1";

                CurrentClient.SourcePort = TCPListenerPort;
                CurrentClient.ServerIp = CurrentClient.SourceIp;
                CurrentClient.ServerPort = CurrentClient.SourcePort;
                CurrentClient.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUTC = CurrentClient.CreatedUTC;

                #endregion

                #region Variables

                bool ResponseSuccess = false;

                #endregion

                #region Process-Recipient-Messages

                ResponseSuccess = ChannelDataSender(CurrentClient, Channel, CurrentMessage);
                return ResponseSuccess;

                #endregion
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendSystemChannelMessage " + CurrentMessage.SenderGuid + " -> " + CurrentMessage.ChannelGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion

        #region Private-Message-Handlers

        private BigQMessage ProcessEchoMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
                CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.Success = true;
                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessEchoMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessLoginMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            bool runClientLoginTask = false;
            bool runSendServerJoinNotifications = false;

            try
            {
                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
                CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentClient.ClientGuid = CurrentMessage.RecipientGuid;
                CurrentClient.Email = CurrentMessage.Email;
                if (String.IsNullOrEmpty(CurrentClient.Email)) CurrentClient.Email = CurrentClient.ClientGuid;

                if (!UpdateClient(CurrentClient))
                {
                    CurrentMessage.Success = false;
                    CurrentMessage.Data = Encoding.UTF8.GetBytes("Unable to update client details");
                }
                else
                {
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = Encoding.UTF8.GetBytes("Login successful");
                    runClientLoginTask = true;
                    runSendServerJoinNotifications = true;
                }

                return CurrentMessage;
            }
            finally
            {
                if (LogMessageResponseTime) Log("ProcessLoginMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.ElapsedMilliseconds + "ms (before tasks)");

                if (runClientLoginTask)
                {
                    if (ClientLogin != null)
                    {
                        Task.Run(() => ClientLogin(CurrentClient));
                    }
                }

                if (runSendServerJoinNotifications)
                {
                    if (SendServerJoinNotifications)
                    {
                        Task.Run(() => ServerJoinEvent(CurrentClient));
                    }
                }

                if (LogMessageResponseTime) Log("ProcessLoginMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.ElapsedMilliseconds + "ms (after tasks)");
            }
        }

        private BigQMessage ProcessIsClientConnectedMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
                CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();

                if (CurrentMessage.Data == null)
                {
                    CurrentMessage.Success = false;
                    CurrentMessage.Data = null;
                }
                else
                {
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = Encoding.UTF8.GetBytes(IsClientConnected(CurrentMessage.Data.ToString()).ToString());
                }

                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessIsClientConnectedMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessJoinChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                BigQChannel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGuid);

                if (CurrentChannel == null)
                {
                    Log("*** ProcessJoinChannelMessage unable to find channel " + CurrentChannel.Guid);
                    BigQMessage ResponseMessage = new BigQMessage();
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }
                else
                {
                    Log("ProcessJoinChannelMessage adding client " + CurrentClient.IpPort() + " to channel " + CurrentChannel.Guid);
                    if (!AddChannelSubscriber(CurrentClient, CurrentChannel))
                    {
                        Log("*** ProcessJoinChannelMessage error while adding channel member " + CurrentClient.IpPort() + " to channel " + CurrentChannel.Guid);
                        BigQMessage ResponseMessage = ChannelJoinFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        return ResponseMessage;
                    }
                    else
                    {
                        if (SendChannelJoinNotifications) ChannelJoinEvent(CurrentClient, CurrentChannel);
                        BigQMessage ResponseMessage = ChannelJoinSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        return ResponseMessage;
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessJoinChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessLeaveChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                BigQChannel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGuid);
                BigQMessage ResponseMessage = new BigQMessage();

                if (CurrentChannel == null)
                {
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }
                else
                {
                    if (String.Compare(CurrentClient.ClientGuid, CurrentChannel.OwnerGuid) == 0)
                    {
                        #region Owner-Abandoning-Channel

                        if (!RemoveChannel(CurrentChannel))
                        {
                            Log("*** ProcessLeaveChannelMessage unable to remove owner " + CurrentClient.IpPort() + " from channel " + CurrentMessage.ChannelGuid);
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
                        #region Subscriber-Leaving-Channel

                        if (!RemoveChannelSubscriber(CurrentClient, CurrentChannel))
                        {
                            Log("*** ProcessLeaveChannelMessage unable to remove client " + CurrentClient.IpPort() + " from channel " + CurrentMessage.ChannelGuid);
                            return ChannelLeaveFailureMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        }
                        else
                        {
                            if (SendChannelJoinNotifications) ChannelLeaveEvent(CurrentClient, CurrentChannel);
                            return ChannelLeaveSuccessMessage(CurrentClient, CurrentMessage, CurrentChannel);
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessLeaveChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessCreateChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                BigQChannel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGuid);
                BigQMessage ResponseMessage = new BigQMessage();

                if (CurrentChannel == null)
                {
                    BigQChannel RequestChannel = BuildChannelFromMessageData(CurrentClient, CurrentMessage);
                    if (RequestChannel == null)
                    {
                        Log("*** ProcessCreateChannelMessage unable to build BigQChannel from BigQMessage data");
                        ResponseMessage = DataErrorMessage(CurrentClient, CurrentMessage, "unable to create BigQChannel from supplied message data");
                        return ResponseMessage;
                    }
                    else
                    {
                        CurrentChannel = GetChannelByName(RequestChannel.ChannelName);
                        if (CurrentChannel != null)
                        {
                            ResponseMessage = ChannelAlreadyExistsMessage(CurrentClient, CurrentMessage);
                            return ResponseMessage;
                        }
                        else
                        {
                            if (String.IsNullOrEmpty(RequestChannel.Guid))
                            {
                                RequestChannel.Guid = Guid.NewGuid().ToString();
                                Log("ProcessCreateChannelMessage adding GUID " + RequestChannel.Guid + " to request (not supplied by requestor)");
                            }

                            RequestChannel.OwnerGuid = CurrentClient.ClientGuid;

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
                    ResponseMessage = ChannelAlreadyExistsMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessCreateChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessDeleteChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                BigQChannel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGuid);
                BigQMessage ResponseMessage = new BigQMessage();

                if (CurrentChannel == null)
                {
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }

                if (String.Compare(CurrentChannel.OwnerGuid, CurrentClient.ClientGuid) != 0)
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
                if (LogMessageResponseTime) Log("ProcessDeleteChannelMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessListChannelsMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                List<BigQChannel> ret = new List<BigQChannel>();
                List<BigQChannel> filtered = new List<BigQChannel>();
                BigQChannel CurrentChannel = new BigQChannel();

                ret = GetAllChannels();
                if (ret == null || ret.Count < 1)
                {
                    Log("*** ProcessListChannelsMessage no channels retrieved");

                    CurrentMessage = RedactMessage(CurrentMessage);
                    CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                    CurrentMessage.SyncRequest = null;
                    CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
                    CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                    CurrentMessage.ChannelGuid = null;
                    CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = null;
                    return CurrentMessage;
                }
                else
                {
                    foreach (BigQChannel curr in ret)
                    {
                        CurrentChannel.Subscribers = null;
                        CurrentChannel.Guid = curr.Guid;
                        CurrentChannel.ChannelName = curr.ChannelName;
                        CurrentChannel.OwnerGuid = curr.OwnerGuid;
                        CurrentChannel.CreatedUTC = curr.CreatedUTC;
                        CurrentChannel.UpdatedUTC = curr.UpdatedUTC;
                        CurrentChannel.Private = curr.Private;

                        if (String.Compare(CurrentChannel.OwnerGuid, CurrentClient.ClientGuid) == 0)
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
                }

                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
                CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                CurrentMessage.ChannelGuid = null;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.Success = true;
                CurrentMessage.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(filtered));
                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessListChannelsMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessListChannelSubscribersMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                BigQChannel CurrentChannel = GetChannelByGuid(CurrentMessage.ChannelGuid);
                BigQMessage ResponseMessage = new BigQMessage();
                List<BigQClient> Clients = new List<BigQClient>();
                List<BigQClient> ret = new List<BigQClient>();

                if (CurrentChannel == null)
                {
                    Log("*** ProcessListChannelSubscribersMessage null channel after retrieval by GUID");
                    ResponseMessage = ChannelNotFoundMessage(CurrentClient, CurrentMessage);
                    return ResponseMessage;
                }

                Clients = GetChannelSubscribers(CurrentChannel.Guid);
                if (Clients == null)
                {
                    Log("ProcessListChannelSubscribersMessage channel " + CurrentChannel.Guid + " is empty");
                    ResponseMessage = ChannelEmptyMessage(CurrentClient, CurrentMessage, CurrentChannel);
                    return ResponseMessage;
                }
                else
                {
                    foreach (BigQClient curr in Clients)
                    {
                        BigQClient temp = new BigQClient();
                        temp.Password = null;
                        temp.SourceIp = null;
                        temp.SourcePort = 0;

                        temp.ClientTCPInterface = null;
                        temp.ClientHTTPContext = null;
                        temp.ClientWSContext = null;
                        temp.ClientWSInterface = null;

                        temp.Email = curr.Email;
                        temp.ClientGuid = curr.ClientGuid;
                        temp.CreatedUTC = curr.CreatedUTC;
                        temp.UpdatedUTC = curr.UpdatedUTC;
                        ret.Add(temp);
                    }

                    CurrentMessage = RedactMessage(CurrentMessage);
                    CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                    CurrentMessage.SyncRequest = null;
                    CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
                    CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                    CurrentMessage.ChannelGuid = CurrentChannel.Guid;
                    CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                    CurrentMessage.Success = true;
                    CurrentMessage.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(ret));
                    return CurrentMessage;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessListChannelSubscribersMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        private BigQMessage ProcessListClientsMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                List<BigQClient> Clients = new List<BigQClient>();
                List<BigQClient> ret = new List<BigQClient>();

                Clients = GetAllClients();
                if (Clients == null || Clients.Count < 1)
                {
                    Log("*** ProcessListClientsMessage no clients retrieved");
                    return null;
                }
                else
                {
                    foreach (BigQClient curr in Clients)
                    {
                        BigQClient temp = new BigQClient();
                        temp.Password = null;
                        temp.SourceIp = null;
                        temp.SourcePort = 0;
                        temp.IsWebsocket = curr.IsWebsocket;
                        temp.IsTCP = curr.IsTCP;

                        temp.ClientTCPInterface = null;
                        temp.ClientHTTPContext = null;
                        temp.ClientWSContext = null;
                        temp.ClientWSInterface = null;

                        temp.Email = curr.Email;
                        temp.ClientGuid = curr.ClientGuid;
                        temp.CreatedUTC = curr.CreatedUTC;
                        temp.UpdatedUTC = curr.UpdatedUTC;
                        ret.Add(temp);
                    }
                }

                CurrentMessage = RedactMessage(CurrentMessage);
                CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
                CurrentMessage.SyncRequest = null;
                CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
                CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                CurrentMessage.ChannelGuid = null;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.Success = true;
                CurrentMessage.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(ret));
                return CurrentMessage;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ProcessListClientsMessage " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid + " " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        #endregion

        #region Private-Message-Builders

        private BigQMessage LoginRequiredMessage()
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;
            ResponseMessage.Data = Encoding.UTF8.GetBytes("Login required");
            return ResponseMessage;
        }

        private BigQMessage HeartbeatRequestMessage(BigQClient CurrentClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.MessageId = Guid.NewGuid().ToString();
            ResponseMessage.RecipientGuid = CurrentClient.ClientGuid; 
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Command = "HeartbeatRequest";
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Data = null;
            return ResponseMessage;
        }

        private BigQMessage UnknownCommandMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Unknown command '" + CurrentMessage.Command + "'");
            return CurrentMessage;
        }

        private BigQMessage RecipientNotFoundMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;

            if (!String.IsNullOrEmpty(CurrentMessage.RecipientGuid))
            {
                CurrentMessage.Data = Encoding.UTF8.GetBytes("Unknown recipient '" + CurrentMessage.RecipientGuid + "'");
            }
            else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGuid))
            {
                CurrentMessage.Data = Encoding.UTF8.GetBytes("Unknown channel '" + CurrentMessage.ChannelGuid + "'");
            }
            else
            {
                CurrentMessage.Data = Encoding.UTF8.GetBytes("No recipient or channel GUID supplied");
            }
            return CurrentMessage;
        }

        private BigQMessage NotChannelMemberMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("You are not a member of this channel");
            return CurrentMessage;
        }
        
        private BigQMessage MessageSendSuccess(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;

            if (!String.IsNullOrEmpty(CurrentMessage.RecipientGuid))
            {
                #region Individual-Recipient

                CurrentMessage.Data = Encoding.UTF8.GetBytes("Message delivered to recipient");
                return CurrentMessage;

                #endregion
            }
            else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGuid))
            {
                #region Channel-Recipient

                CurrentMessage.Data = Encoding.UTF8.GetBytes("Message queued for delivery to channel members");
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

        private BigQMessage MessageSendFailure(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Unable to send message");
            return CurrentMessage;
        }

        private BigQMessage ChannelNotFoundMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Channel not found");
            return CurrentMessage;
        }

        private BigQMessage ChannelEmptyMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Channel is empty");
            return CurrentMessage;
        }

        private BigQMessage ChannelAlreadyExistsMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Channel already exists");
            return CurrentMessage;
        }

        private BigQMessage ChannelCreateSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Channel created successfully");
            return CurrentMessage;
        }

        private BigQMessage ChannelCreateFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Unable to create channel");
            return CurrentMessage;
        }

        private BigQMessage ChannelJoinSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Successfully joined channel");
            return CurrentMessage;
        }

        private BigQMessage ChannelLeaveSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Successfully left channel");
            return CurrentMessage;
        }

        private BigQMessage ChannelLeaveFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Unable to leave channel due to error");
            return CurrentMessage;
        }

        private BigQMessage ChannelJoinFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Failed to join channel");
            return CurrentMessage;
        }

        private BigQMessage ChannelDeletedByOwnerMessage(BigQClient CurrentClient, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = CurrentClient.ClientGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = Encoding.UTF8.GetBytes("Channel deleted by owner");
            return ResponseMessage;
        }

        private BigQMessage ChannelDeleteSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = true;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Successfully deleted channel");
            return CurrentMessage;
        }

        private BigQMessage ChannelDeleteFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.ChannelGuid = CurrentChannel.Guid;
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Unable to delete channel");
            return CurrentMessage;
        }

        private BigQMessage DataErrorMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, string message)
        {
            CurrentMessage = RedactMessage(CurrentMessage);
            CurrentMessage.RecipientGuid = CurrentMessage.SenderGuid;
            CurrentMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            CurrentMessage.Success = false;
            CurrentMessage.SyncResponse = CurrentMessage.SyncRequest;
            CurrentMessage.SyncRequest = null;
            CurrentMessage.Data = Encoding.UTF8.GetBytes("Data error encountered in your message: " + message);
            return CurrentMessage;
        }

        private BigQMessage ServerJoinEventMessage(BigQClient NewClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientJoinedServer";
            ResponseEvent.Data = NewClient.ClientGuid;

            ResponseMessage.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(ResponseEvent));
            return ResponseMessage;
        }

        private BigQMessage ServerLeaveEventMessage(BigQClient LeavingClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientLeftServer";
            ResponseEvent.Data = LeavingClient.ClientGuid;

            ResponseMessage.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(ResponseEvent));
            return ResponseMessage;
        }

        private BigQMessage ChannelJoinEventMessage(BigQChannel CurrentChannel, BigQClient NewClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientJoinedChannel";
            ResponseEvent.Data = NewClient.ClientGuid;

            ResponseMessage.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(ResponseEvent));
            return ResponseMessage;
        }

        private BigQMessage ChannelLeaveEventMessage(BigQChannel CurrentChannel, BigQClient LeavingClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientLeftChannel";
            ResponseEvent.Data = LeavingClient.ClientGuid;

            ResponseMessage.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(ResponseEvent));
            return ResponseMessage;
        }

        #endregion

        #region Private-Logging-Methods

        private void Log(string message)
        {
            if (LogMessage != null) LogMessage(message);
            if (ConsoleDebug)
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

        #endregion
    }
}
