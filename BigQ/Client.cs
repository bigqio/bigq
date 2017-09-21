using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
using System.Web.Script.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SyslogLogging;
using WatsonTcp;
using WatsonWebsocket;

namespace BigQ
{    
    /// <summary>
    /// BigQ client object.
    /// </summary>
    [Serializable]
    public class Client : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Contains configuration-related variables for the client.  
        /// </summary>
        public ClientConfiguration Config;

        /// <summary>
        /// The email address associated with the client.  Do not modify directly; used by the server.  
        /// </summary>
        public string Email;

        /// <summary>
        /// The password associated with the client.  Do not modify directly; used by the server.  
        /// </summary>
        public string Password;

        /// <summary>
        /// The GUID associated with the client.  Do not modify directly; used by the server.  
        /// </summary>
        public string ClientGUID;

        /// <summary>
        /// The name associated with the client.
        /// </summary>
        public string Name;

        /// <summary>
        /// The GUID associated with the server.  
        /// </summary>
        public string ServerGuid;

        /// <summary>
        /// The client's source IP address and port (i.e. 10.1.1.1:5033).  Do not modify directly; used by the server.  
        /// </summary>
        public string IpPort;
         
        /// <summary>
        /// The UTC timestamp of when this client object was created.
        /// </summary>
        public DateTime CreatedUtc;

        /// <summary>
        /// The UTC timestamp of when this client object was last updated.
        /// </summary>
        public DateTime? UpdatedUtc;

        /// <summary>
        /// Indicates whether or not the client is using TCP sockets for messaging.  Do not modify this field.
        /// </summary>
        public bool IsTcp;

        /// <summary>
        /// Indicates whether or not the client is using websockets for messaging.  Do not modify this field.
        /// </summary>
        public bool IsWebsocket;

        /// <summary>
        /// Indicates whether or not the client is using SSL.  Do not modify this field.
        /// </summary>
        public bool IsSsl;
        
        /// <summary>
        /// Indicates whether or not the client is connected to the server.  Do not modify this field.
        /// </summary>
        public bool Connected;

        /// <summary>
        /// Indicates whether or not the client is logged in to the server.  Do not modify this field.
        /// </summary>
        public bool LoggedIn;

        /// <summary>
        /// A blocking collection containing the messages that are queued for delivery to this client.
        /// </summary>
        public BlockingCollection<Message> MessageQueue;

        /// <summary>
        /// Managed by the server to destroy the thread processing the client queue when the client is shutting down.
        /// </summary>
        public CancellationTokenSource ProcessClientQueueTokenSource = null;

        /// <summary>
        /// Managed by the server to destroy the thread processing the client queue when the client is shutting down.
        /// </summary>
        public CancellationToken ProcessClientQueueToken;

        /// <summary>
        /// Managed by the server to destroy the thread sending heartbeats to the client when the client is shutting down.
        /// </summary>
        public CancellationTokenSource HeartbeatTokenSource = null;

        /// <summary>
        /// Managed by the server to destroy the thread sending heartbeats to the client when the client is shutting down.
        /// </summary> 
        public CancellationToken HeartbeatToken;

        #endregion

        #region Private-Members

        private LoggingModule _Logging;
        private CancellationTokenSource _CleanupSyncTokenSource = null;
        private CancellationToken _CleanupSyncToken;
        private ConcurrentDictionary<string, DateTime> _SyncRequests;
        private ConcurrentDictionary<string, Message> _SyncResponses;

        private WatsonTcpClient _WTcpClient;
        private WatsonTcpSslClient _WTcpSslClient;
        private WatsonWsClient _WWsClient;
        private WatsonWsClient _WWsSslClient;

        private Random _Random;

        #endregion

        #region Public-Delegates

        /// <summary>
        /// Delegate method called when an asynchronous message is received.
        /// </summary>
        public Func<Message, bool> AsyncMessageReceived;
        
        /// <summary>
        /// Delegate method called when a synchronous message is received.
        /// </summary>
        public Func<Message, byte[]> SyncMessageReceived;

        /// <summary>
        /// Delegate method called when the server connection is severed.
        /// </summary>
        public Func<bool> ServerDisconnected;

        /// <summary>
        /// Delegate method called when the server connection is restored.
        /// </summary>
        public Func<bool> ServerConnected;

        /// <summary>
        /// Delegate method called when a client joins the server.
        /// </summary>
        public Func<string, bool> ClientJoinedServer;

        /// <summary>
        /// Delegate method called when a client leaves the server.
        /// </summary>
        public Func<string, bool> ClientLeftServer;

        /// <summary>
        /// Delegate method called when a client joins a channel.
        /// </summary>
        public Func<string, string, bool> ClientJoinedChannel;

        /// <summary>
        /// Delegate method called when a client leaves a channel.
        /// </summary>
        public Func<string, string, bool> ClientLeftChannel;

        /// <summary>
        /// Delegate method called when a subscriber joins a channel.
        /// </summary>
        public Func<string, string, bool> SubscriberJoinedChannel;

        /// <summary>
        /// Delegate method called when a subscriber leaves a channel.
        /// </summary>
        public Func<string, string, bool> SubscriberLeftChannel;

        /// <summary>
        /// Delegate method called when a public channel is created.
        /// </summary>
        public Func<string, bool> ChannelCreated;

        /// <summary>
        /// Delegate method called when a public channel is destroyed.
        /// </summary>
        public Func<string, bool> ChannelDestroyed;

        /// <summary>
        /// Delegate method called when the client desires to send a log message.
        /// </summary>
        public Func<string, bool> LogMessage;

        #endregion

        #region Constructors

        /// <summary>
        /// This constructor is used by BigQServer.  Do not use it in client applications!
        /// </summary>
        public Client()
        {
        }

        /// <summary>
        /// Start an instance of the BigQ client process.
        /// </summary>
        /// <param name="configFile">The full path and filename of the configuration file.  Leave null for a default configuration.</param>
        public Client(string configFile)
        {
            #region Load-Config

            CreatedUtc = DateTime.Now.ToUniversalTime();
            Config = null;
            _Random = new Random((int)DateTime.Now.Ticks);

            if (String.IsNullOrEmpty(configFile))
            {
                Config = ClientConfiguration.DefaultConfig();
            }
            else
            {
                Config = ClientConfiguration.LoadConfig(configFile);
            }

            if (Config == null) throw new Exception("Unable to initialize configuration.");
             
            Config.ValidateConfig();

            #endregion
             
            #region Initialize-Logging

            _Logging = new LoggingModule(
                Config.Logging.SyslogServerIp,
                Config.Logging.SyslogServerPort,
                Config.Logging.ConsoleLogging,
                (LoggingModule.Severity)Config.Logging.MinimumSeverity,
                false,
                true,
                true,
                true,
                true,
                true);

            #endregion

            #region Set-Class-Variables

            if (String.IsNullOrEmpty(Config.Email)) Config.Email = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.GUID)) Config.GUID = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.Name)) Config.Name = RandomName(); 
            if (String.IsNullOrEmpty(Config.Password)) Config.Password = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.ServerGUID)) Config.ServerGUID = "00000000-0000-0000-0000-000000000000";
             
            Email = Config.Email;
            Name = Config.Name;
            ClientGUID = Config.GUID;
            ServerGuid = Config.ServerGUID;
            Password = Config.Password; 

            _SyncRequests = new ConcurrentDictionary<string, DateTime>();
            _SyncResponses = new ConcurrentDictionary<string, Message>();
            Connected = false;
            LoggedIn = false;

            #endregion

            #region Set-Delegates-to-Null

            AsyncMessageReceived = null;
            SyncMessageReceived = null;
            ServerDisconnected = null;
            ClientJoinedServer = null;
            ClientLeftServer = null;
            ClientJoinedChannel = null;
            ClientLeftChannel = null;
            ChannelCreated = null;
            ChannelDestroyed = null;
            LogMessage = null;

            #endregion
             
            #region Start-Client

            if (Config.TcpServer.Enable)
            {
                #region Start-TCP-Client

                _WTcpClient = new WatsonTcpClient(
                    Config.TcpServer.Ip,
                    Config.TcpServer.Port,
                    WTcpServerConnected,
                    WTcpServerDisconnected,
                    WTcpMessageReceived,
                    Config.TcpServer.Debug);

                Connected = true;

                #endregion
            }
            else if (Config.TcpSslServer.Enable)
            {
                #region Start-TCP-SSL-Client

                _WTcpSslClient = new WatsonTcpSslClient(
                    Config.TcpSslServer.Ip,
                    Config.TcpSslServer.Port,
                    Config.TcpSslServer.PfxCertFile,
                    Config.TcpSslServer.PfxCertPassword,
                    Config.TcpSslServer.AcceptInvalidCerts,
                    false,
                    WTcpSslServerConnected,
                    WTcpSslServerDisconnected,
                    WTcpSslMessageReceived,
                    Config.TcpSslServer.Debug);

                Connected = true;

                #endregion
            }
            else if (Config.WebsocketServer.Enable)
            {
                #region Start-Websocket-Client

                _WWsClient = new WatsonWsClient(
                    Config.WebsocketServer.Ip,
                    Config.WebsocketServer.Port,
                    false,
                    false,
                    WWsServerConnected,
                    WWsServerDisconnected,
                    WWsMessageReceived,
                    Config.WebsocketServer.Debug);
                 
                Connected = true;

                #endregion
            }
            else if (Config.WebsocketSslServer.Enable)
            {
                #region Start-Websocket-SSL-Client

                _WWsSslClient = new WatsonWsClient(
                    Config.WebsocketSslServer.Ip,
                    Config.WebsocketSslServer.Port,
                    true,
                    Config.WebsocketSslServer.AcceptInvalidCerts,
                    WWsSslServerConnected,
                    WWsSslServerDisconnected,
                    WWsSslMessageReceived,
                    Config.WebsocketSslServer.Debug);
                 
                Connected = true;

                #endregion
            }
            else
            {
                #region Unknown-Server

                throw new ArgumentException("Exactly one server must be enabled in the configuration file.");

                #endregion
            }

            #endregion
             
            #region Start-Tasks
             
            _CleanupSyncTokenSource = new CancellationTokenSource();
            _CleanupSyncToken = _CleanupSyncTokenSource.Token;
            Task.Run(() => CleanupSyncRequests(), _CleanupSyncToken);
             
            // do not start heartbeat until successful login
            // design goal: server needs to free up connections fast
            // client just needs to keep socket open
            
            //
            // HeartbeatTokenSource = new CancellationTokenSource();
            // HeartbeatToken = HeartbeatTokenSource.Token;
            // Task.Run(() => TCPHeartbeatManager());

            #endregion
        }

        /// <summary>
        /// Start an instance of the BigQ client process.
        /// </summary>
        /// <param name="config">Populated client configuration object.</param>
        public Client(ClientConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            config.ValidateConfig();
            Config = config;
            _Random = new Random((int)DateTime.Now.Ticks);

            #region Initialize-Logging

            _Logging = new LoggingModule(
                Config.Logging.SyslogServerIp,
                Config.Logging.SyslogServerPort,
                Config.Logging.ConsoleLogging,
                (LoggingModule.Severity)Config.Logging.MinimumSeverity,
                false,
                true,
                true,
                true,
                true,
                true);

            #endregion

            #region Set-Class-Variables

            if (String.IsNullOrEmpty(Config.Email)) Config.Email = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.GUID)) Config.GUID = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.Name)) Config.Name = RandomName();
            if (String.IsNullOrEmpty(Config.Password)) Config.Password = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.ServerGUID)) Config.ServerGUID = "00000000-0000-0000-0000-000000000000";
             
            Email = Config.Email;
            Name = Config.Name;
            ClientGUID = Config.GUID;
            ServerGuid = Config.ServerGUID;
            Password = Config.Password;

            _SyncRequests = new ConcurrentDictionary<string, DateTime>();
            _SyncResponses = new ConcurrentDictionary<string, Message>();
            Connected = false;
            LoggedIn = false;

            #endregion

            #region Set-Delegates-to-Null

            AsyncMessageReceived = null;
            SyncMessageReceived = null;
            ServerDisconnected = null;
            ClientJoinedServer = null;
            ClientLeftServer = null;
            ClientJoinedChannel = null;
            ClientLeftChannel = null;
            ChannelCreated = null;
            ChannelDestroyed = null;
            LogMessage = null;

            #endregion

            #region Start-Client

            if (Config.TcpServer.Enable)
            {
                #region Start-TCP-Client

                _WTcpClient = new WatsonTcpClient(
                    Config.TcpServer.Ip,
                    Config.TcpServer.Port,
                    WTcpServerConnected,
                    WTcpServerDisconnected,
                    WTcpMessageReceived,
                    Config.TcpServer.Debug);

                Connected = true;

                #endregion
            }
            else if (Config.TcpSslServer.Enable)
            {
                #region Start-TCP-SSL-Client

                _WTcpSslClient = new WatsonTcpSslClient(
                    Config.TcpSslServer.Ip,
                    Config.TcpSslServer.Port,
                    Config.TcpSslServer.PfxCertFile,
                    Config.TcpSslServer.PfxCertPassword,
                    Config.TcpSslServer.AcceptInvalidCerts,
                    false,
                    WTcpSslServerConnected,
                    WTcpSslServerDisconnected,
                    WTcpSslMessageReceived,
                    Config.TcpSslServer.Debug);

                Connected = true;

                #endregion
            }
            else if (Config.WebsocketServer.Enable)
            {
                #region Start-Websocket-Client

                _WWsClient = new WatsonWsClient(
                    Config.WebsocketServer.Ip,
                    Config.WebsocketServer.Port,
                    false,
                    false,
                    WWsServerConnected,
                    WWsServerDisconnected,
                    WWsMessageReceived,
                    Config.WebsocketServer.Debug);

                Connected = true;

                #endregion
            }
            else if (Config.WebsocketSslServer.Enable)
            {
                #region Start-Websocket-SSL-Client

                _WWsSslClient = new WatsonWsClient(
                    Config.WebsocketSslServer.Ip,
                    Config.WebsocketSslServer.Port,
                    true,
                    Config.WebsocketSslServer.AcceptInvalidCerts,
                    WWsSslServerConnected,
                    WWsSslServerDisconnected,
                    WWsSslMessageReceived,
                    Config.WebsocketSslServer.Debug);

                Connected = true;

                #endregion
            }
            else
            {
                #region Unknown-Server

                throw new ArgumentException("Exactly one server must be enabled in the configuration file.");

                #endregion
            }

            #endregion

            #region Start-Tasks

            _CleanupSyncTokenSource = new CancellationTokenSource();
            _CleanupSyncToken = _CleanupSyncTokenSource.Token;
            Task.Run(() => CleanupSyncRequests(), _CleanupSyncToken);

            // do not start heartbeat until successful login
            // design goal: server needs to free up connections fast
            // client just needs to keep socket open

            //
            // HeartbeatTokenSource = new CancellationTokenSource();
            // HeartbeatToken = HeartbeatTokenSource.Token;
            // Task.Run(() => TCPHeartbeatManager());

            #endregion
        }

        /// <summary>
        /// Used by the server, do not use!
        /// </summary>
        /// <param name="ipPort">The IP:port of the client.</param>
        /// <param name="isTcp">Indicates if the connection is a TCP connection.</param>
        /// <param name="isWebsocket">Indicates if the connection is a websocket connection.</param>
        /// <param name="isSsl">Indicates if the connection uses SSL.</param>
        public Client(string ipPort, bool isTcp, bool isWebsocket, bool isSsl)
        {
            if (String.IsNullOrEmpty(ipPort)) throw new ArgumentNullException(nameof(ipPort));

            IpPort = ipPort;
            IsTcp = isTcp;
            IsWebsocket = isWebsocket;
            IsSsl = isSsl;

            MessageQueue = new BlockingCollection<Message>();

            DateTime ts = DateTime.Now.ToUniversalTime();
            CreatedUtc = ts;
            UpdatedUtc = ts;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down the client and dispose of background workers.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Sends a message; makes the assumption that you have populated the object fully and correctly.  In general, this method should not be used.
        /// </summary>
        /// <param name="message">The populated message object to send.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool SendMessage(Message message)
        { 
            if (message == null)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "SendRawMessage null message supplied");
                return false;
            }
                 
            byte[] data = message.ToBytes();

            if (Config.TcpServer.Enable)
            { 
                return _WTcpClient.SendAsync(data).Result;
            }
            else if (Config.TcpSslServer.Enable)
            { 
                return _WTcpSslClient.SendAsync(data).Result;
            }
            else if (Config.WebsocketServer.Enable)
            { 
                return _WWsClient.SendAsync(data).Result;
            }
            else if (Config.WebsocketSslServer.Enable)
            { 
                return _WWsSslClient.SendAsync(data).Result;
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendRawMessage no server enabled");
                return false;
            } 
        }

        /// <summary>
        /// Generates an echo request to the server, which should result in an asynchronous echo response.  Typically used to validate connectivity.
        /// </summary>
        /// <returns>Boolean indicating success.</returns>
        public bool Echo()
        {  
            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "Echo";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = null;
            request.Data = null;
            return SendMessage(request); 
        }

        /// <summary>
        /// Login to the server.
        /// </summary>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating if the login was successful.</returns>
        public bool Login(out Message response)
        { 
            response = null;
            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "Login";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = null;
            request.Data = null;
             
            if (!SendServerMessageSync(request, out response))
            { 
                _Logging.Log(LoggingModule.Severity.Warn, "Login unable to retrieve server response");
                return false;
            }
             
            if (response == null)
            { 
                _Logging.Log(LoggingModule.Severity.Warn, "Login null response from server");
                return false;
            }
             
            if (!Helper.IsTrue(response.Success))
            { 
                _Logging.Log(LoggingModule.Severity.Warn, "Login failed with response data " + response.Data.ToString());
                return false;
            }
            else
            { 
                LoggedIn = true;

                // stop existing heartbeat thread
                if (HeartbeatTokenSource != null) HeartbeatTokenSource.Cancel();

                // start new heartbeat thread
                HeartbeatTokenSource = new CancellationTokenSource();
                HeartbeatToken = HeartbeatTokenSource.Token;
                Task.Run(() => HeartbeatManager(), HeartbeatToken);

                // call server connected delegate
                if (ServerConnected != null) Task.Run(() => ServerConnected());
                return true;
            } 
        }

        /// <summary>
        /// Retrieve a list of all clients on the server.
        /// </summary>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="clients">The list of clients received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListClients(out Message response, out List<Client> clients)
        {
            response = null;
            clients = null;

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "ListClients";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListClients unable to retrieve server response");
                return false;
            } 

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListClients null response from server");
                return false;
            } 

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListClients failed with response data " + response.Data.ToString());
                return false;
            }
            else
            { 
                if (response.Data != null)
                {
                    SuccessData resp = Helper.DeserializeJson<SuccessData>(response.Data);
                    clients = ((JArray)resp.Data).ToObject<List<Client>>();
                }
                return true;
            }
        }

        /// <summary>
        /// Retrieve a list of all channels on the server.
        /// </summary>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="channels">The list of channels received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListChannels(out Message response, out List<Channel> channels)
        {
            response = null;
            channels = null;

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "ListChannels";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannels unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannels null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannels failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = Helper.DeserializeJson<SuccessData>(response.Data);
                    channels = ((JArray)resp.Data).ToObject<List<Channel>>();
                }
                return true;
            }
        }

        /// <summary>
        /// Retrieve a list of all members in a specific channel.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="clients">The list of clients that are members in the specified channel on the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListChannelMembers(string guid, out Message response, out List<Client> clients)
        { 
            response = null;
            clients = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "ListChannelMembers";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannelMembers unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannelMembers null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannelMembers failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = Helper.DeserializeJson<SuccessData>(response.Data);
                    if (resp != null && resp.Data != null)
                    {
                        clients = ((JArray)resp.Data).ToObject<List<Client>>();
                    }
                    else
                    {
                        clients = new List<Client>();
                    }
                }

                return true;
            } 
        }

        /// <summary>
        /// Retrieve a list of all subscribers in a specific channel.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="clients">The list of clients subscribed to the specified channel on the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListChannelSubscribers(string guid, out Message response, out List<Client> clients)
        {
            response = null;
            clients = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "ListChannelSubscribers";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannelSubscribers unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannelSubscribers null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListChannelSubscribers failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = Helper.DeserializeJson<SuccessData>(response.Data);
                    clients = ((JArray)resp.Data).ToObject<List<Client>>();
                }
                return true;
            }
        }

        /// <summary>
        /// Join a specified channel on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool JoinChannel(string guid, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "JoinChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "JoinChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "JoinChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "JoinChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Subscribe to multicast messages on a specified channel on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SubscribeChannel(string guid, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "SubscribeChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscribeChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscribeChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscribeChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Leave a channel on the server to which you are joined.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool LeaveChannel(string guid, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "LeaveChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "LeaveChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "LeaveChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "LeaveChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Unsubscribe from multicast messages on a specified channel on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool UnsubscribeChannel(string guid, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "UnsubscribeChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "UnsubscribeChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "UnsubscribeChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "UnsubscribeChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Create a broadcast channel on the server.  Messages sent to broadcast channels are sent to all members.
        /// </summary>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="priv">Whether or not the channel is private (1) or public (0).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool CreateBroadcastChannel(string name, int priv, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (priv != 0 && priv != 1) throw new ArgumentOutOfRangeException("Value for priv must be 0 or 1");

            Channel currentChannel = new Channel();
            currentChannel.ChannelName = name;
            currentChannel.OwnerGUID = ClientGUID;
            currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            currentChannel.Private = priv;
            currentChannel.Broadcast = 1;
            currentChannel.Multicast = 0;
            currentChannel.Unicast = 0;

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "CreateChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = currentChannel.ChannelGUID;
            request.Data = Encoding.UTF8.GetBytes(Helper.SerializeJson(currentChannel));

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateBroadcastChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateBroadcastChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateBroadcastChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Create a unicast channel on the server.  Messages sent to unicast channels are sent only to one subscriber randomly.
        /// </summary>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="priv">Whether or not the channel is private (1) or public (0).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool CreateUnicastChannel(string name, int priv, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (priv != 0 && priv != 1) throw new ArgumentOutOfRangeException("Value for priv must be 0 or 1");

            Channel currentChannel = new Channel();
            currentChannel.ChannelName = name;
            currentChannel.OwnerGUID = ClientGUID;
            currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            currentChannel.Private = priv;
            currentChannel.Broadcast = 0;
            currentChannel.Multicast = 0;
            currentChannel.Unicast = 1;

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "CreateChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = currentChannel.ChannelGUID;
            request.Data = Encoding.UTF8.GetBytes(Helper.SerializeJson(currentChannel));

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateUnicastChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateUnicastChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateUnicastChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Create a multicast channel on the server.  Messages sent to multicast channels are sent only to subscribers.
        /// </summary>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="priv">Whether or not the channel is private (1) or public (0).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool CreateMulticastChannel(string name, int priv, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (priv != 0 && priv != 1) throw new ArgumentOutOfRangeException("Value for priv must be 0 or 1");

            Channel currentChannel = new Channel();
            currentChannel.ChannelName = name;
            currentChannel.OwnerGUID = ClientGUID;
            currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            currentChannel.Private = priv;
            currentChannel.Broadcast = 0;
            currentChannel.Multicast = 1;
            currentChannel.Unicast = 0;

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "CreateChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = currentChannel.ChannelGUID;
            request.Data = Encoding.UTF8.GetBytes(Helper.SerializeJson(currentChannel));

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "CreateChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Delete a channel you own on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool DeleteChannel(string guid, out Message response)
        { 
            response = null;
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "DeleteChannel";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteChannel unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteChannel null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteChannel failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                return true;
            } 
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendPrivateMessageAsync(string guid, string data)
        { 
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return SendPrivateMessageAsync(guid, Encoding.UTF8.GetBytes(data)); 
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendPrivateMessageAsync(string guid, byte[] data)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = null;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = guid;
            request.ChannelGUID = null;
            request.Data = data;
            return SendMessage(request); 
        }

        /// <summary>
        /// Send a private message to another user on this server synchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendPrivateMessageSync(string guid, string data, out Message response)
        { 
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return SendPrivateMessageSync(guid, Encoding.UTF8.GetBytes(data), out response); 
        }

        /// <summary>
        /// Send a private message to another user on this server synchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendPrivateMessageSync(string guid, byte[] data, out Message response)
        { 
            response = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));

            Message message = new Message();
            message.Email = Email;
            message.Password = Password;
            message.Command = null;
            message.CreatedUtc = DateTime.Now.ToUniversalTime();
            message.MessageID = Guid.NewGuid().ToString();
            message.SenderGUID = ClientGUID;
            message.SenderName = Name;
            message.RecipientGUID = guid;
            message.ChannelGUID = null;
            message.SyncRequest = true;
            message.Data = data;

            if (!AddSyncRequest(message.MessageID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendPrivateMessageSync unable to register sync request GUID " + message.MessageID);
                return false;
            }

            if (!SendMessage(message))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendPrivateMessage unable to send message GUID " + message.MessageID + " to recipient " + message.RecipientGUID);
                return false;
            }

            int timeoutMs = Config.SyncTimeoutMs;
            if (message.SyncTimeoutMs != null) timeoutMs = Convert.ToInt32(message.SyncTimeoutMs);

            if (!GetSyncResponse(message.MessageID, timeoutMs, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendPrivateMessage unable to get response for message GUID " + message.MessageID);
                return false;
            }
                
            return true; 
        }

        /// <summary>
        /// Send a private message to the server asynchronously.
        /// </summary>
        /// <param name="request">The message object you wish to send to the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendServerMessageAsync(Message request)
        { 
            if (request == null) throw new ArgumentNullException(nameof(request));
            request.RecipientGUID = ServerGuid;
            return SendMessage(request); 
        }

        /// <summary>
        /// Send a private message to the server synchronously.
        /// </summary>
        /// <param name="request">The message object you wish to send to the server.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendServerMessageSync(Message request, out Message response)
        { 
            response = null;
            if (request == null) throw new ArgumentNullException(nameof(request));
            if (String.IsNullOrEmpty(request.MessageID)) request.MessageID = Guid.NewGuid().ToString();
            request.SyncRequest = true;
            request.RecipientGUID = ServerGuid;
             
            if (!AddSyncRequest(request.MessageID))
            { 
                _Logging.Log(LoggingModule.Severity.Warn, "SendServerMessageSync unable to register sync request GUID " + request.MessageID);
                return false;
            }
            else
            { 
                _Logging.Log(LoggingModule.Severity.Debug, "SendServerMessageSync registered sync request GUID " + request.MessageID);
            }
             
            if (!SendMessage(request))
            { 
                _Logging.Log(LoggingModule.Severity.Warn, "SendServerMessageSync unable to send message GUID " + request.MessageID + " to server");
                return false;
            }
            else
            { 
                _Logging.Log(LoggingModule.Severity.Debug, "SendServerMessageSync sent message GUID " + request.MessageID + " to server");
            }
             
            int timeoutMs = Config.SyncTimeoutMs;
            if (request.SyncTimeoutMs != null) timeoutMs = Convert.ToInt32(request.SyncTimeoutMs);
             
            if (!GetSyncResponse(request.MessageID, timeoutMs, out response))
            { 
                _Logging.Log(LoggingModule.Severity.Warn, "SendServerMessageSync unable to get response for message GUID " + request.MessageID);
                return false;
            }
            else
            { 
                _Logging.Log(LoggingModule.Severity.Debug, "SendServerMessageSync received response for message GUID " + request.MessageID);
            }
 
            return true; 
        }

        /// <summary>
        /// Send an async message to a channel, which is in turn sent to recipients based on channel configuration.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelMessageAsync(string guid, string data)
        { 
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return SendChannelMessageAsync(guid, Encoding.UTF8.GetBytes(data)); 
        }

        /// <summary>
        /// Send an async message to a channel, which is in turn sent to recipients based on channel configuration.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelMessageAsync(string guid, byte[] data)
        { 
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = null;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = null;
            request.ChannelGUID = guid;
            request.Data = data;
            return SendMessage(request); 
        }

        /// <summary>
        /// Send a sync message to a channel, which is in turn sent to recipients based on channel configuration. 
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelMessageSync(string guid, string data, out Message response)
        { 
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return SendChannelMessageSync(guid, Encoding.UTF8.GetBytes(data), out response); 
        }

        /// <summary>
        /// Send a sync message to a channel, which is in turn sent to recipients based on channel configuration. 
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelMessageSync(string guid, byte[] data, out Message response)
        { 
            response = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = null;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = null;
            request.ChannelGUID = guid;
            request.SyncRequest = true;
            request.Data = data;

            if (!AddSyncRequest(request.MessageID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMessageSync unable to register sync request GUID " + request.MessageID);
                return false;
            }

            if (!SendMessage(request))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMessageSync unable to send message GUID " + request.MessageID + " to channel " + request.ChannelGUID);
                return false;
            }

            int timeoutMs = Config.SyncTimeoutMs;
            if (request.SyncTimeoutMs != null) timeoutMs = Convert.ToInt32(request.SyncTimeoutMs);

            if (!GetSyncResponse(request.MessageID, timeoutMs, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMessageSync unable to get response for message GUID " + request.MessageID);
                return false;
            }
                
            return true; 
        }

        /// <summary>
        /// Retrieve the list of synchronous requests awaiting responses.
        /// </summary>
        /// <param name="response">A dictionary containing the GUID of the synchronous request (key) and the timestamp it was sent (value).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool PendingSyncRequests(out Dictionary<string, DateTime> response)
        { 
            response = null;
            if (_SyncRequests == null) return true;
            if (_SyncRequests.Count < 1) return true;

            response = _SyncRequests.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            return true; 
        }

        /// <summary>
        /// Discern whether or not a given client is connected.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool IsClientConnected(string guid, out Message response)
        { 
            response = null;

            Message request = new Message();
            request.Email = Email;
            request.Password = Password;
            request.Command = "IsClientConnected";
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = ClientGUID;
            request.SenderName = Name;
            request.RecipientGUID = ServerGuid;
            request.SyncRequest = true;
            request.ChannelGUID = null;
            request.Data = Encoding.UTF8.GetBytes(guid);

            if (!SendServerMessageSync(request, out response))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListClients unable to retrieve server response");
                return false;
            }

            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListClients null response from server");
                return false;
            }

            if (!Helper.IsTrue(response.Success))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ListClients failed with response data " + response.Data.ToString());
                return false;
            }
            else
            {
                if (response.Data != null)
                {
                    SuccessData ret = Helper.DeserializeJson<SuccessData>(Encoding.UTF8.GetString(response.Data));
                    return (ret.Success && Convert.ToBoolean(ret.Data));
                }

                return false;
            } 
        }

        #endregion

        #region Private-Watson-Methods

        private bool WTcpServerConnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server connection detected");
            Connected = true;
            return true;
        }

        private bool WTcpServerDisconnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server disconnect detected");
            Connected = false;
            if (ServerDisconnected != null) Task.Run(() => ServerDisconnected());
            return true;
        }

        private bool WTcpMessageReceived(byte[] data)
        {
            Message curr = new Message(data);
            HandleMessage(curr);
            return true;
        }

        private bool WTcpSslServerConnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server connection detected");
            Connected = true;
            return true;
        }

        private bool WTcpSslServerDisconnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server disconnect detected");
            Connected = false;
            if (ServerDisconnected != null) Task.Run(() => ServerDisconnected());
            return true;
        }

        private bool WTcpSslMessageReceived(byte[] data)
        {
            Message curr = new Message(data);
            HandleMessage(curr);
            return true;
        }

        private bool WWsServerConnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server connection detected");
            Connected = true;
            return true;
        }

        private bool WWsServerDisconnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server disconnect detected");
            Connected = false;
            if (ServerDisconnected != null) Task.Run(() => ServerDisconnected());
            return true;
        }

        private bool WWsMessageReceived(byte[] data)
        {
            Message curr = new Message(data);
            HandleMessage(curr);
            return true;
        }

        private bool WWsSslServerConnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server connection detected");
            Connected = true;
            return true;
        }

        private bool WWsSslServerDisconnected()
        {
            _Logging.Log(LoggingModule.Severity.Info, "Server disconnect detected");
            Connected = false;
            if (ServerDisconnected != null) Task.Run(() => ServerDisconnected());
            return true;
        }

        private bool WWsSslMessageReceived(byte[] data)
        {
            Message curr = new Message(data);
            HandleMessage(curr);
            return true;
        }

        #endregion

        #region Private-Sync-Methods

        //
        // Ensure that none of these methods call another method within this region
        // otherwise you have a lock within a lock!  There should be NO methods
        // outside of this region that have a lock statement
        //

        private bool SyncResponseReady(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SyncResponseReady null GUID supplied");
                return false;
            }

            if (_SyncResponses == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SyncResponseReady null sync responses list, initializing");
                _SyncResponses = new ConcurrentDictionary<string, Message>();
                return false;
            }

            if (_SyncResponses.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SyncResponseReady no entries in sync responses list");
                return false;
            }

            if (_SyncResponses.ContainsKey(guid))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "SyncResponseReady found sync response for GUID " + guid);
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Warn, "SyncResponseReady no sync response for GUID " + guid);
            return false;
        }

        private bool AddSyncRequest(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddSyncRequest null GUID supplied");
                return false;
            }

            if (_SyncRequests == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddSyncRequest null sync requests list, initializing");
                _SyncRequests = new ConcurrentDictionary<string, DateTime>();
            }

            if (_SyncRequests.ContainsKey(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddSyncRequest already contains an entry for GUID " + guid);
                return false;
            }

            _SyncRequests.TryAdd(guid, DateTime.Now);
            _Logging.Log(LoggingModule.Severity.Debug, "AddSyncRequest added request for GUID " + guid + ": " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss"));
            return true;
        }

        private bool RemoveSyncRequest(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RemoveSyncRequest null GUID supplied");
                return false;
            }

            if (_SyncRequests == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RemoveSyncRequest null sync requests list, initializing");
                _SyncRequests = new ConcurrentDictionary<string, DateTime>();
                return false;
            }

            DateTime TempDateTime;
            if (_SyncRequests.ContainsKey(guid)) _SyncRequests.TryRemove(guid, out TempDateTime);
            
            _Logging.Log(LoggingModule.Severity.Debug, "RemoveSyncRequest removed sync request for GUID " + guid);
            return true;
        }

        private bool SyncRequestExists(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SyncRequestExists null GUID supplied");
                return false;
            }
            
            if (_SyncRequests == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SyncRequestExists null sync requests list, initializing");
                _SyncRequests = new ConcurrentDictionary<string, DateTime>();
                return false;
            }

            if (_SyncRequests.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SyncRequestExists empty sync requests list, returning false");
                return false;
            }

            if (_SyncRequests.ContainsKey(guid))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "SyncRequestExists found sync request for GUID " + guid);
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Warn, "SyncRequestExists unable to find sync request for GUID " + guid);
            return false;
        }

        private bool AddSyncResponse(Message response)
        {
            if (response == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddSyncResponse null BigQMessage supplied");
                return false;
            }

            if (String.IsNullOrEmpty(response.MessageID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddSyncResponse null MessageId within supplied message");
                return false;
            }
            
            if (_SyncResponses.ContainsKey(response.MessageID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddSyncResponse response already awaits for MessageId " + response.MessageID);
                return false;
            }

            _SyncResponses.TryAdd(response.MessageID, response);
            _Logging.Log(LoggingModule.Severity.Debug, "AddSyncResponse added sync response for MessageId " + response.MessageID);
            return true;
        }

        private bool GetSyncResponse(string guid, int timeoutMs, out Message response)
        { 
            response = new Message();
            DateTime start = DateTime.Now;
             
            #region Check-for-Null-Values

            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetSyncResponse null GUID supplied");
                return false;
            }

            if (_SyncResponses == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetSyncResponse null sync responses list, initializing");
                _SyncResponses = new ConcurrentDictionary<string, Message>();
                return false;
            }

            if (timeoutMs < 1000)
            {
                timeoutMs = 1000;
            }

            #endregion

            #region Process

            int iterations = 0;
            while (true)
            {
                // if (Config.Debug.Enable && Config.Debug.MsgResponseTime) Logging.Log(LoggingModule.Severity.Debug, "GetSyncResponse iteration " + iterations + " " + sw.Elapsed.TotalMilliseconds + "ms");

                if (_SyncResponses.ContainsKey(guid))
                {
                    if (!_SyncResponses.TryGetValue(guid, out response))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "GetSyncResponse unable to retrieve sync response for GUID " + guid + " though one exists");
                        return false;
                    }
                     
                    response.Success = true;
                    _Logging.Log(LoggingModule.Severity.Debug, "GetSyncResponse returning response for message GUID " + guid);
                    return true;
                }

                //
                // Check if timeout exceeded
                //
                TimeSpan ts = DateTime.Now - start;
                if (ts.TotalMilliseconds > timeoutMs)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetSyncResponse timeout waiting for response for message GUID " + guid); 
                    response = null;
                    return false;
                }

                iterations++;
                continue;
            }

            #endregion 
        }
        
        private void CleanupSyncRequests()
        {
            while (true)
            {
                Task.Delay(Config.SyncTimeoutMs).Wait();
                List<string> expiredIds = new List<string>();
                DateTime tempTimestamp;
                Message tempMessage;

                foreach (KeyValuePair<string, DateTime> currRequest in _SyncRequests)
                {
                    DateTime expiryTimestamp = currRequest.Value.AddMilliseconds(Config.SyncTimeoutMs);

                    if (DateTime.Compare(expiryTimestamp, DateTime.Now) < 0)
                    {
                        #region Expiration-Earlier-Than-Current-Time

                        _Logging.Log(LoggingModule.Severity.Debug, "CleanupSyncRequests adding MessageId " + currRequest.Key + " (added " + currRequest.Value.ToString("MM/dd/yyyy hh:mm:ss") + ") to cleanup list (past expiration time " + expiryTimestamp.ToString("MM/dd/yyyy hh:mm:ss") + ")");
                        expiredIds.Add(currRequest.Key);

                        #endregion
                    }
                }

                foreach (string CurrentRequestGuid in expiredIds)
                {
                    // 
                    // remove from sync requests
                    //
                    _SyncRequests.TryRemove(CurrentRequestGuid, out tempTimestamp);
                }
               
                foreach (string CurrentRequestGuid in expiredIds)
                {
                    if (_SyncResponses.ContainsKey(CurrentRequestGuid))
                    {
                        //
                        // remove from sync responses
                        //
                        _SyncResponses.TryRemove(CurrentRequestGuid, out tempMessage);
                    }
                } 
            }
        }

        #endregion

        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        { 
            if (disposing)
            {
                if (_WTcpClient != null) _WTcpClient.Dispose();
                if (_WTcpSslClient != null) _WTcpSslClient.Dispose();
                if (_WWsClient != null) _WWsClient.Dispose();
                if (_WWsSslClient != null) _WWsSslClient.Dispose();

                if (_CleanupSyncTokenSource != null) _CleanupSyncTokenSource.Cancel(false);
                if (HeartbeatTokenSource != null) HeartbeatTokenSource.Cancel(false);
                if (ProcessClientQueueTokenSource != null) ProcessClientQueueTokenSource.Cancel(false);

                return;
            }
        }
        
        private void HandleMessage(Message currentMessage)
        { 
            if (String.Compare(currentMessage.SenderGUID, ServerGuid) == 0
                && !String.IsNullOrEmpty(currentMessage.Command)
                && String.Compare(currentMessage.Command.ToLower().Trim(), "heartbeatrequest") == 0)
            {
                #region Handle-Incoming-Server-Heartbeat

                // do nothing
                return;

                #endregion
            }
            else if (String.Compare(currentMessage.SenderGUID, ServerGuid) == 0
                && !String.IsNullOrEmpty(currentMessage.Command)
                && String.Compare(currentMessage.Command.ToLower().Trim(), "event") == 0)
            {
                #region Server-Event-Message

                if (currentMessage.Data != null)
                {
                    #region Data-Exists

                    EventData ev = null;
                    try
                    {
                        ev = Helper.DeserializeJson<EventData>(currentMessage.Data);
                    }
                    catch (Exception)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage unable to deserialize incoming server message to event");
                        return;
                    }

                    if (ev == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage null event object after deserializing incoming server message");
                        return;
                    }

                    switch (ev.EventType)
                    {
                        case EventTypes.ClientJoinedServer:
                            if (ClientJoinedServer != null) Task.Run(() => ClientJoinedServer(ev.Data.ToString()));
                            return;

                        case EventTypes.ClientLeftServer:
                            if (ClientLeftServer != null) Task.Run(() => ClientLeftServer(ev.Data.ToString()));
                            return;

                        case EventTypes.ClientJoinedChannel:
                            if (ClientJoinedChannel != null) Task.Run(() => ClientJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            return;

                        case EventTypes.ClientLeftChannel:
                            if (ClientLeftChannel != null) Task.Run(() => ClientLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            return;

                        case EventTypes.SubscriberJoinedChannel:
                            if (SubscriberJoinedChannel != null) Task.Run(() => SubscriberJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            return;

                        case EventTypes.SubscriberLeftChannel:
                            if (SubscriberLeftChannel != null) Task.Run(() => SubscriberLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            return;

                        case EventTypes.ChannelCreated:
                            if (ChannelCreated != null) Task.Run(() => ChannelCreated(ev.Data.ToString()));
                            return;

                        case EventTypes.ChannelDestroyed:
                            if (ChannelDestroyed != null) Task.Run(() => ChannelDestroyed(ev.Data.ToString()));
                            return;

                        default:
                            _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage unknown event type: " + ev.EventType);
                            return;
                    }

                    #endregion
                }
                else
                {
                    // do nothing
                    return;
                }

                #endregion
            }
            else if (Helper.IsTrue(currentMessage.SyncRequest))
            {
                #region Handle-Incoming-Sync-Request

                _Logging.Log(LoggingModule.Severity.Debug, "HandleMessage sync request detected for message GUID " + currentMessage.MessageID);

                if (SyncMessageReceived != null)
                {
                    byte[] ResponseData = SyncMessageReceived(currentMessage);

                    currentMessage.Success = true;
                    currentMessage.SyncRequest = false;
                    currentMessage.SyncResponse = true;
                    currentMessage.Data = ResponseData;
                    string tempGuid = String.Copy(currentMessage.SenderGUID);
                    currentMessage.SenderGUID = ClientGUID;
                    currentMessage.RecipientGUID = tempGuid;

                    SendMessage(currentMessage); 
                    _Logging.Log(LoggingModule.Severity.Debug, "HandleMessage sent response message for message GUID " + currentMessage.MessageID);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage sync request received for MessageId " + currentMessage.MessageID + " but no handler specified, sending async");
                    if (AsyncMessageReceived != null)
                    {
                        Task.Run(() => AsyncMessageReceived(currentMessage));
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage no method defined for AsyncMessageReceived");
                    }
                }

                #endregion
            }
            else if (Helper.IsTrue(currentMessage.SyncResponse))
            {
                #region Handle-Incoming-Sync-Response

                _Logging.Log(LoggingModule.Severity.Debug, "HandleMessage sync response detected for message GUID " + currentMessage.MessageID);

                if (SyncRequestExists(currentMessage.MessageID))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "HandleMessage sync request exists for message GUID " + currentMessage.MessageID);

                    if (AddSyncResponse(currentMessage))
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "HandleMessage added sync response for message GUID " + currentMessage.MessageID);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage unable to add sync response for MessageId " + currentMessage.MessageID + ", sending async");
                        if (AsyncMessageReceived != null)
                        {
                            Task.Run(() => AsyncMessageReceived(currentMessage));
                        }
                        else
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage no method defined for AsyncMessageReceived");
                        }

                    }
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage message marked as sync response but no sync request found for MessageId " + currentMessage.MessageID + ", sending async");
                    if (AsyncMessageReceived != null)
                    {
                        Task.Run(() => AsyncMessageReceived(currentMessage));
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage no method defined for AsyncMessageReceived");
                    }
                }

                #endregion
            }
            else
            {
                #region Handle-Async

                _Logging.Log(LoggingModule.Severity.Debug, "HandleMessage async message GUID " + currentMessage.MessageID);

                if (AsyncMessageReceived != null)
                {
                    Task.Run(() => AsyncMessageReceived(currentMessage));
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HandleMessage no method defined for AsyncMessageReceived");
                }

                #endregion
            } 
        }
         
        private void HeartbeatManager()
        { 
            DateTime threadStart = DateTime.Now;
            DateTime lastHeartbeatAttempt = DateTime.Now;
            DateTime lastSuccess = DateTime.Now;
            DateTime lastFailure = DateTime.Now;
            int numConsecutiveFailures = 0;
            bool firstRun = true;
              
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
                Message HeartbeatMessage = HeartbeatRequestMessage();
                    
                if (!SendServerMessageAsync(HeartbeatMessage))
                { 
                    numConsecutiveFailures++;
                    lastFailure = DateTime.Now;

                    _Logging.Log(LoggingModule.Severity.Warn, "HeartbeatManager failed to send heartbeat to server (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                    if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "HeartbeatManager maximum number of failed heartbeats reached");
                        Connected = false;
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
        }

        private Message HeartbeatRequestMessage()
        {
            Message request = new Message();
            request.MessageID = Guid.NewGuid().ToString();
            request.RecipientGUID = ServerGuid;
            request.Command = "HeartbeatRequest";
            request.SenderGUID = ClientGUID;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.Data = null;
            return request;
        }

        private string RandomName()
        {
            string[] names = new string[]
            {
                "anthony", 
                "brian", 
                "chris",
                "david",
                "ed",
                "fred",
                "george",
                "harry",
                "isaac",
                "joel",
                "kevin",
                "larry",
                "mark",
                "noah",
                "oscar",
                "pete",
                "quentin",
                "ryan",
                "steve",
                "uriah",
                "victor",
                "will",
                "xavier",
                "yair",
                "zachary",
                "ashley",
                "brianna",
                "chloe",
                "daisy",
                "emma",
                "fiona",
                "grace",
                "hannah",
                "isabella",
                "jenny",
                "katie",
                "lisa",
                "maria",
                "natalie",
                "olivia",
                "pearl",
                "quinn",
                "riley",
                "sophia",
                "tara",
                "ulyssa",
                "victoria",
                "whitney",
                "xena",
                "yuri",
                "zoey"
            };

            int selected = _Random.Next(0, names.Length - 1);
            return names[selected];
        }

        #endregion
    }
}
