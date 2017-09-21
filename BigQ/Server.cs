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
using SyslogLogging;
using WatsonTcp;
using WatsonWebsocket;

namespace BigQ
{
    /// <summary>
    /// The BigQ server listens on TCP and Websockets and acts as a message queue and distribution network for connected clients.
    /// </summary>
    public class Server : IDisposable
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
        private DateTime _CreatedUtc;
        private Random _Random;
        private string _ServerGUID = "00000000-0000-0000-0000-000000000000";

        //
        // Logging
        //
        private LoggingModule _Logging;

        //
        // resources
        //
        private MessageBuilder _MsgBuilder;
        private ConnectionManager _ConnMgr;
        private ChannelManager _ChannelMgr;
        private ConcurrentDictionary<string, DateTime> _ClientActiveSendMap;     // Receiver GUID, AddedUTC

        //
        // Server variables
        //
        private WatsonTcpServer _WTcpServer;
        private WatsonTcpSslServer _WTcpSslServer;
        private WatsonWsServer _WWsServer;
        private WatsonWsServer _WWsSslServer;
        
        //
        // authentication and authorization
        //
        private string _UsersLastModified;
        private ConcurrentList<User> _UsersList;
        private CancellationTokenSource _UsersCancellationTokenSource;
        private CancellationToken _UsersCancellationToken;

        private string _PermissionsLastModified;
        private ConcurrentList<Permission> _PermissionsList;
        private CancellationTokenSource _PermissionsCancellationTokenSource;
        private CancellationToken _PermissionsCancellationToken;

        //
        // cleanup
        //
        private CancellationTokenSource _CleanupCancellationTokenSource;
        private CancellationToken _CleanupCancellationToken;

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
         
        #endregion

        #region Constructors

        /// <summary>
        /// Start an instance of the BigQ server process.
        /// </summary>
        /// <param name="configFile">The full path and filename of the configuration file.  Leave null for a default configuration.</param>
        public Server(string configFile)
        {
            #region Load-Config

            _CreatedUtc = DateTime.Now.ToUniversalTime();
            _Random = new Random((int)DateTime.Now.Ticks);

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

            _Logging.Log(LoggingModule.Severity.Debug, "BigQ server configuration loaded");

            #endregion

            #region Set-Class-Variables

            if (!String.IsNullOrEmpty(Config.GUID)) _ServerGUID = Config.GUID;

            _MsgBuilder = new MessageBuilder(_ServerGUID);
            _ConnMgr = new ConnectionManager(_Logging, Config);
            _ChannelMgr = new ChannelManager(_Logging, Config);
            _ClientActiveSendMap = new ConcurrentDictionary<string, DateTime>();
             
            _UsersLastModified = "";
            _UsersList = new ConcurrentList<User>();
            _PermissionsLastModified = "";
            _PermissionsList = new ConcurrentList<Permission>();

            #endregion

            #region Set-Delegates-to-Null

            MessageReceived = null;
            ServerStopped = null;
            ClientConnected = null;
            ClientLogin = null;
            ClientDisconnected = null;
            
            #endregion
             
            #region Start-Users-and-Permissions-File-Monitor

            _UsersCancellationTokenSource = new CancellationTokenSource();
            _UsersCancellationToken = _UsersCancellationTokenSource.Token;
            Task.Run(() => MonitorUsersFile(), _UsersCancellationToken);

            _PermissionsCancellationTokenSource = new CancellationTokenSource();
            _PermissionsCancellationToken = _PermissionsCancellationTokenSource.Token;
            Task.Run(() => MonitorPermissionsFile(), _PermissionsCancellationToken);

            #endregion

            #region Start-Cleanup-Task

            _CleanupCancellationTokenSource = new CancellationTokenSource();
            _CleanupCancellationToken = _CleanupCancellationTokenSource.Token;
            Task.Run(() => CleanupTask(), _CleanupCancellationToken);

            #endregion

            #region Start-Server-Channels

            if (Config.ServerChannels != null && Config.ServerChannels.Count > 0)
            {
                Client CurrentClient = new Client();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGUID = _ServerGUID;
                CurrentClient.IpPort = "127.0.0.1:0";
                CurrentClient.CreatedUtc = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUtc = CurrentClient.CreatedUtc;

                foreach (Channel curr in Config.ServerChannels)
                {
                    if (!AddChannel(CurrentClient, curr))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "Unable to add server channel " + curr.ChannelName);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "Added server channel " + curr.ChannelName);
                    }
                }
            }

            #endregion
            
            #region Start-Watson-Servers

            if (Config.TcpServer.Enable)
            {
                #region Start-TCP-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting TCP server: " + Config.TcpServer.Ip + ":" + Config.TcpServer.Port);

                _WTcpServer = new WatsonTcpServer(
                    Config.TcpServer.Ip,
                    Config.TcpServer.Port,
                    WTcpClientConnected,
                    WTcpClientDisconnected,
                    WTcpMessageReceived,
                    Config.TcpServer.Debug);
                 
                #endregion
            }

            if (Config.TcpSslServer.Enable)
            {
                #region Start-TCP-SSL-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting TCP SSL server: " + Config.TcpSslServer.Ip + ":" + Config.TcpSslServer.Port);

                _WTcpSslServer = new WatsonTcpSslServer(
                    Config.TcpSslServer.Ip,
                    Config.TcpSslServer.Port,
                    Config.TcpSslServer.PfxCertFile,
                    Config.TcpSslServer.PfxCertPassword,
                    Config.TcpSslServer.AcceptInvalidCerts,
                    false,
                    WTcpSslClientConnected,
                    WTcpSslClientDisconnected,
                    WTcpSslMessageReceived,
                    Config.TcpSslServer.Debug);

                #endregion
            }

            if (Config.WebsocketServer.Enable)
            {
                #region Start-Websocket-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting websocket server: " + Config.WebsocketServer.Ip + ":" + Config.WebsocketServer.Port);

                _WWsServer = new WatsonWsServer(
                    Config.WebsocketServer.Ip,
                    Config.WebsocketServer.Port,
                    false,
                    false,
                    null,
                    WWsClientConnected,
                    WWsClientDisconnected,
                    WWsMessageReceived,
                    Config.WebsocketServer.Debug);

                #endregion
            }

            if (Config.WebsocketSslServer.Enable)
            {
                #region Start-Websocket-SSL-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting websocket SSL server: " + Config.WebsocketSslServer.Ip + ":" + Config.WebsocketSslServer.Port);

                _WWsSslServer = new WatsonWsServer(
                    Config.WebsocketSslServer.Ip,
                    Config.WebsocketSslServer.Port,
                    true,
                    Config.WebsocketSslServer.AcceptInvalidCerts,
                    null,
                    WWsSslClientConnected,
                    WWsSslClientDisconnected,
                    WWsSslMessageReceived,
                    Config.WebsocketServer.Debug);

                #endregion
            }

            _Logging.Log(LoggingModule.Severity.Debug, "BigQ server started");

            #endregion
        }

        /// <summary>
        /// Start an instance of the BigQ server process.
        /// </summary>
        /// <param name="config">Populated server configuration object.</param>
        public Server(ServerConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            config.ValidateConfig();
            Config = config;

            _CreatedUtc = DateTime.Now.ToUniversalTime();
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

            _Logging.Log(LoggingModule.Severity.Debug, "BigQ server configuration loaded");

            #endregion

            #region Set-Class-Variables

            if (!String.IsNullOrEmpty(Config.GUID)) _ServerGUID = Config.GUID;

            _MsgBuilder = new MessageBuilder(_ServerGUID);
            _ConnMgr = new ConnectionManager(_Logging, Config);
            _ChannelMgr = new ChannelManager(_Logging, Config);
            _ClientActiveSendMap = new ConcurrentDictionary<string, DateTime>();

            _UsersLastModified = "";
            _UsersList = new ConcurrentList<User>();
            _PermissionsLastModified = "";
            _PermissionsList = new ConcurrentList<Permission>();

            #endregion

            #region Set-Delegates-to-Null

            MessageReceived = null;
            ServerStopped = null;
            ClientConnected = null;
            ClientLogin = null;
            ClientDisconnected = null;

            #endregion

            #region Start-Users-and-Permissions-File-Monitor

            _UsersCancellationTokenSource = new CancellationTokenSource();
            _UsersCancellationToken = _UsersCancellationTokenSource.Token;
            Task.Run(() => MonitorUsersFile(), _UsersCancellationToken);

            _PermissionsCancellationTokenSource = new CancellationTokenSource();
            _PermissionsCancellationToken = _PermissionsCancellationTokenSource.Token;
            Task.Run(() => MonitorPermissionsFile(), _PermissionsCancellationToken);

            #endregion

            #region Start-Cleanup-Task

            _CleanupCancellationTokenSource = new CancellationTokenSource();
            _CleanupCancellationToken = _CleanupCancellationTokenSource.Token;
            Task.Run(() => CleanupTask(), _CleanupCancellationToken);

            #endregion

            #region Start-Server-Channels

            if (Config.ServerChannels != null && Config.ServerChannels.Count > 0)
            {
                Client CurrentClient = new Client();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGUID = _ServerGUID;
                CurrentClient.IpPort = "127.0.0.1:0";
                CurrentClient.CreatedUtc = DateTime.Now.ToUniversalTime();
                CurrentClient.UpdatedUtc = CurrentClient.CreatedUtc;

                foreach (Channel curr in Config.ServerChannels)
                {
                    if (!AddChannel(CurrentClient, curr))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "Unable to add server channel " + curr.ChannelName);
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "Added server channel " + curr.ChannelName);
                    }
                }
            }

            #endregion

            #region Start-Watson-Servers

            if (Config.TcpServer.Enable)
            {
                #region Start-TCP-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting TCP server: " + Config.TcpServer.Ip + ":" + Config.TcpServer.Port);

                _WTcpServer = new WatsonTcpServer(
                    Config.TcpServer.Ip,
                    Config.TcpServer.Port,
                    WTcpClientConnected,
                    WTcpClientDisconnected,
                    WTcpMessageReceived,
                    Config.TcpServer.Debug);

                #endregion
            }

            if (Config.TcpSslServer.Enable)
            {
                #region Start-TCP-SSL-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting TCP SSL server: " + Config.TcpSslServer.Ip + ":" + Config.TcpSslServer.Port);

                _WTcpSslServer = new WatsonTcpSslServer(
                    Config.TcpSslServer.Ip,
                    Config.TcpSslServer.Port,
                    Config.TcpSslServer.PfxCertFile,
                    Config.TcpSslServer.PfxCertPassword,
                    Config.TcpSslServer.AcceptInvalidCerts,
                    false,
                    WTcpSslClientConnected,
                    WTcpSslClientDisconnected,
                    WTcpSslMessageReceived,
                    Config.TcpSslServer.Debug);

                #endregion
            }

            if (Config.WebsocketServer.Enable)
            {
                #region Start-Websocket-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting websocket server: " + Config.WebsocketServer.Ip + ":" + Config.WebsocketServer.Port);

                _WWsServer = new WatsonWsServer(
                    Config.WebsocketServer.Ip,
                    Config.WebsocketServer.Port,
                    false,
                    false,
                    null,
                    WWsClientConnected,
                    WWsClientDisconnected,
                    WWsMessageReceived,
                    Config.WebsocketServer.Debug);

                #endregion
            }

            if (Config.WebsocketSslServer.Enable)
            {
                #region Start-Websocket-SSL-Server

                _Logging.Log(LoggingModule.Severity.Debug, "Starting websocket SSL server: " + Config.WebsocketSslServer.Ip + ":" + Config.WebsocketSslServer.Port);

                _WWsSslServer = new WatsonWsServer(
                    Config.WebsocketSslServer.Ip,
                    Config.WebsocketSslServer.Port,
                    true,
                    Config.WebsocketSslServer.AcceptInvalidCerts,
                    null,
                    WWsSslClientConnected,
                    WWsSslClientDisconnected,
                    WWsSslMessageReceived,
                    Config.WebsocketServer.Debug);

                #endregion
            }

            _Logging.Log(LoggingModule.Severity.Debug, "BigQ server started");

            #endregion
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down the server and dispose of background workers.
        /// </summary>
        public void Dispose()
        {
            _Logging.Log(LoggingModule.Severity.Debug, "BigQ server terminating");
            Dispose(true);
        }
         
        /// <summary>
        /// Retrieve list of all channels.
        /// </summary>
        /// <returns>List of Channel objects.</returns>
        public List<Channel> ListChannels()
        {
            return _ChannelMgr.GetChannels();
        }

        /// <summary>
        /// Retrieve list of members in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListChannelMembers(string guid)
        {
            return _ChannelMgr.GetChannelMembers(guid);
        }

        /// <summary>
        /// Retrieve list of subscribers in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListChannelSubscribers(string guid)
        {
            return _ChannelMgr.GetChannelSubscribers(guid);
        }

        /// <summary>
        /// Retrieve list of all clients on the server.
        /// </summary>
        /// <returns>List of Client objects.</returns>
        public List<Client> ListClients()
        {
            return _ConnMgr.GetClients();
        }

        /// <summary>
        /// Retrieve list of all client GUID to IP:port maps.
        /// </summary>
        /// <returns>A dictionary containing client GUIDs (keys) and IP:port strings (values).</returns>
        public Dictionary<string, string> ListClientGUIDMaps()
        {
            return _ConnMgr.GetGUIDMaps();
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
            _ClientActiveSendMap.Clear();
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

        /// <summary>
        /// Create a broadcast channel owned by the server.
        /// </summary>
        /// <param name="name">The name of the channel.</param>
        /// <param name="guid">The GUID of the channel.  If null, a random GUID will be created.</param>
        /// <param name="priv">Indicates whether or not the channel is private, i.e. hidden from list channel responses.</param>
        public void CreateBroadcastChannel(string name, string guid, int priv)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            Channel newChannel = new Channel();

            if (!String.IsNullOrEmpty(guid)) newChannel.ChannelGUID = guid;
            else newChannel.ChannelGUID = Guid.NewGuid().ToString();

            newChannel.ChannelName = name;
            newChannel.OwnerGUID = _ServerGUID;
            newChannel.CreatedUtc = timestamp;
            newChannel.UpdatedUtc = timestamp;
            newChannel.Private = priv;
            newChannel.Broadcast = 1;
            newChannel.Multicast = 0;
            newChannel.Unicast = 0;
            newChannel.Members = new List<Client>();
            newChannel.Subscribers = new List<Client>();
            
            _ChannelMgr.AddChannel(newChannel);

            _Logging.Log(LoggingModule.Severity.Debug, "CreateBroadcastChannel successfully added server channel with GUID " + newChannel.ChannelGUID);
            return;
        }

        /// <summary>
        /// Create a unicast channel owned by the server.
        /// </summary>
        /// <param name="name">The name of the channel.</param>
        /// <param name="guid">The GUID of the channel.  If null, a random GUID will be created.</param>
        /// <param name="priv">Indicates whether or not the channel is private, i.e. hidden from list channel responses.</param>
        public void CreateUnicastChannel(string name, string guid, int priv)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            Channel newChannel = new Channel();

            if (!String.IsNullOrEmpty(guid)) newChannel.ChannelGUID = guid;
            else newChannel.ChannelGUID = Guid.NewGuid().ToString();

            newChannel.ChannelName = name;
            newChannel.OwnerGUID = _ServerGUID;
            newChannel.CreatedUtc = timestamp;
            newChannel.UpdatedUtc = timestamp;
            newChannel.Private = priv;
            newChannel.Broadcast = 0;
            newChannel.Multicast = 0;
            newChannel.Unicast = 1;
            newChannel.Members = new List<Client>();
            newChannel.Subscribers = new List<Client>();

            _ChannelMgr.AddChannel(newChannel);

            _Logging.Log(LoggingModule.Severity.Debug, "CreateUnicastChannel successfully added server channel with GUID " + newChannel.ChannelGUID);
            return;
        }

        /// <summary>
        /// Create a multicast channel owned by the server.
        /// </summary>
        /// <param name="name">The name of the channel.</param>
        /// <param name="guid">The GUID of the channel.  If null, a random GUID will be created.</param>
        /// <param name="priv">Indicates whether or not the channel is private, i.e. hidden from list channel responses.</param>
        public void CreateMulticastChannel(string name, string guid, int priv)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            Channel newChannel = new Channel();

            if (!String.IsNullOrEmpty(guid)) newChannel.ChannelGUID = guid;
            else newChannel.ChannelGUID = Guid.NewGuid().ToString();

            newChannel.ChannelName = name;
            newChannel.OwnerGUID = _ServerGUID;
            newChannel.CreatedUtc = timestamp;
            newChannel.UpdatedUtc = timestamp;
            newChannel.Private = priv;
            newChannel.Broadcast = 0;
            newChannel.Multicast = 1;
            newChannel.Unicast = 0;
            newChannel.Members = new List<Client>();
            newChannel.Subscribers = new List<Client>();

            _ChannelMgr.AddChannel(newChannel);

            _Logging.Log(LoggingModule.Severity.Debug, "CreateMulticastChannel successfully added server channel with GUID " + newChannel.ChannelGUID);
            return;
        }

        /// <summary>
        /// Delete a channel from the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool DeleteChannel(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Channel currentChannel = _ChannelMgr.GetChannelByGUID(guid);
            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DeleteChannel unable to find specified channel");
                return false;
            }

            return RemoveChannel(currentChannel);
        }

        #endregion

        #region Private-Watson-Methods

        private bool WTcpClientConnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WTcpClientConnected new connection from " + ipPort);
            Client currentClient = new Client(ipPort, true, false, false);
            _ConnMgr.AddClient(currentClient);
            StartClientQueue(currentClient);
            return true;
        }

        private bool WTcpClientDisconnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WTcpClientDisconnected connection termination from " + ipPort);
            Client currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(Client))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "WTcpClientDisconnected unable to find client " + ipPort);
                return true;
            }
            currentClient.Dispose();
            _ConnMgr.RemoveClient(ipPort);
            return true;
        }

        private bool WTcpMessageReceived(string ipPort, byte[] data)
        {
            Client currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(Client))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "WTcpMessageReceived unable to retrieve client " + ipPort);
                return true;
            }

            Message currentMessage = new Message(data);
            MessageProcessor(currentClient, currentMessage);
            if (MessageReceived != null) Task.Run(() => MessageReceived(currentMessage));
            return true;
        }
         
        private bool WTcpSslClientConnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WTcpSslClientConnected new connection from " + ipPort);
            Client currentClient = new Client(ipPort, true, false, true);
            _ConnMgr.AddClient(currentClient);
            StartClientQueue(currentClient);
            return true;
        }

        private bool WTcpSslClientDisconnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WTcpSslClientDisconnected connection termination from " + ipPort);
            Client currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(Client))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "WTcpSslClientDisconnected unable to find client " + ipPort);
                return true;
            }
            currentClient.Dispose();
            _ConnMgr.RemoveClient(ipPort);
            return true;
        }

        private bool WTcpSslMessageReceived(string ipPort, byte[] data)
        {
            Client currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(Client))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "WTcpSslMessageReceived unable to retrieve client " + ipPort);
                return true;
            }

            Message currentMessage = new Message(data);
            MessageProcessor(currentClient, currentMessage);
            if (MessageReceived != null) Task.Run(() => MessageReceived(currentMessage));
            return true;
        }

        private bool WWsClientConnected(string ipPort, IDictionary<string, string> qs)
        {
            try
            {
                _Logging.Log(LoggingModule.Severity.Debug, "WWsClientConnected new connection from " + ipPort);
                Client currentClient = new Client(ipPort, false, true, false);
                _ConnMgr.AddClient(currentClient);
                StartClientQueue(currentClient);
                return true;
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "WWsClientConnected", e);
                return false;
            }
        }

        private bool WWsClientDisconnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WWsClientDisconnected connection termination from " + ipPort);
            Client currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(Client))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "WWsClientDisconnected unable to find client " + ipPort);
                return true;
            }
            currentClient.Dispose();
            _ConnMgr.RemoveClient(ipPort);
            return true;
        }

        private bool WWsMessageReceived(string ipPort, byte[] data)
        {
            try
            {
                Client currentClient = _ConnMgr.GetByIpPort(ipPort);
                if (currentClient == null || currentClient == default(Client))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "WWsMessageReceived unable to retrieve client " + ipPort);
                    return true;
                }

                Message currentMessage = new Message(data);
                Console.WriteLine(currentMessage.ToString());
                MessageProcessor(currentClient, currentMessage);
                if (MessageReceived != null) Task.Run(() => MessageReceived(currentMessage));
                return true;
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "WWsMessageReceived", e);
                return false;
            }
        }

        private bool WWsSslClientConnected(string ipPort, IDictionary<string, string> qs)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WWsSslClientConnected new connection from " + ipPort);
            Client currentClient = new Client(ipPort, false, true, true);
            _ConnMgr.AddClient(currentClient);
            StartClientQueue(currentClient);
            return true;
        }

        private bool WWsSslClientDisconnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WWsSslClientDisconnected connection termination from " + ipPort);
            Client currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(Client))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "WWsSslClientDisconnected unable to find client " + ipPort);
                return true;
            }
            currentClient.Dispose();
            _ConnMgr.RemoveClient(ipPort);
            return true;
        }

        private bool WWsSslMessageReceived(string ipPort, byte[] data)
        {
            Client currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(Client))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "WWsSslMessageReceived unable to retrieve client " + ipPort);
                return true;
            }

            Message currentMessage = new Message(data);
            MessageProcessor(currentClient, currentMessage);
            if (MessageReceived != null) Task.Run(() => MessageReceived(currentMessage));
            return true;
        }
        
        #endregion

        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_WTcpServer != null) _WTcpServer.Dispose();
                if (_WTcpSslServer != null) _WTcpSslServer.Dispose();
                if (_WWsServer != null) _WWsServer.Dispose();
                if (_WWsSslServer != null) _WWsSslServer.Dispose();
                if (_UsersCancellationTokenSource != null) _UsersCancellationTokenSource.Cancel();
                if (_PermissionsCancellationTokenSource != null) _PermissionsCancellationTokenSource.Cancel();
                if (_CleanupCancellationTokenSource != null) _CleanupCancellationTokenSource.Cancel();
                return;
            }
        }

        #endregion
         
        #region Private-Senders-and-Queues

        private bool SendMessage(Client currentClient, Message currentMessage)
        {
            bool locked = false;

            try
            {
                #region Check-for-Null-Values

                if (currentClient == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendMessage null client supplied");
                    return false;
                }

                if (currentMessage == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendMessage null message supplied");
                    return false;
                }

                if (String.IsNullOrEmpty(currentMessage.RecipientGUID))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendMessage null recipient GUID supplied");
                    return false;
                }

                #endregion

                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!_ClientActiveSendMap.TryAdd(currentMessage.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //

                    Task.Delay(25).Wait();
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "SendMessage locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "SendMessage locked send map attempting to add recipient GUID " + currentMessage.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message

                byte[] data = currentMessage.ToBytes();

                if (currentClient.IsTcp && !currentClient.IsSsl) return _WTcpServer.Send(currentClient.IpPort, data);
                else if (currentClient.IsTcp && currentClient.IsSsl) return _WTcpSslServer.Send(currentClient.IpPort, data);
                else if (currentClient.IsWebsocket && !currentClient.IsSsl) return _WWsServer.SendAsync(currentClient.IpPort, data).Result;
                else if (currentClient.IsWebsocket && currentClient.IsSsl) return _WWsSslServer.SendAsync(currentClient.IpPort, data).Result;
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendMessage unable to discern transport for client " + currentClient.IpPort);
                    return false;
                }

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "SendMessage " + currentClient.IpPort, e);
                return false;
            }
            finally
            {
                #region Cleanup
                       
                if (locked)
                {
                    DateTime removedVal = DateTime.Now;
                    int removeLoopCount = 0;
                    while (!_ClientActiveSendMap.TryRemove(currentMessage.RecipientGUID, out removedVal))
                    {
                        Task.Delay(25).Wait();
                        removeLoopCount += 25;

                        if (!_ClientActiveSendMap.ContainsKey(currentMessage.RecipientGUID))
                        {
                            // there was (temporarily) a conflict that has been resolved
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "SendMessage locked send map attempting to remove recipient GUID " + currentMessage.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }

                #endregion
            }
        }

        private bool ChannelDataSender(Client currentClient, Channel currentChannel, Message currentMessage)
        { 
            if (Helper.IsTrue(currentChannel.Broadcast))
            {
                #region Broadcast-Channel

                List<Client> currChannelMembers = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
                if (currChannelMembers == null || currChannelMembers.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender no members found in channel " + currentChannel.ChannelGUID);
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
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender error queuing channel message from " + currentMessage.SenderGUID + " to member " + currentMessage.RecipientGUID + " in channel " + currentMessage.ChannelGUID);
                        }
                    });
                }

                return true;

                #endregion
            }
            else if (Helper.IsTrue(currentChannel.Multicast))
            {
                #region Multicast-Channel-to-Subscribers

                List<Client> currChannelSubscribers = _ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
                if (currChannelSubscribers == null || currChannelSubscribers.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender no subscribers found in channel " + currentChannel.ChannelGUID);
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
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender error queuing channel message from " + currentMessage.SenderGUID + " to subscriber " + currentMessage.RecipientGUID + " in channel " + currentMessage.ChannelGUID);
                        }
                    });
                }

                return true;

                #endregion
            }
            else if (Helper.IsTrue(currentChannel.Unicast))
            {
                #region Unicast-Channel-to-Subscriber

                List<Client> currChannelSubscribers = _ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
                if (currChannelSubscribers == null || currChannelSubscribers.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender no subscribers found in channel " + currentChannel.ChannelGUID);
                    return true;
                }

                currentMessage.SenderGUID = currentClient.ClientGUID;
                Client recipient = currChannelSubscribers[_Random.Next(0, currChannelSubscribers.Count)];
                Task.Run(() =>
                {
                    currentMessage.RecipientGUID = recipient.ClientGUID;
                    bool respSuccess = false;
                    respSuccess = QueueClientMessage(recipient, currentMessage);
                    if (!respSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender error queuing channel message from " + currentMessage.SenderGUID + " to subscriber " + currentMessage.RecipientGUID + " in channel " + currentMessage.ChannelGUID);
                    }
                });

                return true;

                #endregion
            }
            else
            {
                #region Unknown

                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender channel is not designated as broadcast, multicast, or unicast, deleting");
                return RemoveChannel(currentChannel);

                #endregion
            }
        }
        
        private bool QueueClientMessage(Client currentClient, Message currentMessage)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "QueueClientMessage queued message for client " + currentClient.IpPort + " " + currentClient.ClientGUID + " from " + currentMessage.SenderGUID);
            currentClient.MessageQueue.Add(currentMessage);
            return true;
        }

        private bool ProcessClientQueue(Client currentClient)
        {
            try
            { 
                #region Process

                while (true)
                {
                    Message currMessage = currentClient.MessageQueue.Take(currentClient.ProcessClientQueueToken);
                    if (currMessage != null)
                    {
                        if (String.IsNullOrEmpty(currMessage.RecipientGUID))
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientQueue unable to deliver message " + currMessage.MessageID + " from " + currMessage.SenderGUID + " (empty recipient), discarding");
                        }
                        else
                        {
                            bool success = SendMessage(currentClient, currMessage);
                            if (!success)
                            {
                                Client tempClient = _ConnMgr.GetClientByGUID(currMessage.RecipientGUID);
                                if (tempClient == null)
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientQueue recipient " + currMessage.RecipientGUID + " no longer exists, disposing");
                                    currentClient.Dispose();
                                    return false;
                                }
                                else
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientQueue unable to deliver message from " + currMessage.SenderGUID + " to " + currMessage.RecipientGUID + ", requeuing (client still exists)");
                                    currentClient.MessageQueue.Add(currMessage);
                                }
                            }
                            else
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientQueue successfully sent message from " + currMessage.SenderGUID + " to " + currMessage.RecipientGUID);
                            }
                        }
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientQueue received null message from queue for client " + currentClient.ClientGUID + ", discarding");
                    }
                }

                #endregion
            }
            catch (OperationCanceledException oce)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientQueue canceled for client " + currentClient.IpPort + ": " + oce.Message);
                return false;
            }
            catch (Exception e)
            {
                if (currentClient != null)
                {
                    _Logging.LogException("Server", "ProcessClientQueue (" + currentClient.IpPort + ")", e);
                }
                else
                {
                    _Logging.LogException("Server", "ProcessClientQueue (null)", e);
                }

                return false;
            }
        }

        private void StartClientQueue(Client currentClient)
        {
            currentClient.ProcessClientQueueTokenSource = new CancellationTokenSource();
            currentClient.ProcessClientQueueToken = currentClient.ProcessClientQueueTokenSource.Token;
            _Logging.Log(LoggingModule.Severity.Debug, "StartClientQueue starting queue processor for " + currentClient.IpPort);
            Task.Run(() => ProcessClientQueue(currentClient), currentClient.ProcessClientQueueToken);
        }

        private void HeartbeatManager(Client currentClient)
        { 
            // Should only be called after client login

            try
            {
                #region Check-for-Null-Values

                if (Config.Heartbeat.IntervalMs <= 0)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HeartbeatManager invalid heartbeat interval, using 1000ms");
                    Config.Heartbeat.IntervalMs = 1000;
                }
                 
                if (String.IsNullOrEmpty(currentClient.ClientGUID))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "HeartbeatManager null client GUID in supplied client");
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

                    Message heartbeatMessage = _MsgBuilder.HeartbeatRequest(currentClient);
                    if (Config.Debug.SendHeartbeat) _Logging.Log(LoggingModule.Severity.Debug, "HeartbeatManager sending heartbeat to " + currentClient.IpPort + " GUID " + currentClient.ClientGUID);

                    if (!SendMessage(currentClient, heartbeatMessage))
                    {
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        _Logging.Log(LoggingModule.Severity.Debug, "HeartbeatManager failed to send heartbeat to client " + currentClient.IpPort + " (" + numConsecutiveFailures + "/" + Config.Heartbeat.MaxFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= Config.Heartbeat.MaxFailures)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "HeartbeatManager maximum number of failed heartbeats reached, abandoning client " + currentClient.IpPort);
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
            catch (ThreadAbortException)
            {
                // do nothing
            }
            catch (Exception e)
            { 
                _Logging.LogException("Server", "HeartbeatManager " + currentClient.IpPort, e);
            }
            finally
            {
                List<Channel> affectedChannels = null;
                _ConnMgr.RemoveClient(currentClient.IpPort);
                _ChannelMgr.RemoveClientChannels(currentClient.ClientGUID, out affectedChannels);
                ChannelDestroyEvent(affectedChannels);
                _ChannelMgr.RemoveClient(currentClient.IpPort);
                if (Config.Notification.ServerJoinNotification) Task.Run(() => ServerLeaveEvent(currentClient));
                currentClient.Dispose();
            }
        }
         
        #endregion
        
        #region Private-Event-Methods

        private bool ServerJoinEvent(Client currentClient)
        {
            if (currentClient == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerJoinEvent null ClientGUID suplied within Client");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ServerJoinEvent sending server join notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID);

            List<Client> currentClients = _ConnMgr.GetClients(); 
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerJoinEvent no clients found on server");
                return true;
            }

            Message msg = _MsgBuilder.ServerJoinEvent(currentClient);

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
                            _Logging.Log(LoggingModule.Severity.Warn, "ServerJoinEvent error queuing server join event to " + msg.RecipientGUID + " (join by " + currentClient.ClientGUID + ")");
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
                _Logging.Log(LoggingModule.Severity.Warn, "ServerLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerLeaveEvent null ClientGUID suplied within Client");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ServerLeaveEvent sending server leave notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID);

            List<Client> currentClients = _ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerLeaveEvent no clients found on server");
                return true;
            }

            Message msg = _MsgBuilder.ServerLeaveEvent(currentClient);

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
                            _Logging.Log(LoggingModule.Severity.Warn, "ServerLeaveEvent error queuing server leave event to " + msg.RecipientGUID + " (leave by " + currentClient.ClientGUID + ")");
                        }
                        else
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "ServerLeaveEvent queued server leave event to " + msg.RecipientGUID + " (leave by " + currentClient.ClientGUID + ")");
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
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent null GUID supplied within Channel");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ChannelJoinEvent sending channel join notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelJoinEvent(currentChannel, currentClient);

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
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent error queuing channel join event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (join by " + currentClient.ClientGUID + ")");
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
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent null GUID supplied within Channel");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ChannelLeaveEvent sending channel leave notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelLeaveEvent(currentChannel, currentClient);

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
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent error queuing channel leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
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
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent null GUID supplied within Channel");
                return true;
            }

            if (Helper.IsTrue(currentChannel.Private))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent skipping create notification for channel " + currentChannel.ChannelGUID + " (private)");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ChannelCreateEvent sending channel create notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = _ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent no clients found on server");
                return true;
            }

            foreach (Client curr in currentClients)
            {
                Task.Run(() =>
                {
                    Message msg = _MsgBuilder.ChannelCreateEvent(currentClient, currentChannel);
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                    if (!responseSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent error queuing channel create event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
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
                            Message msg = _MsgBuilder.ChannelDestroyEvent(currMember, currChannel);
                            msg.RecipientGUID = currMember.ClientGUID;
                            bool responseSuccess = SendSystemMessage(msg);
                            if (!responseSuccess)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent error sending channel destroy event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currChannel.OwnerGUID + ")");
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
                            Message msg = _MsgBuilder.ChannelDestroyEvent(currSubscriber, currChannel);
                            msg.RecipientGUID = currSubscriber.ClientGUID;
                            bool responseSuccess = SendSystemMessage(msg);
                            if (!responseSuccess)
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent error sending channel destroy event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currChannel.OwnerGUID + ")");
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
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent null GUID supplied within Channel");
                return true;
            }

            if (Helper.IsTrue(currentChannel.Private))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent skipping destroy notification for channel " + currentChannel.ChannelGUID + " (private)");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ChannelDestroyEvent sending channel destroy notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            foreach (Client curr in currentClients)
            {
                Task.Run(() =>
                {
                    Message msg = _MsgBuilder.ChannelDestroyEvent(currentClient, currentChannel);
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                    if (!responseSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent error queuing channel leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
                    }
                });
            }

            return true;
        }

        private bool SubscriberJoinEvent(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent null GUID supplied within Channel");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "SubscriberJoinEvent sending subcriber join notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = _ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelSubscriberJoinEvent(currentChannel, currentClient);

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
                            _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent error queuing subscriber join event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (join by " + currentClient.ClientGUID + ")");
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
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent null Client supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentClient.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent null ClientGUID supplied within Client");
                return true;
            }

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent null Channel supplied");
                return true;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent null GUID supplied within Channel");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "SubscriberLeaveEvent sending subscriber leave notification for " + currentClient.IpPort + " GUID " + currentClient.ClientGUID + " channel " + currentChannel.ChannelGUID);

            List<Client> currentClients = _ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent no clients found in channel " + currentChannel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelSubscriberLeaveEvent(currentChannel, currentClient);

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
                            _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent error queuing subscriber leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + currentClient.ClientGUID + ")");
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
                        _UsersList = new ConcurrentList<User>();
                        continue;
                    }

                    #endregion

                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(_UsersLastModified))
                    {
                        #region First-Read

                        _Logging.Log(LoggingModule.Severity.Debug, "MonitorUsersFile loading " + Config.Files.UsersFile);

                        //
                        // get timestamp
                        //
                        _UsersLastModified = File.GetLastWriteTimeUtc(Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(Config.Files.UsersFile);
                        if (String.IsNullOrEmpty(fileContents))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile empty file found at " + Config.Files.UsersFile);
                            continue;
                        }

                        try
                        {
                            _UsersList = Helper.DeserializeJson<ConcurrentList<User>>(Encoding.UTF8.GetBytes(fileContents));
                        }
                        catch (Exception EInner)
                        {
                            _Logging.LogException("Server", "MonitorUsersFile", EInner);
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile unable to deserialize contents of " + Config.Files.UsersFile);
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
                        if (String.Compare(_UsersLastModified, tempTimestamp) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "MonitorUsersFile loading " + Config.Files.UsersFile);

                            //
                            // get timestamp
                            //
                            _UsersLastModified = File.GetLastWriteTimeUtc(Config.Files.UsersFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(Config.Files.UsersFile);
                            if (String.IsNullOrEmpty(fileContents))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile empty file found at " + Config.Files.UsersFile);
                                continue;
                            }

                            try
                            {
                                _UsersList = Helper.DeserializeJson<ConcurrentList<User>>(Encoding.UTF8.GetBytes(fileContents));
                            }
                            catch (Exception EInner)
                            {
                                _Logging.LogException("Server", "MonitorUsersFile", EInner);
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorUsersFile unable to deserialize contents of " + Config.Files.UsersFile);
                                continue;
                            }
                        }

                        #endregion
                    }

                    #endregion
                }
            }
            catch (ThreadAbortException)
            {
                // do nothing
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "MonitorUsersFile", e);
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
                        _PermissionsList = new ConcurrentList<Permission>();
                        continue;
                    }

                    #endregion

                    #region Process

                    string tempTimestamp = "";
                    string fileContents = "";

                    if (String.IsNullOrEmpty(_PermissionsLastModified))
                    {
                        #region First-Read

                        _Logging.Log(LoggingModule.Severity.Debug, "MonitorPermissionsFile loading " + Config.Files.PermissionsFile);

                        //
                        // get timestamp
                        //
                        _PermissionsLastModified = File.GetLastWriteTimeUtc(Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                        //
                        // read and store
                        //
                        fileContents = File.ReadAllText(Config.Files.PermissionsFile);
                        if (String.IsNullOrEmpty(fileContents))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile empty file found at " + Config.Files.PermissionsFile);
                            continue;
                        }

                        try
                        {
                            _PermissionsList = Helper.DeserializeJson<ConcurrentList<Permission>>(Encoding.UTF8.GetBytes(fileContents));
                        }
                        catch (Exception EInner)
                        {
                            _Logging.LogException("Server", "MonitorPermissionsFile", EInner);
                            _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile unable to deserialize contents of " + Config.Files.PermissionsFile);
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
                        if (String.Compare(_PermissionsLastModified, tempTimestamp) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "MonitorPermissionsFile loading " + Config.Files.PermissionsFile);

                            //
                            // get timestamp
                            //
                            _PermissionsLastModified = File.GetLastWriteTimeUtc(Config.Files.PermissionsFile).ToString("MMddyyyy-HHmmss");

                            //
                            // read and store
                            //
                            fileContents = File.ReadAllText(Config.Files.PermissionsFile);
                            if (String.IsNullOrEmpty(fileContents))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile empty file found at " + Config.Files.PermissionsFile);
                                continue;
                            }

                            try
                            {
                                _PermissionsList = Helper.DeserializeJson<ConcurrentList<Permission>>(Encoding.UTF8.GetBytes(fileContents));
                            }
                            catch (Exception EInner)
                            {
                                _Logging.LogException("Server", "MonitorPermissionsFile", EInner);
                                _Logging.Log(LoggingModule.Severity.Warn, "MonitorPermissionsFile unable to deserialize contents of " + Config.Files.PermissionsFile);
                                continue;
                            }
                        }

                        #endregion
                    }

                    #endregion
                }
            }
            catch (ThreadAbortException)
            {
                // do nothing
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "MonitorPermissionsFile", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private bool AllowConnection(string email, string ip)
        {
            try
            {
                if (_UsersList != null && _UsersList.Count > 0)
                {
                    #region Check-for-Null-Values

                    if (String.IsNullOrEmpty(email))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection no email supplied");
                        return false;
                    }

                    if (String.IsNullOrEmpty(ip))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection no IP supplied");
                        return false;
                    }

                    #endregion

                    #region Users-List-Present

                    User currUser = GetUser(email);
                    if (currUser == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection unable to find entry for email " + email);
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
                            _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection permission entry " + currUser.Permission + " not found for user " + email);
                            return false;
                        }

                        if (!currPermission.Login)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "AllowConnection login permission denied in permission entry " + currUser.Permission + " for user " + email);
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
                _Logging.LogException("Server", "AllowConnection", e);
                return false;
            }
        }

        private User GetUser(string email)
        {
            try
            {
                #region Check-for-Null-Values

                if (_UsersList == null || _UsersList.Count < 1) return null;

                if (String.IsNullOrEmpty(email))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetUser null email supplied");
                    return null;
                }

                #endregion

                #region Process

                foreach (User currUser in _UsersList)
                {
                    if (String.IsNullOrEmpty(currUser.Email)) continue;
                    if (String.Compare(currUser.Email.ToLower(), email.ToLower()) == 0)
                    {
                        return currUser;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Warn, "GetUser unable to find email " + email);
                return null;

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "GetUser", e);
                return null;
            }
        }

        private Permission GetUserPermission(string email)
        {
            try
            {
                #region Check-for-Null-Values

                if (_PermissionsList == null || _PermissionsList.Count < 1) return null;
                if (_UsersList == null || _UsersList.Count < 1) return null;

                if (String.IsNullOrEmpty(email))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetUserPermissions null email supplied");
                    return null;
                }

                #endregion

                #region Process

                User currUser = GetUser(email);
                if (currUser == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetUserPermission unable to find user " + email);
                    return null;
                }

                if (String.IsNullOrEmpty(currUser.Permission)) return null;
                return GetPermission(currUser.Permission);

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "GetUserPermissions", e);
                return null;
            }
        }

        private Permission GetPermission(string permission)
        {
            try
            {
                #region Check-for-Null-Values

                if (_PermissionsList == null || _PermissionsList.Count < 1) return null;
                if (String.IsNullOrEmpty(permission))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "GetPermission null permission supplied");
                    return null;
                }

                #endregion

                #region Process

                foreach (Permission currPermission in _PermissionsList)
                {
                    if (String.IsNullOrEmpty(currPermission.Name)) continue;
                    if (String.Compare(permission.ToLower(), currPermission.Name.ToLower()) == 0)
                    {
                        return currPermission;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Warn, "GetPermission permission " + permission + " not found");
                return null;

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "GetPermission", e);
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
                    _Logging.Log(LoggingModule.Severity.Warn, "AuthorizeMessage null message supplied");
                    return false;
                }

                if (_UsersList == null || _UsersList.Count < 1)
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
                        _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateUser unable to find user " + currentMessage.Email);
                        return false;
                    }

                    if (!String.IsNullOrEmpty(currUser.Password))
                    {
                        if (String.Compare(currUser.Password, currentMessage.Password) != 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateUser invalid password supplied for user " + currentMessage.Email);
                            return false;
                        }
                    }

                    #endregion

                    #region Verify-Permissions

                    if (String.IsNullOrEmpty(currUser.Permission))
                    {
                        // default permit
                        // Logging.Log(LoggingModule.Severity.Debug, "AuthenticateUser default permit in use (user " + CurrentMessage.Email + " has null permission list)");
                        return true;
                    }

                    if (String.IsNullOrEmpty(currentMessage.Command))
                    {
                        // default permit
                        // Logging.Log(LoggingModule.Severity.Debug, "AuthenticateUser default permit in use (user " + CurrentMessage.Email + " sending message with no command)");
                        return true;
                    }

                    Permission currPermission = GetPermission(currUser.Permission);
                    if (currPermission == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AuthorizeMessage unable to find permission " + currUser.Permission + " for user " + currUser.Email);
                        return false;
                    }

                    if (currPermission.Permissions == null || currPermission.Permissions.Count < 1)
                    {
                        // default permit
                        // Logging.Log(LoggingModule.Severity.Debug, "AuthorizeMessage default permit in use (no permissions found for permission name " + currUser.Permission);
                        return true;
                    }

                    if (currPermission.Permissions.Contains(currentMessage.Command))
                    {
                        // Logging.Log(LoggingModule.Severity.Debug, "AuthorizeMessage found permission for command " + CurrentMessage.Command + " in permission " + currUser.Permission + " for user " + currUser.Email);
                        return true;
                    }
                    else
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "AuthorizeMessage permission " + currPermission.Name + " does not contain command " + currentMessage.Command + " for user " + currUser.Email);
                        return false;
                    }

                    #endregion
                }
                else
                {
                    #region No-Material

                    _Logging.Log(LoggingModule.Severity.Warn, "AuthenticateUser no authentication material supplied");
                    return false;

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "AuthorizeMessage", e);
                return false;
            }
        }

        private List<User> GetCurrentUsersFile()
        {
            if (_UsersList == null || _UsersList.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetCurrentUsersFile no users listed or no users file");
                return null;
            }

            List<User> ret = new List<User>();
            foreach (User curr in _UsersList)
            {
                ret.Add(curr);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "GetCurrentUsersFile returning " + ret.Count + " users");
            return ret;
        }

        private List<Permission> GetCurrentPermissionsFile()
        {
            if (_PermissionsList == null || _PermissionsList.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "GetCurrentPermissionsFile no permissions listed or no permissions file");
                return null;
            }

            List<Permission> ret = new List<Permission>();
            foreach (Permission curr in _PermissionsList)
            {
                ret.Add(curr);
            }

            _Logging.Log(LoggingModule.Severity.Debug, "GetCurrentPermissionsFile returning " + ret.Count + " permissions");
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

                    foreach (KeyValuePair<string, DateTime> curr in _ClientActiveSendMap)
                    {
                        if (String.IsNullOrEmpty(curr.Key)) continue;
                        if (DateTime.Compare(DateTime.Now.ToUniversalTime(), curr.Value) > 0)
                        {
                            Task.Run(() =>
                            {
                                int elapsed = 0;
                                while (true)
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "CleanupTask attempting to remove active send map for " + curr.Key + " (elapsed " + elapsed + "ms)");
                                    if (!_ClientActiveSendMap.ContainsKey(curr.Key))
                                    {
                                        _Logging.Log(LoggingModule.Severity.Debug, "CleanupTask key " + curr.Key + " no longer present in active send map, exiting");
                                        break;
                                    }
                                    else
                                    {
                                        DateTime removedVal = DateTime.Now;
                                        if (_ClientActiveSendMap.TryRemove(curr.Key, out removedVal))
                                        {
                                            _Logging.Log(LoggingModule.Severity.Debug, "CleanupTask key " + curr.Key + " removed by cleanup task, exiting");
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
            catch (ThreadAbortException)
            {
                // do nothing
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "CleanupTask", e);
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
            if (_ClientActiveSendMap == null || _ClientActiveSendMap.Count < 1) return new Dictionary<string, DateTime>();
            Dictionary<string, DateTime> ret = _ClientActiveSendMap.ToDictionary(entry => entry.Key, entry => entry.Value);
            return ret;
        }
        
        private bool AddChannel(Client currentClient, Channel currentChannel)
        {
            if (currentClient == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddChannel null client supplied");
                return false;
            }

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddChannel null channel supplied");
                return false;
            }

            if (String.IsNullOrEmpty(currentChannel.ChannelGUID)) currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(currentChannel.ChannelName)) currentChannel.ChannelName = currentChannel.ChannelGUID;

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            if (currentChannel.CreatedUtc == null) currentChannel.CreatedUtc = timestamp;
            if (currentChannel.UpdatedUtc == null) currentChannel.UpdatedUtc = timestamp;
            currentChannel.Members = new List<Client>();
            currentChannel.Members.Add(currentClient);
            currentChannel.Subscribers = new List<Client>();
            currentChannel.OwnerGUID = currentClient.ClientGUID;

            if (_ChannelMgr.ChannelExists(currentChannel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddChannel channel GUID " + currentChannel.ChannelGUID + " already exists");
                return false;
            }

            _ChannelMgr.AddChannel(currentChannel);

            _Logging.Log(LoggingModule.Severity.Debug, "AddChannel successfully added channel with GUID " + currentChannel.ChannelGUID + " for client " + currentChannel.OwnerGUID);
            return true;
        }

        private bool RemoveChannel(Channel currentChannel)
        {
            currentChannel = _ChannelMgr.GetChannelByGUID(currentChannel.ChannelGUID);
            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RemoveChannel unable to find specified channel");
                return false;
            }
                
            if (String.Compare(currentChannel.OwnerGUID, _ServerGUID) == 0)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel skipping removal of channel " + currentChannel.ChannelGUID + " (server channel)");
                return true;
            }

            _ChannelMgr.RemoveChannel(currentChannel.ChannelGUID);
            _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel notifying channel members of channel removal");

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
                        foreach (Client currentClient in tempMembers)
                        {
                            if (String.Compare(currentClient.ClientGUID, currentChannel.OwnerGUID) != 0)
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel notifying channel " + tempChannel.ChannelGUID + " member " + currentClient.ClientGUID + " of channel deletion by owner");
                                SendSystemMessage(_MsgBuilder.ChannelDeletedByOwner(currentClient, tempChannel));
                            }
                        }
                    }
                    );
                }
            }

            _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel removed channel " + currentChannel.ChannelGUID + " successfully");
            return true;
        }

        private bool AddChannelMember(Client currentClient, Channel currentChannel)
        {
            if (_ChannelMgr.AddChannelMember(currentChannel, currentClient))
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
            if (_ChannelMgr.AddChannelSubscriber(currentChannel, currentClient))
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
            if (_ChannelMgr.RemoveChannelMember(currentChannel, currentClient))
            {
                #region Send-Notifications

                if (Config.Notification.ChannelJoinNotification)
                {
                    List<Client> curr = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
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
                                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelMember notifying channel " + TempChannel.ChannelGUID + " member " + c.ClientGUID + " of channel leave by member " + currentClient.ClientGUID);
                                SendSystemMessage(_MsgBuilder.ChannelLeaveEvent(TempChannel, currentClient));
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
            if (_ChannelMgr.RemoveChannelMember(currentChannel, currentClient))
            {
                #region Send-Notifications

                if (Config.Notification.ChannelJoinNotification)
                {
                    List<Client> curr = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
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
                                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelSubscriber notifying channel " + tempChannel.ChannelGUID + " member " + c.ClientGUID + " of channel leave by subscriber " + currentClient.ClientGUID);
                                SendSystemMessage(_MsgBuilder.ChannelSubscriberLeaveEvent(tempChannel, currentClient));
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
            return _ChannelMgr.IsChannelMember(currentClient, currentChannel);
        }

        private bool IsChannelSubscriber(Client currentClient, Channel currentChannel)
        {
            return _ChannelMgr.IsChannelSubscriber(currentClient, currentChannel);
        }

        #endregion

        #region Private-Message-Processing-Methods
         
        private bool MessageProcessor(Client currentClient, Message currentMessage)
        {  
            #region Check-for-Null-Values

            if (currentClient == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor null client supplied");
                return false;
            }

            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor null message supplied");
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

                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor received message from client with no GUID");
                        responseSuccess = QueueClientMessage(currentClient, _MsgBuilder.LoginRequired());
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue login required message to client " + currentClient.IpPort);
                        }
                        return responseSuccess;

                        #endregion
                    }
                }
            }
            else
            {
                #region Ensure-GUID-Exists

                if (String.Compare(currentClient.ClientGUID, _ServerGUID) != 0)
                {
                    //
                    // All zeros is the BigQ server
                    //
                    Client verifyClient = _ConnMgr.GetClientByGUID(currentClient.ClientGUID);
                    if (verifyClient == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor received message from unknown client GUID " + currentClient.ClientGUID + " from " + currentClient.IpPort);
                        responseSuccess = QueueClientMessage(currentClient, _MsgBuilder.LoginRequired());
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue login required message to client " + currentClient.IpPort);
                        }
                        return responseSuccess;
                    }
                }

                #endregion
            }

            #endregion
            
            #region Authorize-Message

            if (!AuthorizeMessage(currentMessage))
            {
                if (String.IsNullOrEmpty(currentMessage.Command))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to authenticate or authorize message from " + currentMessage.Email + " " + currentMessage.SenderGUID);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to authenticate or authorize message of type " + currentMessage.Command + " from " + currentMessage.Email + " " + currentMessage.SenderGUID);
                }

                responseMessage = _MsgBuilder.AuthorizationFailed(currentMessage);
                responseSuccess = QueueClientMessage(currentClient, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue authorization failed message to client " + currentClient.IpPort);
                }
                return responseSuccess;
            }

            #endregion

            #region Process-Administrative-Messages

            if (!String.IsNullOrEmpty(currentMessage.Command))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "MessageProcessor processing administrative message of type " + currentMessage.Command + " from client " + currentClient.IpPort);

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
                        responseMessage = _MsgBuilder.UnknownCommand(currentClient, currentMessage);
                        responseSuccess = QueueClientMessage(currentClient, responseMessage);
                        return responseSuccess;
                }
            }

            #endregion

            #region Get-Recipient-or-Channel

            if (!String.IsNullOrEmpty(currentMessage.RecipientGUID))
            {
                currentRecipient = _ConnMgr.GetClientByGUID(currentMessage.RecipientGUID);
            }
            else if (!String.IsNullOrEmpty(currentMessage.ChannelGUID))
            {
                currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            }
            else
            {
                #region Recipient-Not-Supplied

                _Logging.Log(LoggingModule.Severity.Debug, "MessageProcessor no recipient specified either by RecipientGUID or ChannelGUID");
                responseMessage = _MsgBuilder.RecipientNotFound(currentClient, currentMessage);
                responseSuccess = QueueClientMessage(currentClient, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue recipient not found message to " + currentClient.IpPort);
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
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue to recipient " + currentRecipient.ClientGUID + ", sent failure notification to sender");
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
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to send to members in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
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
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to send to subscribers in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
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
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to send to subscriber in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
                    }

                    return responseSuccess;

                    #endregion
                }
                else
                {
                    #region Unknown-Channel-Type

                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor channel " + currentChannel.ChannelGUID + " not marked as broadcast, multicast, or unicast, deleting");
                    if (!RemoveChannel(currentChannel))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to remove channel " + currentChannel.ChannelGUID);
                    }

                    return false;

                    #endregion
                }

                #endregion
            }
            else
            {
                #region Recipient-Not-Found

                _Logging.Log(LoggingModule.Severity.Debug, "MessageProcessor unable to find either recipient or channel");
                responseMessage = _MsgBuilder.RecipientNotFound(currentClient, currentMessage);
                responseSuccess = QueueClientMessage(currentClient, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue recipient not found message to client " + currentClient.IpPort);
                }
                return false;

                #endregion
            }

            #endregion 
        }

        private bool SendPrivateMessage(Client sender, Client rcpt, Message currentMessage)
        { 
            #region Check-for-Null-Values

            if (sender == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendPrivateMessage null Sender supplied");
                return false;
            }
             
            if (rcpt == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendPrivateMessage null Recipient supplied");
                return false;
            }
             
            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendPrivateMessage null message supplied");
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
                        responseMessage = _MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                        responseSuccess = QueueClientMessage(sender, responseMessage);
                    }
                    return true;
                }
                else
                {
                    responseMessage = _MsgBuilder.MessageQueueFailure(sender, currentMessage);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    return false;
                }

                #endregion
            }

            #endregion 
        }

        private bool SendChannelMembersMessage(Client sender, Channel currentChannel, Message currentMessage)
        { 
            #region Check-for-Null-Values

            if (sender == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMembersMessage null Sender supplied");
                return false;
            }
            
            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMembersMessage null channel supplied");
                return false;
            }

            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMembersMessage null message supplied");
                return false;
            }

            if (String.IsNullOrEmpty(currentMessage.ChannelName)) currentMessage.ChannelName = currentChannel.ChannelName;

            #endregion

            #region Variables

            bool responseSuccess = false;
            Message responseMessage = new Message();

            #endregion

            #region Verify-Channel-Membership

            if (!IsChannelMember(sender, currentChannel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, currentMessage, currentChannel);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMembersMessage unable to queue not channel member message to " + sender.IpPort);
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
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMembersMessage unable to queue message queue success notification to " + sender.IpPort);
                }
            }
            return true;

            #endregion 
        }

        private bool SendChannelSubscribersMessage(Client sender, Channel currentChannel, Message currentMessage)
        { 
            #region Check-for-Null-Values

            if (sender == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscribersMessage null Sender supplied");
                return false;
            }
            
            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscribersMessage null channel supplied");
                return false;
            }

            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscribersMessage null message supplied");
                return false;
            }

            if (String.IsNullOrEmpty(currentMessage.ChannelName)) currentMessage.ChannelName = currentChannel.ChannelName;

            #endregion

            #region Variables

            bool responseSuccess = false;
            Message responseMessage = new Message();

            #endregion

            #region Verify-Channel-Membership

            if (!IsChannelMember(sender, currentChannel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, currentMessage, currentChannel);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscribersMessage unable to queue not channel member message to " + sender.IpPort);
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
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscribersMessage unable to queue message queue success mesage to " + sender.IpPort);
                }
            }
            return true;

            #endregion 
        }
        
        private bool SendChannelSubscriberMessage(Client sender, Channel currentChannel, Message currentMessage)
        { 
            #region Check-for-Null-Values

            if (sender == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscriberMessage null Sender supplied");
                return false;
            }
            
            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscriberMessage null channel supplied");
                return false;
            }

            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscriberMessage null message supplied");
                return false;
            }

            if (String.IsNullOrEmpty(currentMessage.ChannelName)) currentMessage.ChannelName = currentChannel.ChannelName;

            #endregion

            #region Variables

            bool responseSuccess = false;
            Message responseMessage = new Message();

            #endregion

            #region Verify-Channel-Membership

            if (!IsChannelMember(sender, currentChannel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, currentMessage, currentChannel);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscriberMessage unable to queue not channel member message to " + sender.IpPort);
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
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, currentMessage);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscriberMessage unable to queue message queue success mesage to " + sender.IpPort);
                }
            }
            return true;

            #endregion 
        }

        private bool SendSystemMessage(Message currentMessage)
        { 
            #region Check-for-Null-Values

            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSystemMessage null message supplied");
                return false;
            }

            #endregion

            #region Create-System-Client-Object

            Client currentClient = new Client();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = _ServerGUID;
            currentClient.Name = "Server";
            currentClient.IpPort = "127.0.0.1:0";
            currentClient.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentClient.UpdatedUtc = currentClient.CreatedUtc;

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
                currentRecipient = _ConnMgr.GetClientByGUID(currentMessage.RecipientGUID);
            }
            else if (!String.IsNullOrEmpty(currentMessage.ChannelGUID))
            {
                currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            }
            else
            {
                #region Recipient-Not-Supplied

                _Logging.Log(LoggingModule.Severity.Debug, "SendSystemMessage no recipient specified either by RecipientGUID or ChannelGUID");
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
                    _Logging.Log(LoggingModule.Severity.Debug, "SendSystemMessage successfully queued message to recipient " + currentRecipient.ClientGUID);
                    return true;
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendSystemMessage unable to queue message to recipient " + currentRecipient.ClientGUID);
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
                    _Logging.Log(LoggingModule.Severity.Debug, "SendSystemMessage successfully sent message to channel " + currentChannel.ChannelGUID);
                    return true;
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendSystemMessage unable to send message to channel " + currentChannel.ChannelGUID);
                    return false;
                }

                #endregion
            }
            else
            {
                #region Recipient-Not-Found

                _Logging.Log(LoggingModule.Severity.Debug, "Unable to find either recipient or channel");
                responseMessage = _MsgBuilder.RecipientNotFound(currentClient, currentMessage);
                responseSuccess = QueueClientMessage(currentClient, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendSystemMessage unable to queue recipient not found message to " + currentClient.IpPort);
                }
                return false;

                #endregion
            }

                    #endregion
        }

        private bool SendSystemPrivateMessage(Client rcpt, Message currentMessage)
        { 
            #region Check-for-Null-Values

            if (rcpt == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSystemPrivateMessage null recipient supplied");
                return false;
            }
             
            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSystemPrivateMessage null message supplied");
                return false;
            }

            #endregion

            #region Create-System-Client-Object

            Client currentClient = new Client();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = _ServerGUID;
            currentClient.Name = "Server";
            currentClient.IpPort = "127.0.0.1:0";
            currentClient.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentClient.UpdatedUtc = currentClient.CreatedUtc;

            #endregion

            #region Variables

            Channel currentChannel = new Channel();
            bool responseSuccess = false;

            #endregion

            #region Process-Recipient-Messages

            responseSuccess = QueueClientMessage(rcpt, currentMessage.Redact());
            if (!responseSuccess)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSystemPrivateMessage unable to queue message to " + rcpt.IpPort);
            }
            return responseSuccess;

            #endregion 
        }

        private bool SendSystemChannelMessage(Channel currentChannel, Message currentMessage)
        { 
            #region Check-for-Null-Values

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSystemChannelMessage null channel supplied");
                return false;
            }

            if (currentChannel.Subscribers == null || currentChannel.Subscribers.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "SendSystemChannelMessage no subscribers in channel " + currentChannel.ChannelGUID);
                return true;
            }

            if (currentMessage == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSystemPrivateMessage null message supplied");
                return false;
            }

            if (String.IsNullOrEmpty(currentMessage.ChannelName)) currentMessage.ChannelName = currentChannel.ChannelName;

            #endregion

            #region Create-System-Client-Object

            Client currentClient = new Client();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = _ServerGUID;
            currentClient.Name = "Server";
            currentClient.IpPort = "127.0.0.1:0";
            currentClient.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentClient.UpdatedUtc = currentClient.CreatedUtc;

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

        #endregion

        #region Private-Message-Handlers

        private Message ProcessEchoMessage(Client currentClient, Message currentMessage)
        { 
            currentMessage = currentMessage.Redact();
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = null;
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = _ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            return currentMessage; 
        }

        private Message ProcessLoginMessage(Client currentClient, Message currentMessage)
        { 
            bool runClientLoginTask = false;
            bool runServerJoinNotification = false;

            try
            {
                // build response message and update client
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = _ServerGUID;
                currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                currentMessage.Success = true;

                currentClient.ClientGUID = currentMessage.RecipientGUID;
                currentClient.Email = currentMessage.Email;
                currentClient.Name = currentMessage.SenderName;

                _ConnMgr.UpdateClient(currentClient);

                // start heartbeat
                currentClient.HeartbeatTokenSource = new CancellationTokenSource();
                currentClient.HeartbeatToken = currentClient.HeartbeatTokenSource.Token;
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessLoginMessage starting heartbeat manager for " + currentClient.IpPort);
                Task.Run(() => HeartbeatManager(currentClient));

                currentMessage = currentMessage.Redact();
                runClientLoginTask = true;
                runServerJoinNotification = true;

                return currentMessage;
            }
            finally
            { 
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
            }
        }

        private Message ProcessIsClientConnectedMessage(Client currentClient, Message currentMessage)
        { 
            currentMessage = currentMessage.Redact();
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = null;
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = _ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();

            if (currentMessage.Data == null)
            {
                currentMessage.Success = false;
                currentMessage.Data = FailureData.ToBytes(ErrorTypes.BadRequest, "Data does not include client GUID", null);
            }
            else
            {
                currentMessage.Success = true;
                bool exists = _ConnMgr.ClientExists(Encoding.UTF8.GetString(currentMessage.Data));
                currentMessage.Data = SuccessData.ToBytes(null, exists);
            }

            return currentMessage; 
        }

        private Message ProcessJoinChannelMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = null;

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessJoinChannelMessage unable to find channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                return responseMessage;
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessJoinChannelMessage adding client " + currentClient.IpPort + " as member to channel " + currentChannel.ChannelGUID);
                if (!AddChannelMember(currentClient, currentChannel))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessJoinChannelMessage error while adding " + currentClient.IpPort + " " + currentClient.ClientGUID + " as member of channel " + currentChannel.ChannelGUID);
                    responseMessage = _MsgBuilder.ChannelJoinFailure(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }
                else
                {
                    responseMessage = _MsgBuilder.ChannelJoinSuccess(currentClient, currentMessage, currentChannel);
                    return responseMessage;
                }
            } 
        }

        private Message ProcessSubscribeChannelMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = null;

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessSubscribeChannelMessage unable to find channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                return responseMessage;
            }

            if (currentChannel.Broadcast == 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessSubscribeChannelMessage channel marked as broadcast, calling ProcessJoinChannelMessage");
                return ProcessJoinChannelMessage(currentClient, currentMessage);
            }
                
            #region Add-Member

            _Logging.Log(LoggingModule.Severity.Debug, "ProcessSubscribeChannelMessage adding client " + currentClient.IpPort + " as subscriber to channel " + currentChannel.ChannelGUID);
            if (!AddChannelMember(currentClient, currentChannel))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessSubscribeChannelMessage error while adding " + currentClient.IpPort + " " + currentClient.ClientGUID + " as member of channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelJoinFailure(currentClient, currentMessage, currentChannel);
                return responseMessage;
            }

            #endregion

            #region Add-Subscriber

            if (!AddChannelSubscriber(currentClient, currentChannel))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessSubscribeChannelMessage error while adding " + currentClient.IpPort + " " + currentClient.ClientGUID + " as subscriber to channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelSubscribeFailure(currentClient, currentMessage, currentChannel);
                return responseMessage;
            }

            #endregion

            #region Return

            responseMessage = _MsgBuilder.ChannelSubscribeSuccess(currentClient, currentMessage, currentChannel);
            return responseMessage;

            #endregion 
        }

        private Message ProcessLeaveChannelMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                responseMessage = _MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                return responseMessage;
            }
            else
            {
                if (String.Compare(currentClient.ClientGUID, currentChannel.OwnerGUID) == 0)
                {
                    #region Owner-Abandoning-Channel

                    if (!RemoveChannel(currentChannel))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ProcessLeaveChannelMessage unable to remove owner " + currentClient.IpPort + " from channel " + currentMessage.ChannelGUID);
                        return _MsgBuilder.ChannelLeaveFailure(currentClient, currentMessage, currentChannel);
                    }
                    else
                    {
                        return _MsgBuilder.ChannelDeleteSuccess(currentClient, currentMessage, currentChannel);
                    }

                    #endregion
                }
                else
                {
                    #region Member-Leaving-Channel

                    if (!RemoveChannelMember(currentClient, currentChannel))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ProcessLeaveChannelMessage unable to remove member " + currentClient.IpPort + " " + currentClient.ClientGUID + " from channel " + currentMessage.ChannelGUID);
                        return _MsgBuilder.ChannelLeaveFailure(currentClient, currentMessage, currentChannel);
                    }
                    else
                    {
                        if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(currentClient, currentChannel);
                        return _MsgBuilder.ChannelLeaveSuccess(currentClient, currentMessage, currentChannel);
                    }

                    #endregion
                }
            } 
        }

        private Message ProcessUnsubscribeChannelMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                responseMessage = _MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                return responseMessage;
            }
                
            if (currentChannel.Broadcast == 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessUnsubscribeChannelMessage channel marked as broadcast, calling ProcessLeaveChannelMessage");
                return ProcessLeaveChannelMessage(currentClient, currentMessage);
            }
                
            if (String.Compare(currentClient.ClientGUID, currentChannel.OwnerGUID) == 0)
            {
                #region Owner-Abandoning-Channel

                if (!RemoveChannel(currentChannel))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessUnsubscribeChannelMessage unable to remove owner " + currentClient.IpPort + " from channel " + currentMessage.ChannelGUID);
                    return _MsgBuilder.ChannelUnsubscribeFailure(currentClient, currentMessage, currentChannel);
                }
                else
                {
                    return _MsgBuilder.ChannelDeleteSuccess(currentClient, currentMessage, currentChannel);
                }

                #endregion
            }
            else
            {
                #region Subscriber-Leaving-Channel

                if (!RemoveChannelSubscriber(currentClient, currentChannel))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessUnsubscribeChannelMessage unable to remove subscrber " + currentClient.IpPort + " " + currentClient.ClientGUID + " from channel " + currentMessage.ChannelGUID);
                    return _MsgBuilder.ChannelUnsubscribeFailure(currentClient, currentMessage, currentChannel);
                }
                else
                {
                    if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(currentClient, currentChannel);
                    return _MsgBuilder.ChannelUnsubscribeSuccess(currentClient, currentMessage, currentChannel);
                }

                #endregion
            } 
        }

        private Message ProcessCreateChannelMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                Channel requestChannel = Channel.FromMessage(currentClient, currentMessage);
                if (requestChannel == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessCreateChannelMessage unable to build Channel from Message data");
                    responseMessage = _MsgBuilder.DataError(currentClient, currentMessage, "unable to create Channel from supplied message data");
                    return responseMessage;
                }
                else
                {
                    currentChannel = _ChannelMgr.GetChannelByName(requestChannel.ChannelName);
                    if (currentChannel != null)
                    {
                        responseMessage = _MsgBuilder.ChannelAlreadyExists(currentClient, currentMessage, currentChannel);
                        return responseMessage;
                    }
                    else
                    {
                        if (String.IsNullOrEmpty(requestChannel.ChannelGUID))
                        {
                            requestChannel.ChannelGUID = Guid.NewGuid().ToString();
                            _Logging.Log(LoggingModule.Severity.Debug, "ProcessCreateChannelMessage adding GUID " + requestChannel.ChannelGUID + " to request (not supplied by requestor)");
                        }

                        requestChannel.OwnerGUID = currentClient.ClientGUID;

                        if (!AddChannel(currentClient, requestChannel))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ProcessCreateChannelMessage error while adding channel " + currentChannel.ChannelGUID);
                            responseMessage = _MsgBuilder.ChannelCreateFailure(currentClient, currentMessage);
                            return responseMessage;
                        }
                        else
                        {
                            ChannelCreateEvent(currentClient, requestChannel);
                        }

                        if (!AddChannelSubscriber(currentClient, requestChannel))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ProcessCreateChannelMessage error while adding channel member " + currentClient.IpPort + " to channel " + currentChannel.ChannelGUID);
                            responseMessage = _MsgBuilder.ChannelJoinFailure(currentClient, currentMessage, currentChannel);
                            return responseMessage;
                        }

                        responseMessage = _MsgBuilder.ChannelCreateSuccess(currentClient, currentMessage, requestChannel);
                        return responseMessage;
                    }
                }
            }
            else
            {
                responseMessage = _MsgBuilder.ChannelAlreadyExists(currentClient, currentMessage, currentChannel);
                return responseMessage;
            } 
        }

        private Message ProcessDeleteChannelMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                responseMessage = _MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                return responseMessage;
            }

            if (String.Compare(currentChannel.OwnerGUID, currentClient.ClientGUID) != 0)
            {
                responseMessage = _MsgBuilder.ChannelDeleteFailure(currentClient, currentMessage, currentChannel);
                return responseMessage;
            }

            if (!RemoveChannel(currentChannel))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessDeleteChannelMessage unable to remove channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelDeleteFailure(currentClient, currentMessage, currentChannel);
            }
            else
            {
                responseMessage = _MsgBuilder.ChannelDeleteSuccess(currentClient, currentMessage, currentChannel);
                ChannelDestroyEvent(currentClient, currentChannel);
            }

            return responseMessage; 
        }

        private Message ProcessListChannelsMessage(Client currentClient, Message currentMessage)
        { 
            List<Channel> ret = new List<Channel>();
            List<Channel> filtered = new List<Channel>();
            Channel currentChannel = new Channel();

            ret = _ChannelMgr.GetChannels();
            if (ret == null || ret.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListChannelsMessage no channels retrieved");

                currentMessage = currentMessage.Redact();
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = _ServerGUID;
                currentMessage.ChannelGUID = null;
                currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                currentMessage.Success = true;
                currentMessage.Data = SuccessData.ToBytes(null, new List<Channel>());
                return currentMessage;
            }
            else
            {
                foreach (Channel curr in ret)
                {
                    currentChannel = new Channel();
                    currentChannel.Subscribers = null;
                    currentChannel.ChannelGUID = curr.ChannelGUID;
                    currentChannel.ChannelName = curr.ChannelName;
                    currentChannel.OwnerGUID = curr.OwnerGUID;
                    currentChannel.CreatedUtc = curr.CreatedUtc;
                    currentChannel.UpdatedUtc = curr.UpdatedUtc;
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
            }
                
            currentMessage = currentMessage.Redact();
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = null;
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = _ServerGUID;
            currentMessage.ChannelGUID = null;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.Data = SuccessData.ToBytes(null, filtered);
            return currentMessage; 
        }

        private Message ProcessListChannelMembersMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = new Message();
            List<Client> clients = new List<Client>();
            List<Client> ret = new List<Client>();

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListChannelMembersMessage null channel after retrieval by GUID");
                responseMessage = _MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                return responseMessage;
            }

            clients = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (clients == null || clients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessListChannelMembersMessage channel " + currentChannel.ChannelGUID + " has no members");
                responseMessage = _MsgBuilder.ChannelNoMembers(currentClient, currentMessage, currentChannel);
                return responseMessage;
            }
            else
            {
                foreach (Client curr in clients)
                {
                    Client temp = new Client();
                    temp.Password = null;
                         
                    temp.Email = curr.Email;
                    temp.ClientGUID = curr.ClientGUID;
                    temp.CreatedUtc = curr.CreatedUtc;
                    temp.UpdatedUtc = curr.UpdatedUtc;
                    temp.IpPort = curr.IpPort;
                    temp.IsTcp = curr.IsTcp;
                    temp.IsWebsocket = curr.IsWebsocket;
                    temp.IsSsl = curr.IsSsl;
                    
                    ret.Add(temp);
                }
                 
                currentMessage = currentMessage.Redact();
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = _ServerGUID;
                currentMessage.ChannelGUID = currentChannel.ChannelGUID;
                currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                currentMessage.Success = true;
                currentMessage.Data = SuccessData.ToBytes(null, ret);
                return currentMessage;
            } 
        }

        private Message ProcessListChannelSubscribersMessage(Client currentClient, Message currentMessage)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(currentMessage.ChannelGUID);
            Message responseMessage = new Message();
            List<Client> clients = new List<Client>();
            List<Client> ret = new List<Client>();

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListChannelSubscribersMessage null channel after retrieval by GUID");
                responseMessage = _MsgBuilder.ChannelNotFound(currentClient, currentMessage);
                return responseMessage;
            }

            if (currentChannel.Broadcast == 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessListChannelSubscribersMessage channel is broadcast, calling ProcessListChannelMembers");
                return ProcessListChannelMembersMessage(currentClient, currentMessage);
            }

            clients = _ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
            if (clients == null || clients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessListChannelSubscribersMessage channel " + currentChannel.ChannelGUID + " has no subscribers");
                responseMessage = _MsgBuilder.ChannelNoSubscribers(currentClient, currentMessage, currentChannel);
                return responseMessage;
            }
            else
            { 
                foreach (Client curr in clients)
                {
                    Client temp = new Client();
                    temp.Password = null;
                         
                    temp.Email = curr.Email;
                    temp.ClientGUID = curr.ClientGUID;
                    temp.CreatedUtc = curr.CreatedUtc;
                    temp.UpdatedUtc = curr.UpdatedUtc;
                    temp.IpPort = curr.IpPort;
                    temp.IsTcp = curr.IsTcp;
                    temp.IsWebsocket = curr.IsWebsocket;
                    temp.IsSsl = curr.IsSsl;
                    
                    ret.Add(temp);
                }
                 
                currentMessage = currentMessage.Redact();
                currentMessage.SyncResponse = currentMessage.SyncRequest;
                currentMessage.SyncRequest = null;
                currentMessage.RecipientGUID = currentMessage.SenderGUID;
                currentMessage.SenderGUID = _ServerGUID;
                currentMessage.ChannelGUID = currentChannel.ChannelGUID;
                currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
                currentMessage.Success = true;
                currentMessage.Data = Encoding.UTF8.GetBytes(Helper.SerializeJson(ret));
                return currentMessage;
            } 
        }

        private Message ProcessListClientsMessage(Client currentClient, Message currentMessage)
        { 
            List<Client> clients = new List<Client>();
            List<Client> ret = new List<Client>();

            clients = _ConnMgr.GetClients();
            if (clients == null || clients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListClientsMessage no clients retrieved");
                return null;
            }
            else
            { 
                foreach (Client curr in clients)
                {
                    Client temp = new Client();
                    temp.IsTcp = curr.IsTcp;
                    temp.IpPort = curr.IpPort;
                    temp.IsWebsocket = curr.IsWebsocket;
                    temp.IsSsl = curr.IsSsl;
                     
                    temp.Email = curr.Email;
                    temp.Name = curr.Name;
                    temp.Password = null;
                    temp.ClientGUID = curr.ClientGUID;
                    temp.CreatedUtc = curr.CreatedUtc;
                    temp.UpdatedUtc = curr.UpdatedUtc;
                    
                    ret.Add(temp);
                } 
            }

            currentMessage = currentMessage.Redact();
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = null;
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = _ServerGUID;
            currentMessage.ChannelGUID = null;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.Data = SuccessData.ToBytes(null, ret);
            return currentMessage; 
        }

        #endregion
    }
}
