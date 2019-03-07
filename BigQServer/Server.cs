using BigQ.Core;
using BigQ.Server.Managers;
using SyslogLogging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WatsonTcp;
using WatsonWebsocket;

namespace BigQ.Server
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

        /// <summary>
        /// Callback methods used when certain events occur.
        /// </summary>
        public ServerCallbacks Callbacks;

        #endregion

        #region Private-Members

        //
        // configuration
        //
        private DateTime _CreatedUtc;
        private Random _Random; 

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
        private PersistenceManager _PersistenceMgr;
        private ConcurrentDictionary<string, DateTime> _ClientActiveSendMap;     // Receiver GUID, AddedUTC
        private AuthManager _AuthMgr;

        //
        // Server variables
        //
        private WatsonTcpServer _WTcpServer;
        private WatsonTcpSslServer _WTcpSslServer;
        private WatsonWsServer _WWsServer;
        private WatsonWsServer _WWsSslServer;
         
        //
        // cleanup
        //
        private CancellationTokenSource _CleanupCancellationTokenSource;
        private CancellationToken _CleanupCancellationToken;

        #endregion
         
        #region Constructors-and-Factories

        /// <summary>
        /// Start an instance of the BigQ server process with a default configuration.
        /// </summary>
        public Server()
        {
            _CreatedUtc = DateTime.Now.ToUniversalTime();
            _Random = new Random((int)DateTime.Now.Ticks);
            Config = ServerConfiguration.Default();
            InitializeServer();
        }

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
                Config = ServerConfiguration.Default();
            }
            else
            {
                Config = ServerConfiguration.LoadConfig(configFile);
            }

            if (Config == null) throw new Exception("Unable to initialize configuration.");

            Config.ValidateConfig();

            #endregion

            InitializeServer(); 
        }

        /// <summary>
        /// Start an instance of the BigQ server process.
        /// </summary>
        /// <param name="config">Populated server configuration object.</param>
        public Server(ServerConfiguration config)
        {
            #region Load-Config

            if (config == null) throw new ArgumentNullException(nameof(config));
            config.ValidateConfig();
            Config = config;

            #endregion

            InitializeServer();
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
        /// <returns>List of ServerClient objects.</returns>
        public List<ServerClient> ListChannelMembers(string guid)
        {
            return _ChannelMgr.GetChannelMembers(guid);
        }

        /// <summary>
        /// Retrieve list of subscribers in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of ServerClient objects.</returns>
        public List<ServerClient> ListChannelSubscribers(string guid)
        {
            return _ChannelMgr.GetChannelSubscribers(guid);
        }

        /// <summary>
        /// Retrieve list of all clients on the server.
        /// </summary>
        /// <returns>List of ServerClient objects.</returns>
        public List<ServerClient> ListClients()
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
            return _AuthMgr.GetCurrentUsersFile();
        }

        /// <summary>
        /// Retrieve all objects in the permissions configuration file defined in the server configuration file.
        /// </summary>
        /// <returns></returns>
        public List<Permission> ListCurrentPermissionsFile()
        {
            return _AuthMgr.GetCurrentPermissionsFile();
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
            newChannel.OwnerGUID = Config.GUID;
            newChannel.CreatedUtc = timestamp;
            newChannel.UpdatedUtc = timestamp;
            newChannel.Private = priv;
            newChannel.Broadcast = 1;
            newChannel.Multicast = 0;
            newChannel.Unicast = 0;
            newChannel.Members = new List<ServerClient>();
            newChannel.Subscribers = new List<ServerClient>();
            
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
            newChannel.OwnerGUID = Config.GUID;
            newChannel.CreatedUtc = timestamp;
            newChannel.UpdatedUtc = timestamp;
            newChannel.Private = priv;
            newChannel.Broadcast = 0;
            newChannel.Multicast = 0;
            newChannel.Unicast = 1;
            newChannel.Members = new List<ServerClient>();
            newChannel.Subscribers = new List<ServerClient>();

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
            newChannel.OwnerGUID = Config.GUID;
            newChannel.CreatedUtc = timestamp;
            newChannel.UpdatedUtc = timestamp;
            newChannel.Private = priv;
            newChannel.Broadcast = 0;
            newChannel.Multicast = 1;
            newChannel.Unicast = 0;
            newChannel.Members = new List<ServerClient>();
            newChannel.Subscribers = new List<ServerClient>();

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

        /// <summary>
        /// Return the number of unexpired persistent messages awaiting delivery.
        /// </summary>
        /// <returns>The number of persistent messages.</returns>
        public int PersistentQueueDepth()
        {
            if (_PersistenceMgr == null) return -1;
            return _PersistenceMgr.QueueDepth();
        }

        /// <summary>
        /// Return the number of unexpired persistent messages awaiting delivery to a recipient.
        /// </summary>
        /// <param name="guid">The GUID of the recipient.</param>
        /// <returns>The number of persistent messages.</returns>
        public int PersistentQueueDepth(string guid)
        {
            if (_PersistenceMgr == null) return -1;
            return _PersistenceMgr.QueueDepth(guid);
        }

        #endregion

        #region Private-Watson-Callback-Methods

        private bool WatsonTcpClientConnected(string ipPort)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "WTcpClientConnected new connection from " + ipPort);
            ServerClient currentClient = new ServerClient(ipPort, ConnectionType.Tcp);
            _ConnMgr.AddClient(currentClient);
            return true;
        }

        private bool WatsonTcpSslClientConnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WTcpSslClientConnected new connection from " + ipPort);
            ServerClient currentClient = new ServerClient(ipPort, ConnectionType.TcpSsl);
            _ConnMgr.AddClient(currentClient); 
            return true;
        }
         
        private bool WatsonWebsocketClientConnected(string ipPort, IDictionary<string, string> qs)
        {
            try
            {
                _Logging.Log(LoggingModule.Severity.Debug, "WWsClientConnected new connection from " + ipPort);
                ServerClient currentClient = new ServerClient(ipPort, ConnectionType.Websocket);
                _ConnMgr.AddClient(currentClient); 
                return true;
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "WWsClientConnected", e);
                return false;
            }
        }
         
        private bool WatsonWebsocketSslClientConnected(string ipPort, IDictionary<string, string> qs)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WWsSslClientConnected new connection from " + ipPort);
            ServerClient currentClient = new ServerClient(ipPort, ConnectionType.WebsocketSsl);
            _ConnMgr.AddClient(currentClient); 
            return true;
        }

        private bool WatsonClientDisconnected(string ipPort)
        {
            _Logging.Log(LoggingModule.Severity.Debug, "WTcpClientDisconnected connection termination from " + ipPort);
            DestroyClient(ipPort);
            return true;
        }

        private bool WatsonMessageReceived(string ipPort, byte[] data)
        {
            ServerClient currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(ServerClient))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "HandleMessageReceived unable to retrieve client " + ipPort);
                return false;
            }

            Message currentMessage = new Message(data);
            MessageProcessor(currentClient, currentMessage);
            if (Callbacks.MessageReceived != null) Task.Run(() => Callbacks.MessageReceived(currentMessage));
            return true;
        }

        #endregion

        #region Private-Methods

        private void InitializeServer()
        {
            #region Initialize-Globals

            _CreatedUtc = DateTime.Now.ToUniversalTime();
            _Random = new Random((int)DateTime.Now.Ticks);
            Callbacks = new ServerCallbacks();

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
                false);

            _Logging.Log(LoggingModule.Severity.Debug, "BigQ server configuration loaded");

            #endregion

            #region Set-Class-Variables
             
            _MsgBuilder = new MessageBuilder(Config.GUID);
            _ConnMgr = new ConnectionManager(_Logging, Config);
            _ChannelMgr = new ChannelManager(_Logging, Config);
            _ClientActiveSendMap = new ConcurrentDictionary<string, DateTime>();
            _AuthMgr = new AuthManager(_Logging, Config);
             
            if (Config.Persistence != null && Config.Persistence.EnablePersistence)
            {
                _PersistenceMgr = new PersistenceManager(_Logging, Config);
            }
            else
            {
                _PersistenceMgr = null;
            }

            #endregion
             
            #region Start-Cleanup-Task

            _CleanupCancellationTokenSource = new CancellationTokenSource();
            _CleanupCancellationToken = _CleanupCancellationTokenSource.Token;
            Task.Run(() => CleanupTask(), _CleanupCancellationToken);

            #endregion

            #region Start-Server-Channels

            if (Config.ServerChannels != null && Config.ServerChannels.Count > 0)
            {
                ServerClient CurrentClient = new ServerClient();
                CurrentClient.Email = null;
                CurrentClient.Password = null;
                CurrentClient.ClientGUID = Config.GUID;
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
                    WatsonTcpClientConnected,
                    WatsonClientDisconnected,
                    WatsonMessageReceived,
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
                    WatsonTcpSslClientConnected,
                    WatsonClientDisconnected,
                    WatsonMessageReceived,
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
                    WatsonWebsocketClientConnected,
                    WatsonClientDisconnected,
                    WatsonMessageReceived,
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
                    WatsonWebsocketSslClientConnected,
                    WatsonClientDisconnected,
                    WatsonMessageReceived,
                    Config.WebsocketServer.Debug);

                #endregion
            }

            _Logging.Log(LoggingModule.Severity.Debug, "BigQ server started");

            #endregion
        }
         
        private void DestroyClient(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort)) return;
            ServerClient currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(ServerClient))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "DestroyClient unable to find client " + ipPort);
                return;
            }
            else
            {
                currentClient.Dispose();
                _ConnMgr.RemoveClient(ipPort);
                return;
            }
        }
          
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_WTcpServer != null) _WTcpServer.Dispose();
                if (_WTcpSslServer != null) _WTcpSslServer.Dispose();
                if (_WWsServer != null) _WWsServer.Dispose();
                if (_WWsSslServer != null) _WWsSslServer.Dispose();
                _AuthMgr.Dispose();
                if (_CleanupCancellationTokenSource != null) _CleanupCancellationTokenSource.Cancel();
                return;
            }
        }

        #endregion
         
        #region Private-Senders-and-Queues

        private bool SendMessage(ServerClient client, Message message)
        {
            bool locked = false;

            try
            { 
                #region Wait-for-Client-Active-Send-Lock

                int addLoopCount = 0;
                while (!_ClientActiveSendMap.TryAdd(message.RecipientGUID, DateTime.Now.ToUniversalTime()))
                {
                    //
                    // wait
                    //
                    
                    Task.Delay(25).Wait();
                    addLoopCount += 25;

                    if (addLoopCount % 250 == 0)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "SendMessage locked send map attempting to add recipient GUID " + message.RecipientGUID + " for " + addLoopCount + "ms");
                    }

                    if (addLoopCount == 2500)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "SendMessage locked send map attempting to add recipient GUID " + message.RecipientGUID + " for " + addLoopCount + "ms, failing");
                        return false;
                    }
                }

                locked = true;

                #endregion

                #region Send-Message
                 
                byte[] data = message.ToBytes();
                 
                if (client.Connection == ConnectionType.Tcp) return _WTcpServer.Send(client.IpPort, data);
                else if (client.Connection == ConnectionType.TcpSsl) return _WTcpSslServer.Send(client.IpPort, data);
                else if (client.Connection == ConnectionType.Websocket) return _WWsServer.SendAsync(client.IpPort, data).Result;
                else if (client.Connection == ConnectionType.WebsocketSsl) return _WWsSslServer.SendAsync(client.IpPort, data).Result;
                else
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendMessage unable to discern transport for client " + client.IpPort);
                    return false;
                }

                #endregion
            }
            catch (Exception e)
            {
                _Logging.LogException("Server", "SendMessage " + client.IpPort, e);
                return false;
            }
            finally
            {
                #region Cleanup
                       
                if (locked)
                {
                    DateTime removedVal = DateTime.Now;
                    int removeLoopCount = 0;
                    while (!_ClientActiveSendMap.TryRemove(message.RecipientGUID, out removedVal))
                    {
                        Task.Delay(25).Wait();
                        removeLoopCount += 25;

                        if (!_ClientActiveSendMap.ContainsKey(message.RecipientGUID))
                        {
                            // there was (temporarily) a conflict that has been resolved
                            break;
                        }

                        if (removeLoopCount % 250 == 0)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "SendMessage locked send map attempting to remove recipient GUID " + message.RecipientGUID + " for " + removeLoopCount + "ms");
                        }
                    }
                }
                 
                #endregion
            }
        }

        private bool ChannelDataSender(ServerClient client, Channel channel, Message message)
        { 
            if (Common.IsTrue(channel.Broadcast))
            {
                #region Broadcast-Channel

                List<ServerClient> currChannelMembers = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
                if (currChannelMembers == null || currChannelMembers.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender no members found in channel " + channel.ChannelGUID);
                    return true;
                }

                message.SenderGUID = client.ClientGUID;
                foreach (ServerClient curr in currChannelMembers)
                {
                    Task.Run(() =>
                    {
                        message.RecipientGUID = curr.ClientGUID;
                        bool ResponseSuccess = false;
                        ResponseSuccess = QueueClientMessage(curr, message);
                        if (!ResponseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender error queuing channel message from " + message.SenderGUID + " to member " + message.RecipientGUID + " in channel " + message.ChannelGUID);
                        }
                    });
                }

                return true;

                #endregion
            }
            else if (Common.IsTrue(channel.Multicast))
            {
                #region Multicast-Channel-to-Subscribers

                List<ServerClient> currChannelSubscribers = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
                if (currChannelSubscribers == null || currChannelSubscribers.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender no subscribers found in channel " + channel.ChannelGUID);
                    return true;
                }

                message.SenderGUID = client.ClientGUID;
                foreach (ServerClient curr in currChannelSubscribers)
                {
                    Task.Run(() =>
                    {
                        message.RecipientGUID = curr.ClientGUID;
                        bool respSuccess = false;
                        respSuccess = QueueClientMessage(curr, message);
                        if (!respSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender error queuing channel message from " + message.SenderGUID + " to subscriber " + message.RecipientGUID + " in channel " + message.ChannelGUID);
                        }
                    });
                }

                return true;

                #endregion
            }
            else if (Common.IsTrue(channel.Unicast))
            {
                #region Unicast-Channel-to-Subscriber

                List<ServerClient> currChannelSubscribers = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
                if (currChannelSubscribers == null || currChannelSubscribers.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender no subscribers found in channel " + channel.ChannelGUID);
                    return true;
                }

                message.SenderGUID = client.ClientGUID;
                ServerClient recipient = currChannelSubscribers[_Random.Next(0, currChannelSubscribers.Count)];
                Task.Run(() =>
                {
                    message.RecipientGUID = recipient.ClientGUID;
                    bool respSuccess = false;
                    respSuccess = QueueClientMessage(recipient, message);
                    if (!respSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender error queuing channel message from " + message.SenderGUID + " to subscriber " + message.RecipientGUID + " in channel " + message.ChannelGUID);
                    }
                });

                return true;

                #endregion
            }
            else
            {
                #region Unknown

                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDataSender channel is not designated as broadcast, multicast, or unicast, deleting");
                return RemoveChannel(channel);

                #endregion
            }
        }

        private void StartClientQueue(ServerClient client)
        {
            client.RamQueueTokenSource = new CancellationTokenSource();
            client.RamQueueToken = client.RamQueueTokenSource.Token;
            client.RamQueueToken.ThrowIfCancellationRequested();

            client.DiskQueueTokenSource = new CancellationTokenSource();
            client.DiskQueueToken = client.DiskQueueTokenSource.Token;
            client.DiskQueueToken.ThrowIfCancellationRequested();

            _Logging.Log(LoggingModule.Severity.Debug, "StartClientQueue starting queue processors for " + client.IpPort);

            Task.Run(() => ProcessClientRamQueue(client, client.RamQueueToken), client.RamQueueToken);
            Task.Run(() => ProcessClientDiskQueue(client, client.RamQueueToken), client.DiskQueueToken);
        }

        private bool QueueClientMessage(ServerClient client, Message message)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "QueueClientMessage queuing message for client " + client.IpPort + " " + client.ClientGUID + " from " + message.SenderGUID);

            if (message.Persist)
            {
                if (!_PersistenceMgr.PersistMessage(message))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "QueueClientMessage unable to queue persistent message for client " + client.IpPort + " " + client.ClientGUID + " from " + message.SenderGUID);
                    return false;
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "QueueClientMessage persisted message for client " + client.IpPort + " " + client.ClientGUID + " from " + message.SenderGUID);
                }
            }
            else
            {
                client.MessageQueue.Add(message);
            }

            return true;
        }

        private void ProcessClientRamQueue(ServerClient client, CancellationToken token)
        {
            if (token != null) token.ThrowIfCancellationRequested();

            try
            { 
                #region Process

                while (true)
                {
                    if (token.IsCancellationRequested) break;

                    Message currMessage = client.MessageQueue.Take(client.RamQueueToken);

                    if (currMessage != null)
                    {
                        if (String.IsNullOrEmpty(currMessage.RecipientGUID))
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientRamQueue unable to deliver message " + currMessage.MessageID + " from " + currMessage.SenderGUID + " (empty recipient), discarding");
                        }
                        else
                        {
                            if (!SendMessage(client, currMessage))
                            {
                                ServerClient tempClient = _ConnMgr.GetClientByGUID(currMessage.RecipientGUID);
                                if (tempClient == null)
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientRamQueue recipient " + currMessage.RecipientGUID + " no longer exists, disposing");
                                    client.Dispose();
                                    return;
                                }
                                else
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientRamQueue unable to deliver message from " + currMessage.SenderGUID + " to " + currMessage.RecipientGUID + ", requeuing (client still exists)");
                                    client.MessageQueue.Add(currMessage);
                                }
                            }
                            else
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientRamQueue successfully sent message from " + currMessage.SenderGUID + " to " + currMessage.RecipientGUID);
                            }
                        }
                    } 
                }

                #endregion
            }
            catch (OperationCanceledException oce)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientRamQueue canceled for client " + client.IpPort + ": " + oce.Message);
                return;
            }
            catch (Exception e)
            {
                if (client != null)
                {
                    _Logging.LogException("Server", "ProcessClientRamQueue (" + client.IpPort + ")", e);
                }
                else
                {
                    _Logging.LogException("Server", "ProcessClientRamQueue (null)", e);
                }

                return;
            }
        }
         
        private void ProcessClientDiskQueue(ServerClient client, CancellationToken token)
        {
            if (Config.Persistence == null || !Config.Persistence.EnablePersistence) return;
            if (token != null) token.ThrowIfCancellationRequested();

            try
            {
                #region Process

                Dictionary<int, Message> msgs = null;

                while (true)
                {
                    if (token.IsCancellationRequested) break;

                    Task.Delay(Config.Persistence.RefreshIntervalMs).Wait();
                    
                    _PersistenceMgr.GetMessagesForRecipient(client.ClientGUID, out msgs);
                     
                    if (msgs != null && msgs.Count > 0)
                    {
                        foreach (KeyValuePair<int, Message> curr in msgs)
                        {
                            if (!SendMessage(client, curr.Value))
                            {
                                _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientDiskQueue unable to send message ID " + curr.Key + ", remaining in queue");

                                ServerClient tempClient = _ConnMgr.GetClientByGUID(client.ClientGUID);
                                if (tempClient == null)
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientDiskQueue recipient " + client.ClientGUID + " no longer exists, disposing");
                                    client.Dispose();
                                    return;
                                }
                                else
                                {
                                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessClientDiskQueue unable to deliver message from " + curr.Value.SenderGUID + " to " + client.ClientGUID);
                                    break;
                                }
                            }
                            else
                            {
                                _PersistenceMgr.ExpireMessage(curr.Key);
                                _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientDiskQueue successfully drained message ID " + curr.Key + " to client " + client.ClientGUID);
                            }
                        }
                    }
                }

                #endregion
            }
            catch (OperationCanceledException oce)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessClientDiskQueue canceled for client " + client.IpPort + ": " + oce.Message);
                return;
            }
            catch (Exception e)
            {
                if (client != null)
                {
                    _Logging.LogException("Server", "ProcessClientDiskQueue (" + client.IpPort + ")", e);
                }
                else
                {
                    _Logging.LogException("Server", "ProcessClientDiskQueue (null)", e);
                }

                return;
            }
        }

        #endregion
        
        #region Private-Event-Methods

        private bool ServerJoinEvent(ServerClient client)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "ServerJoinEvent sending server join notification for " + client.IpPort + " GUID " + client.ClientGUID);

            List<ServerClient> currentClients = _ConnMgr.GetClients(); 
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerJoinEvent no clients found on server");
                return true;
            }

            Message msg = _MsgBuilder.ServerJoinEvent(client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ServerJoinEvent error queuing server join event to " + msg.RecipientGUID + " (join by " + client.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ServerLeaveEvent(ServerClient client)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "ServerLeaveEvent sending server leave notification for " + client.IpPort + " GUID " + client.ClientGUID);

            List<ServerClient> currentClients = _ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ServerLeaveEvent no clients found on server");
                return true;
            }

            Message msg = _MsgBuilder.ServerLeaveEvent(client);

            foreach (ServerClient curr in currentClients)
            {
                if (!String.IsNullOrEmpty(curr.ClientGUID))
                {
                    if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ServerLeaveEvent error queuing server leave event to " + msg.RecipientGUID + " (leave by " + client.ClientGUID + ")");
                        }
                        else
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "ServerLeaveEvent queued server leave event to " + msg.RecipientGUID + " (leave by " + client.ClientGUID + ")");
                        }
                    }
                }
            }

            return true;
        }

        private bool ChannelJoinEvent(ServerClient client, Channel channel)
        {  
            List<ServerClient> currentClients = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent no clients found in channel " + channel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelJoinEvent(channel, client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelJoinEvent error queuing channel join event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (join by " + client.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ChannelLeaveEvent(ServerClient client, Channel channel)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "ChannelLeaveEvent sending channel leave notification for " + client.IpPort + " GUID " + client.ClientGUID + " channel " + channel.ChannelGUID);

            List<ServerClient> currentClients = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent no clients found in channel " + channel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelLeaveEvent(channel, client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ChannelLeaveEvent error queuing channel leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + client.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }

        private bool ChannelCreateEvent(ServerClient client, Channel channel)
        { 
            if (Common.IsTrue(channel.Private))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent skipping create notification for channel " + channel.ChannelGUID + " (private)");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ChannelCreateEvent sending channel create notification for " + client.IpPort + " GUID " + client.ClientGUID + " channel " + channel.ChannelGUID);

            List<ServerClient> currentClients = _ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent no clients found on server");
                return true;
            }

            foreach (ServerClient curr in currentClients)
            {
                Task.Run(() =>
                {
                    Message msg = _MsgBuilder.ChannelCreateEvent(client, channel);
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                    if (!responseSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ChannelCreateEvent error queuing channel create event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + client.ClientGUID + ")");
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
                    foreach (ServerClient currMember in currChannel.Members)
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
                    foreach (ServerClient currSubscriber in currChannel.Subscribers)
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

        private bool ChannelDestroyEvent(ServerClient client, Channel channel)
        { 
            if (Common.IsTrue(channel.Private))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent skipping destroy notification for channel " + channel.ChannelGUID + " (private)");
                return true;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "ChannelDestroyEvent sending channel destroy notification for " + client.IpPort + " GUID " + client.ClientGUID + " channel " + channel.ChannelGUID);

            List<ServerClient> currentClients = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent no clients found in channel " + channel.ChannelGUID);
                return true;
            }

            foreach (ServerClient curr in currentClients)
            {
                Task.Run(() =>
                {
                    Message msg = _MsgBuilder.ChannelDestroyEvent(client, channel);
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                    if (!responseSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ChannelDestroyEvent error queuing channel leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + client.ClientGUID + ")");
                    }
                });
            }

            return true;
        }

        private bool SubscriberJoinEvent(ServerClient client, Channel channel)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "SubscriberJoinEvent sending subcriber join notification for " + client.IpPort + " GUID " + client.ClientGUID + " channel " + channel.ChannelGUID);

            List<ServerClient> currentClients = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent no clients found in channel " + channel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelSubscriberJoinEvent(channel, client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "SubscriberJoinEvent error queuing subscriber join event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (join by " + client.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
        }
         
        private bool SubscriberLeaveEvent(ServerClient client, Channel channel)
        { 
            _Logging.Log(LoggingModule.Severity.Debug, "SubscriberLeaveEvent sending subscriber leave notification for " + client.IpPort + " GUID " + client.ClientGUID + " channel " + channel.ChannelGUID);

            List<ServerClient> currentClients = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent no clients found in channel " + channel.ChannelGUID);
                return true;
            }

            Message msg = _MsgBuilder.ChannelSubscriberLeaveEvent(channel, client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    Task.Run(() =>
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "SubscriberLeaveEvent error queuing subscriber leave event to " + msg.RecipientGUID + " for channel " + msg.ChannelGUID + " (leave by " + client.ClientGUID + ")");
                        }
                    });
                }
            }

            return true;
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
                if (Callbacks.ServerStopped != null) Callbacks.ServerStopped();
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
        
        private bool AddChannel(ServerClient client, Channel channel)
        { 
            DateTime timestamp = DateTime.Now.ToUniversalTime();
            if (channel.CreatedUtc == null) channel.CreatedUtc = timestamp;
            if (channel.UpdatedUtc == null) channel.UpdatedUtc = timestamp;
            channel.Members = new List<ServerClient>();
            channel.Members.Add(client);
            channel.Subscribers = new List<ServerClient>();
            channel.OwnerGUID = client.ClientGUID;

            if (_ChannelMgr.ChannelExists(channel.ChannelGUID))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "AddChannel channel GUID " + channel.ChannelGUID + " already exists");
                return false;
            }

            _ChannelMgr.AddChannel(channel);

            _Logging.Log(LoggingModule.Severity.Debug, "AddChannel successfully added channel with GUID " + channel.ChannelGUID + " for client " + channel.OwnerGUID);
            return true;
        }

        private bool RemoveChannel(Channel channel)
        { 
            channel = _ChannelMgr.GetChannelByGUID(channel.ChannelGUID);
            if (channel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "RemoveChannel unable to find specified channel");
                return false;
            }
                
            if (channel.OwnerGUID.Equals(Config.GUID))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel skipping removal of channel " + channel.ChannelGUID + " (server channel)");
                return true;
            }

            _ChannelMgr.RemoveChannel(channel.ChannelGUID);
            _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel notifying channel members of channel removal");

            if (channel.Members != null)
            {
                if (channel.Members.Count > 0)
                {
                    //
                    // create another reference in case list is modified
                    //
                    Channel tempChannel = channel;
                    List<ServerClient> tempMembers = new List<ServerClient>(channel.Members);

                    Task.Run(() =>
                    {
                        foreach (ServerClient currentClient in tempMembers)
                        {
                            if (String.Compare(currentClient.ClientGUID, channel.OwnerGUID) != 0)
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel notifying channel " + tempChannel.ChannelGUID + " member " + currentClient.ClientGUID + " of channel deletion by owner");
                                SendSystemMessage(_MsgBuilder.ChannelDeletedByOwner(currentClient, tempChannel));
                            }
                        }
                    }
                    );
                }
            }

            _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel removed channel " + channel.ChannelGUID + " successfully");
            return true;
        }

        private bool AddChannelMember(ServerClient client, Channel channel)
        {
            if (_ChannelMgr.AddChannelMember(channel, client))
            {
                if (Config.Notification.ChannelJoinNotification)
                {
                    ChannelJoinEvent(client, channel);
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private bool AddChannelSubscriber(ServerClient client, Channel channel)
        {
            if (_ChannelMgr.AddChannelSubscriber(channel, client))
            {
                if (Config.Notification.ChannelJoinNotification)
                {
                    SubscriberJoinEvent(client, channel);
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private bool RemoveChannelMember(ServerClient client, Channel channel)
        {
            if (_ChannelMgr.RemoveChannelMember(channel, client))
            {
                #region Send-Notifications

                if (Config.Notification.ChannelJoinNotification)
                {
                    List<ServerClient> curr = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
                    if (curr != null && channel.Members != null && channel.Members.Count > 0)
                    {
                        foreach (ServerClient c in curr)
                        {
                            //
                            // create another reference in case list is modified
                            //
                            Channel TempChannel = channel;
                            Task.Run(() =>
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelMember notifying channel " + TempChannel.ChannelGUID + " member " + c.ClientGUID + " of channel leave by member " + client.ClientGUID);
                                SendSystemMessage(_MsgBuilder.ChannelLeaveEvent(TempChannel, client));
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

        private bool RemoveChannelSubscriber(ServerClient client, Channel channel)
        {
            if (_ChannelMgr.RemoveChannelMember(channel, client))
            {
                #region Send-Notifications

                if (Config.Notification.ChannelJoinNotification)
                {
                    List<ServerClient> curr = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
                    if (curr != null && channel.Members != null && channel.Members.Count > 0)
                    {
                        foreach (ServerClient c in curr)
                        {
                            //
                            // create another reference in case list is modified
                            //
                            Channel tempChannel = channel;
                            Task.Run(() =>
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelSubscriber notifying channel " + tempChannel.ChannelGUID + " member " + c.ClientGUID + " of channel leave by subscriber " + client.ClientGUID);
                                SendSystemMessage(_MsgBuilder.ChannelSubscriberLeaveEvent(tempChannel, client));
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

        private bool IsChannelMember(ServerClient client, Channel channel)
        {
            return _ChannelMgr.IsChannelMember(client, channel);
        }

        private bool IsChannelSubscriber(ServerClient client, Channel channel)
        {
            return _ChannelMgr.IsChannelSubscriber(client, channel);
        }

        #endregion

        #region Private-Message-Processing-Methods
         
        private bool MessageProcessor(ServerClient client, Message message)
        { 
            #region Variables-and-Initialization

            ServerClient currentRecipient = null;
            Channel currentChannel = null;
            Message responseMessage = new Message();
            bool responseSuccess = false;
            message.Success = null;

            #endregion

            #region Verify-Client-GUID-Present

            if (String.IsNullOrEmpty(client.ClientGUID))
            {
                if (message.Command != MessageCommand.Login)
                { 
                    #region Null-GUID-and-Not-Login

                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor received message from client with no GUID");
                    responseSuccess = QueueClientMessage(client, _MsgBuilder.LoginRequired());
                    if (!responseSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue login required message to client " + client.IpPort);
                    }
                    return responseSuccess;

                    #endregion 
                }
            }
            else
            {
                #region Ensure-GUID-Exists

                if (String.Compare(client.ClientGUID, Config.GUID) != 0)
                {
                    ServerClient verifyClient = _ConnMgr.GetClientByGUID(client.ClientGUID);
                    if (verifyClient == null)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor received message from unknown client GUID " + client.ClientGUID + " from " + client.IpPort);
                        responseSuccess = QueueClientMessage(client, _MsgBuilder.LoginRequired());
                        if (!responseSuccess)
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue login required message to client " + client.IpPort);
                        }
                        return responseSuccess;
                    }
                }

                #endregion
            }

            #endregion
            
            #region Authorize-Message

            if (!_AuthMgr.AuthorizeMessage(message))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to authenticate or authorize message of type " + message.Command + " from " + message.Email + " " + message.SenderGUID);
                responseMessage = _MsgBuilder.AuthorizationFailed(message);
                responseSuccess = QueueClientMessage(client, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue authorization failed message to client " + client.IpPort);
                }
                return responseSuccess;
            }

            #endregion

            #region Check-Persistence

            if (message.Persist)
            {
                if (Config.Persistence == null || !Config.Persistence.EnablePersistence)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor message from " + client.ClientGUID + " requested persistence but persistence not enabled");
                    return false;
                }
            }

            #endregion

            #region Process-Administrative-Messages
             
            _Logging.Log(LoggingModule.Severity.Debug, "MessageProcessor processing administrative message of type " + message.Command + " from client " + client.IpPort);

            switch (message.Command)
            {
                case MessageCommand.Echo:
                    responseMessage = ProcessEchoMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.Login:
                    responseMessage = ProcessLoginMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.HeartbeatRequest:
                    // no need to send response
                    return true;

                case MessageCommand.JoinChannel:
                    responseMessage = ProcessJoinChannelMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.LeaveChannel:
                    responseMessage = ProcessLeaveChannelMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.SubscribeChannel:
                    responseMessage = ProcessSubscribeChannelMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.UnsubscribeChannel:
                    responseMessage = ProcessUnsubscribeChannelMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.CreateChannel:
                    responseMessage = ProcessCreateChannelMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.DeleteChannel:
                    responseMessage = ProcessDeleteChannelMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.ListChannels:
                    responseMessage = ProcessListChannelsMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.ListChannelMembers:
                    responseMessage = ProcessListChannelMembersMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.ListChannelSubscribers:
                    responseMessage = ProcessListChannelSubscribersMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.ListClients:
                    responseMessage = ProcessListClientsMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                case MessageCommand.IsClientConnected:
                    responseMessage = ProcessIsClientConnectedMessage(client, message);
                    responseSuccess = QueueClientMessage(client, responseMessage);
                    return responseSuccess;

                default:
                    // Fall through, likely a recipient or channel message
                    break;
            }

            #endregion

            #region Get-Recipient-or-Channel

            if (!String.IsNullOrEmpty(message.RecipientGUID))
            {
                currentRecipient = _ConnMgr.GetClientByGUID(message.RecipientGUID);
            }
            else if (!String.IsNullOrEmpty(message.ChannelGUID))
            {
                currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            }
            else
            {
                #region Recipient-Not-Supplied

                _Logging.Log(LoggingModule.Severity.Debug, "MessageProcessor no recipient specified either by RecipientGUID or ChannelGUID");
                responseMessage = _MsgBuilder.RecipientNotFound(client, message);
                responseSuccess = QueueClientMessage(client, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue recipient not found message to " + client.IpPort);
                }
                return false;

                #endregion
            }

            #endregion

            #region Process-Recipient-Messages

            if (currentRecipient != null)
            {
                #region Send-to-Recipient
                 
                responseSuccess = QueueClientMessage(currentRecipient, message);
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

                if (Common.IsTrue(currentChannel.Broadcast))
                {
                    #region Broadcast-Message

                    responseSuccess = SendChannelMembersMessage(client, currentChannel, message);
                    if (!responseSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to send to members in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
                    }

                    return responseSuccess;

                    #endregion
                }
                else if (Common.IsTrue(currentChannel.Multicast))
                {
                    #region Multicast-Message-to-Subscribers

                    responseSuccess = SendChannelSubscribersMessage(client, currentChannel, message);
                    if (!responseSuccess)
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to send to subscribers in channel " + currentChannel.ChannelGUID + ", sent failure notification to sender");
                    }

                    return responseSuccess;

                    #endregion
                }
                else if (Common.IsTrue(currentChannel.Unicast))
                {
                    #region Unicast-Message-to-One-Subscriber

                    responseSuccess = SendChannelSubscriberMessage(client, currentChannel, message);
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
                responseMessage = _MsgBuilder.RecipientNotFound(client, message);
                responseSuccess = QueueClientMessage(client, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "MessageProcessor unable to queue recipient not found message to client " + client.IpPort);
                }
                return false;

                #endregion
            }

            #endregion 
        }

        private bool SendPrivateMessage(ServerClient sender, ServerClient rcpt, Message message)
        {  
            bool responseSuccess = false;
            Message responseMessage = new Message();
             
            responseSuccess = QueueClientMessage(rcpt, message.Redact());
             
            #region Send-Success-or-Failure-to-Sender

            if (message.SyncRequest != null && Convert.ToBoolean(message.SyncRequest))
            {
                #region Sync-Request

                //
                // do not send notifications for success/fail on a sync message
                //

                return true;

                #endregion
            }
            else if (message.SyncRequest != null && Convert.ToBoolean(message.SyncResponse))
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
                        responseMessage = _MsgBuilder.MessageQueueSuccess(sender, message);
                        responseSuccess = QueueClientMessage(sender, responseMessage);
                    }
                    return true;
                }
                else
                {
                    responseMessage = _MsgBuilder.MessageQueueFailure(sender, message);
                    responseSuccess = QueueClientMessage(sender, responseMessage);
                    return false;
                }

                #endregion
            }

            #endregion 
        }

        private bool SendChannelMembersMessage(ServerClient sender, Channel channel, Message message)
        { 
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
             
            #region Variables

            bool responseSuccess = false;
            Message responseMessage = new Message();

            #endregion

            #region Verify-Channel-Membership

            if (!IsChannelMember(sender, channel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, message, channel);
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
                responseSuccess = ChannelDataSender(sender, channel, message.Redact());
            });

            if (Config.Notification.MsgAcknowledgement)
            {
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, message);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelMembersMessage unable to queue message queue success notification to " + sender.IpPort);
                }
            }
            return true;

            #endregion 
        }

        private bool SendChannelSubscribersMessage(ServerClient sender, Channel channel, Message message)
        {  
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
             
            #region Variables

            bool responseSuccess = false;
            Message responseMessage = new Message();

            #endregion

            #region Verify-Channel-Membership

            if (!IsChannelMember(sender, channel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, message, channel);
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
                responseSuccess = ChannelDataSender(sender, channel, message.Redact());
            });

            if (Config.Notification.MsgAcknowledgement)
            {
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, message);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscribersMessage unable to queue message queue success mesage to " + sender.IpPort);
                }
            }
            return true;

            #endregion 
        }
        
        private bool SendChannelSubscriberMessage(ServerClient sender, Channel channel, Message message)
        {  
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
             
            #region Variables

            bool responseSuccess = false;
            Message responseMessage = new Message();

            #endregion

            #region Verify-Channel-Membership

            if (!IsChannelMember(sender, channel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, message, channel);
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
                responseSuccess = ChannelDataSender(sender, channel, message.Redact());
            });

            if (Config.Notification.MsgAcknowledgement)
            {
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, message);
                responseSuccess = QueueClientMessage(sender, responseMessage);
                if (!responseSuccess)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "SendChannelSubscriberMessage unable to queue message queue success mesage to " + sender.IpPort);
                }
            }
            return true;

            #endregion 
        }

        private bool SendSystemMessage(Message message)
        {  
            #region Create-System-Client-Object

            ServerClient currentClient = new ServerClient();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = Config.GUID;
            currentClient.Name = "Server";
            currentClient.IpPort = "127.0.0.1:0";
            currentClient.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentClient.UpdatedUtc = currentClient.CreatedUtc;

            #endregion

            #region Variables

            ServerClient currentRecipient = new ServerClient();
            Channel currentChannel = new Channel();
            Message responseMessage = new Message();
            bool responseSuccess = false;

            #endregion

            #region Get-Recipient-or-Channel

            if (!String.IsNullOrEmpty(message.RecipientGUID))
            {
                currentRecipient = _ConnMgr.GetClientByGUID(message.RecipientGUID);
            }
            else if (!String.IsNullOrEmpty(message.ChannelGUID))
            {
                currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
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

                responseSuccess = QueueClientMessage(currentRecipient, message.Redact());
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

                responseSuccess = ChannelDataSender(currentClient, currentChannel, message.Redact());
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
                responseMessage = _MsgBuilder.RecipientNotFound(currentClient, message);
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

        private bool SendSystemPrivateMessage(ServerClient rcpt, Message message)
        {
            #region Create-System-Client-Object

            ServerClient currentClient = new ServerClient();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = Config.GUID;
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

            responseSuccess = QueueClientMessage(rcpt, message.Redact());
            if (!responseSuccess)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "SendSystemPrivateMessage unable to queue message to " + rcpt.IpPort);
            }
            return responseSuccess;

            #endregion 
        }

        private bool SendSystemChannelMessage(Channel channel, Message message)
        {  
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
             
            #region Create-System-Client-Object

            ServerClient currentClient = new ServerClient();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = Config.GUID;
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
            channel.Broadcast = 1;
            channel.Multicast = 0;                

            #endregion
                
            #region Send-to-Channel

            bool responseSuccess = ChannelDataSender(currentClient, channel, message);
            return responseSuccess;

            #endregion 
        }

        #endregion

        #region Private-Message-Handlers

        private Message ProcessEchoMessage(ServerClient client, Message message)
        { 
            message = message.Redact();
            message.SyncResponse = message.SyncRequest;
            message.SyncRequest = null;
            message.RecipientGUID = message.SenderGUID;
            message.SenderGUID = Config.GUID;
            message.CreatedUtc = DateTime.Now.ToUniversalTime();
            message.Success = true;
            return message; 
        }

        private Message ProcessLoginMessage(ServerClient client, Message message)
        { 
            bool runClientLoginTask = false;
            bool runServerJoinNotification = false;

            try
            {
                // build response message and update client
                message.SyncResponse = message.SyncRequest;
                message.SyncRequest = null;
                message.RecipientGUID = message.SenderGUID;
                message.SenderGUID = Config.GUID;
                message.CreatedUtc = DateTime.Now.ToUniversalTime();
                message.Success = true;

                client.ClientGUID = message.RecipientGUID;
                client.Email = message.Email;
                client.Name = message.SenderName;

                _ConnMgr.UpdateClient(client);
                 
                message = message.Redact();
                runClientLoginTask = true;
                runServerJoinNotification = true;

                StartClientQueue(client);

                return message;
            }
            finally
            { 
                if (runClientLoginTask)
                {
                    if (Callbacks.ClientLogin != null)
                    {
                        Task.Run(() => Callbacks.ClientLogin(client));
                    }
                }

                if (runServerJoinNotification)
                {
                    if (Config.Notification.ServerJoinNotification)
                    {
                        Task.Run(() => ServerJoinEvent(client));
                    }
                } 
            }
        }

        private Message ProcessIsClientConnectedMessage(ServerClient client, Message message)
        { 
            message = message.Redact();
            message.SyncResponse = message.SyncRequest;
            message.SyncRequest = null;
            message.RecipientGUID = message.SenderGUID;
            message.SenderGUID = Config.GUID;
            message.CreatedUtc = DateTime.Now.ToUniversalTime();

            if (message.Data == null)
            {
                message.Success = false;
                message.Data = FailureData.ToBytes(ErrorTypes.BadRequest, "Data does not include client GUID", null);
            }
            else
            {
                message.Success = true;
                bool exists = _ConnMgr.ClientExists(Encoding.UTF8.GetString(message.Data));
                message.Data = SuccessData.ToBytes(null, exists);
            }

            return message; 
        }

        private Message ProcessJoinChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = null;

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessJoinChannelMessage unable to find channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelNotFound(client, message);
                return responseMessage;
            }
            else
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessJoinChannelMessage adding client " + client.IpPort + " as member to channel " + currentChannel.ChannelGUID);
                if (!AddChannelMember(client, currentChannel))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessJoinChannelMessage error while adding " + client.IpPort + " " + client.ClientGUID + " as member of channel " + currentChannel.ChannelGUID);
                    responseMessage = _MsgBuilder.ChannelJoinFailure(client, message, currentChannel);
                    return responseMessage;
                }
                else
                {
                    responseMessage = _MsgBuilder.ChannelJoinSuccess(client, message, currentChannel);
                    return responseMessage;
                }
            } 
        }

        private Message ProcessSubscribeChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = null;

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessSubscribeChannelMessage unable to find channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelNotFound(client, message);
                return responseMessage;
            }

            if (currentChannel.Broadcast == 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessSubscribeChannelMessage channel marked as broadcast, calling ProcessJoinChannelMessage");
                return ProcessJoinChannelMessage(client, message);
            }
                
            #region Add-Member

            _Logging.Log(LoggingModule.Severity.Debug, "ProcessSubscribeChannelMessage adding client " + client.IpPort + " as subscriber to channel " + currentChannel.ChannelGUID);
            if (!AddChannelMember(client, currentChannel))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessSubscribeChannelMessage error while adding " + client.IpPort + " " + client.ClientGUID + " as member of channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelJoinFailure(client, message, currentChannel);
                return responseMessage;
            }

            #endregion

            #region Add-Subscriber

            if (!AddChannelSubscriber(client, currentChannel))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessSubscribeChannelMessage error while adding " + client.IpPort + " " + client.ClientGUID + " as subscriber to channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelSubscribeFailure(client, message, currentChannel);
                return responseMessage;
            }

            #endregion

            #region Return

            responseMessage = _MsgBuilder.ChannelSubscribeSuccess(client, message, currentChannel);
            return responseMessage;

            #endregion 
        }

        private Message ProcessLeaveChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                responseMessage = _MsgBuilder.ChannelNotFound(client, message);
                return responseMessage;
            }
            else
            {
                if (client.ClientGUID.Equals(currentChannel.OwnerGUID)) 
                {
                    #region Owner-Abandoning-Channel

                    if (!RemoveChannel(currentChannel))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ProcessLeaveChannelMessage unable to remove owner " + client.IpPort + " from channel " + message.ChannelGUID);
                        return _MsgBuilder.ChannelLeaveFailure(client, message, currentChannel);
                    }
                    else
                    {
                        return _MsgBuilder.ChannelDeleteSuccess(client, message, currentChannel);
                    }

                    #endregion
                }
                else
                {
                    #region Member-Leaving-Channel

                    if (!RemoveChannelMember(client, currentChannel))
                    {
                        _Logging.Log(LoggingModule.Severity.Warn, "ProcessLeaveChannelMessage unable to remove member " + client.IpPort + " " + client.ClientGUID + " from channel " + message.ChannelGUID);
                        return _MsgBuilder.ChannelLeaveFailure(client, message, currentChannel);
                    }
                    else
                    {
                        if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(client, currentChannel);
                        return _MsgBuilder.ChannelLeaveSuccess(client, message, currentChannel);
                    }

                    #endregion
                }
            } 
        }

        private Message ProcessUnsubscribeChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                responseMessage = _MsgBuilder.ChannelNotFound(client, message);
                return responseMessage;
            }
                
            if (currentChannel.Broadcast == 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessUnsubscribeChannelMessage channel marked as broadcast, calling ProcessLeaveChannelMessage");
                return ProcessLeaveChannelMessage(client, message);
            }
                
            if (client.ClientGUID.Equals(currentChannel.OwnerGUID))
            {
                #region Owner-Abandoning-Channel

                if (!RemoveChannel(currentChannel))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessUnsubscribeChannelMessage unable to remove owner " + client.IpPort + " from channel " + message.ChannelGUID);
                    return _MsgBuilder.ChannelUnsubscribeFailure(client, message, currentChannel);
                }
                else
                {
                    return _MsgBuilder.ChannelDeleteSuccess(client, message, currentChannel);
                }

                #endregion
            }
            else
            {
                #region Subscriber-Leaving-Channel

                if (!RemoveChannelSubscriber(client, currentChannel))
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessUnsubscribeChannelMessage unable to remove subscrber " + client.IpPort + " " + client.ClientGUID + " from channel " + message.ChannelGUID);
                    return _MsgBuilder.ChannelUnsubscribeFailure(client, message, currentChannel);
                }
                else
                {
                    if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(client, currentChannel);
                    return _MsgBuilder.ChannelUnsubscribeSuccess(client, message, currentChannel);
                }

                #endregion
            } 
        }

        private Message ProcessCreateChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                Channel requestChannel = Channel.FromMessage(client, message);
                if (requestChannel == null)
                {
                    _Logging.Log(LoggingModule.Severity.Warn, "ProcessCreateChannelMessage unable to build Channel from Message data");
                    responseMessage = _MsgBuilder.DataError(client, message, "unable to create Channel from supplied message data");
                    return responseMessage;
                }
                else
                {
                    currentChannel = _ChannelMgr.GetChannelByName(requestChannel.ChannelName);
                    if (currentChannel != null)
                    {
                        responseMessage = _MsgBuilder.ChannelAlreadyExists(client, message, currentChannel);
                        return responseMessage;
                    }
                    else
                    {
                        if (String.IsNullOrEmpty(requestChannel.ChannelGUID))
                        {
                            requestChannel.ChannelGUID = Guid.NewGuid().ToString();
                            _Logging.Log(LoggingModule.Severity.Debug, "ProcessCreateChannelMessage adding GUID " + requestChannel.ChannelGUID + " to request (not supplied by requestor)");
                        }

                        requestChannel.OwnerGUID = client.ClientGUID;

                        if (!AddChannel(client, requestChannel))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ProcessCreateChannelMessage error while adding channel " + currentChannel.ChannelGUID);
                            responseMessage = _MsgBuilder.ChannelCreateFailure(client, message);
                            return responseMessage;
                        }
                        else
                        {
                            ChannelCreateEvent(client, requestChannel);
                        }

                        if (!AddChannelSubscriber(client, requestChannel))
                        {
                            _Logging.Log(LoggingModule.Severity.Warn, "ProcessCreateChannelMessage error while adding channel member " + client.IpPort + " to channel " + currentChannel.ChannelGUID);
                            responseMessage = _MsgBuilder.ChannelJoinFailure(client, message, currentChannel);
                            return responseMessage;
                        }

                        responseMessage = _MsgBuilder.ChannelCreateSuccess(client, message, requestChannel);
                        return responseMessage;
                    }
                }
            }
            else
            {
                responseMessage = _MsgBuilder.ChannelAlreadyExists(client, message, currentChannel);
                return responseMessage;
            } 
        }

        private Message ProcessDeleteChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = new Message();

            if (currentChannel == null)
            {
                responseMessage = _MsgBuilder.ChannelNotFound(client, message);
                return responseMessage;
            }

            if (String.Compare(currentChannel.OwnerGUID, client.ClientGUID) != 0)
            {
                responseMessage = _MsgBuilder.ChannelDeleteFailure(client, message, currentChannel);
                return responseMessage;
            }

            if (!RemoveChannel(currentChannel))
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessDeleteChannelMessage unable to remove channel " + currentChannel.ChannelGUID);
                responseMessage = _MsgBuilder.ChannelDeleteFailure(client, message, currentChannel);
            }
            else
            {
                responseMessage = _MsgBuilder.ChannelDeleteSuccess(client, message, currentChannel);
                ChannelDestroyEvent(client, currentChannel);
            }

            return responseMessage; 
        }

        private Message ProcessListChannelsMessage(ServerClient client, Message message)
        { 
            List<Channel> ret = new List<Channel>();
            List<Channel> filtered = new List<Channel>();
            Channel currentChannel = new Channel();

            ret = _ChannelMgr.GetChannels();
            if (ret == null || ret.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListChannelsMessage no channels retrieved");

                message = message.Redact();
                message.SyncResponse = message.SyncRequest;
                message.SyncRequest = null;
                message.RecipientGUID = message.SenderGUID;
                message.SenderGUID = Config.GUID;
                message.ChannelGUID = null;
                message.CreatedUtc = DateTime.Now.ToUniversalTime();
                message.Success = true;
                message.Data = SuccessData.ToBytes(null, new List<Channel>());
                return message;
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

                    if (currentChannel.OwnerGUID.Equals(client.ClientGUID))
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
                
            message = message.Redact();
            message.SyncResponse = message.SyncRequest;
            message.SyncRequest = null;
            message.RecipientGUID = message.SenderGUID;
            message.SenderGUID = Config.GUID;
            message.ChannelGUID = null;
            message.CreatedUtc = DateTime.Now.ToUniversalTime();
            message.Success = true;
            message.Data = SuccessData.ToBytes(null, filtered);
            return message; 
        }

        private Message ProcessListChannelMembersMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = new Message();
            List<ServerClient> clients = new List<ServerClient>();
            List<ServerClient> ret = new List<ServerClient>();

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListChannelMembersMessage null channel after retrieval by GUID");
                responseMessage = _MsgBuilder.ChannelNotFound(client, message);
                return responseMessage;
            }

            clients = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (clients == null || clients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessListChannelMembersMessage channel " + currentChannel.ChannelGUID + " has no members");
                responseMessage = _MsgBuilder.ChannelNoMembers(client, message, currentChannel);
                return responseMessage;
            }
            else
            {
                foreach (ServerClient curr in clients)
                {
                    ServerClient temp = new ServerClient();
                    temp.Password = null;
                    temp.Name = curr.Name;
                    temp.Email = curr.Email;
                    temp.ClientGUID = curr.ClientGUID;
                    temp.CreatedUtc = curr.CreatedUtc;
                    temp.UpdatedUtc = curr.UpdatedUtc;
                    temp.IpPort = curr.IpPort;
                    temp.Connection = curr.Connection; 
                    
                    ret.Add(temp);
                }
                 
                message = message.Redact();
                message.SyncResponse = message.SyncRequest;
                message.SyncRequest = null;
                message.RecipientGUID = message.SenderGUID;
                message.SenderGUID = Config.GUID;
                message.ChannelGUID = currentChannel.ChannelGUID;
                message.CreatedUtc = DateTime.Now.ToUniversalTime();
                message.Success = true;
                message.Data = SuccessData.ToBytes(null, ret);
                return message;
            } 
        }

        private Message ProcessListChannelSubscribersMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            Message responseMessage = new Message();
            List<ServerClient> clients = new List<ServerClient>();
            List<ServerClient> ret = new List<ServerClient>();

            if (currentChannel == null)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListChannelSubscribersMessage null channel after retrieval by GUID");
                responseMessage = _MsgBuilder.ChannelNotFound(client, message);
                return responseMessage;
            }

            if (currentChannel.Broadcast == 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessListChannelSubscribersMessage channel is broadcast, calling ProcessListChannelMembers");
                return ProcessListChannelMembersMessage(client, message);
            }

            clients = _ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
            if (clients == null || clients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ProcessListChannelSubscribersMessage channel " + currentChannel.ChannelGUID + " has no subscribers");
                responseMessage = _MsgBuilder.ChannelNoSubscribers(client, message, currentChannel);
                return responseMessage;
            }
            else
            { 
                foreach (ServerClient curr in clients)
                {
                    ServerClient temp = new ServerClient();
                    temp.Password = null;
                    temp.Name = curr.Name;
                    temp.Email = curr.Email;
                    temp.ClientGUID = curr.ClientGUID;
                    temp.CreatedUtc = curr.CreatedUtc;
                    temp.UpdatedUtc = curr.UpdatedUtc;
                    temp.IpPort = curr.IpPort;
                    temp.Connection = curr.Connection;
                    
                    ret.Add(temp);
                }
                 
                message = message.Redact();
                message.SyncResponse = message.SyncRequest;
                message.SyncRequest = null;
                message.RecipientGUID = message.SenderGUID;
                message.SenderGUID = Config.GUID;
                message.ChannelGUID = currentChannel.ChannelGUID;
                message.CreatedUtc = DateTime.Now.ToUniversalTime();
                message.Success = true;
                message.Data = Encoding.UTF8.GetBytes(Common.SerializeJson(ret));
                return message;
            } 
        }

        private Message ProcessListClientsMessage(ServerClient client, Message message)
        { 
            List<ServerClient> clients = new List<ServerClient>();
            List<ServerClient> ret = new List<ServerClient>();

            clients = _ConnMgr.GetClients();
            if (clients == null || clients.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Warn, "ProcessListClientsMessage no clients retrieved");
                return null;
            }
            else
            { 
                foreach (ServerClient curr in clients)
                {
                    ServerClient temp = new ServerClient();
                    temp.Connection = curr.Connection;
                    temp.IpPort = curr.IpPort; 
                    temp.Email = curr.Email;
                    temp.Name = curr.Name;
                    temp.Password = null;
                    temp.ClientGUID = curr.ClientGUID;
                    temp.CreatedUtc = curr.CreatedUtc;
                    temp.UpdatedUtc = curr.UpdatedUtc;
                    
                    ret.Add(temp);
                } 
            }

            message = message.Redact();
            message.SyncResponse = message.SyncRequest;
            message.SyncRequest = null;
            message.RecipientGUID = message.SenderGUID;
            message.SenderGUID = Config.GUID;
            message.ChannelGUID = null;
            message.CreatedUtc = DateTime.Now.ToUniversalTime();
            message.Success = true;
            message.Data = SuccessData.ToBytes(null, ret);
            return message; 
        }

        #endregion
    }
}
