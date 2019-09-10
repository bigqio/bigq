using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WatsonTcp;
using WatsonWebsocket;

using BigQ.Server.Classes;
using BigQ.Server.Managers;

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
        // resources
        //
        private MessageBuilder _MsgBuilder;
        private ConnectionManager _ConnMgr;
        private ChannelManager _ChannelMgr;
        private PersistenceManager _PersistenceMgr;
        private readonly object _ClientActiveSendMapLock = new object();
        private Dictionary<string, DateTime> _ClientActiveSendMap = new Dictionary<string, DateTime>(); // Receiver GUID, AddedUTC
        private AuthManager _AuthMgr;

        //
        // Server variables
        //
        private WatsonTcpServer _WTcpServer; 
        private WatsonWsServer _WWsServer; 
         
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
            if (_WTcpServer != null)
            {
                List<string> tcpClients = _WTcpServer.ListClients();
                if (tcpClients != null && tcpClients.Count > 0)
                {
                    foreach (string curr in tcpClients)
                    {
                        _WTcpServer.DisconnectClient(curr);
                    }
                }

                _WTcpServer.Dispose();
                _WTcpServer = null;
            }

            if (_WWsServer != null)
            {
                List<string> wsClients = _WWsServer.ListClients().ToList();
                if (wsClients != null && wsClients.Count > 0)
                {
                    foreach (string curr in wsClients)
                    {
                        _WWsServer.DisconnectClient(curr);
                    }
                }

                _WWsServer.Dispose();
                _WWsServer = null;
            }
             
            _AuthMgr.Dispose();
            _ConnMgr.Dispose();
            _ChannelMgr.Dispose();
            _PersistenceMgr.Dispose();
            _ClientActiveSendMap = null;

            if (_CleanupCancellationTokenSource != null)
            {
                if (!_CleanupCancellationTokenSource.IsCancellationRequested) _CleanupCancellationTokenSource.Cancel();
                _CleanupCancellationTokenSource.Dispose();
                _CleanupCancellationTokenSource = null;
            }

            return; 
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
        public List<ServerClient> ListMembers(string guid)
        {
            return _ChannelMgr.GetChannelMembers(guid);
        }

        /// <summary>
        /// Retrieve list of subscribers in a given channel.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>List of ServerClient objects.</returns>
        public List<ServerClient> ListSubscribers(string guid)
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
        /// Retrieve all objects in the user configuration file defined in the server configuration file.
        /// </summary>
        /// <returns></returns>
        public List<User> ListUsersFile()
        {
            return _AuthMgr.GetCurrentUsersFile();
        }

        /// <summary>
        /// Retrieve all objects in the permissions configuration file defined in the server configuration file.
        /// </summary>
        /// <returns></returns>
        public List<Permission> ListPermissionsFile()
        {
            return _AuthMgr.GetCurrentPermissionsFile();
        }

        /// <summary>
        /// Create a channel owned by the server.
        /// </summary>
        /// <param name="channelType">ChannelType.</param>
        /// <param name="name">Name of the channel.</param>
        /// <param name="guid">GUID of the channel.</param>
        /// <param name="isPrivate">True if the channel is private.</param>
        public void Create(ChannelType channelType, string name, string guid, bool isPrivate)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            Channel newChannel = new Channel();

            if (!String.IsNullOrEmpty(guid)) newChannel.ChannelGUID = guid;
            else newChannel.ChannelGUID = Guid.NewGuid().ToString();

            newChannel.ChannelName = name;
            newChannel.OwnerGUID = Config.GUID;
            newChannel.CreatedUtc = timestamp;

            if (isPrivate) newChannel.Visibility = ChannelVisibility.Private;
            else newChannel.Visibility = ChannelVisibility.Public;

            newChannel.Type = channelType;
            newChannel.Members = new List<ServerClient>();
            newChannel.Subscribers = new List<ServerClient>();

            _ChannelMgr.AddChannel(newChannel);

            return;
        }
         
        /// <summary>
        /// Delete a channel from the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool Delete(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Channel currentChannel = _ChannelMgr.GetChannelByGUID(guid);
            if (currentChannel == null) return false;

            return RemoveChannel(currentChannel);
        }

        /// <summary>
        /// Disconnect a client by GUID.
        /// </summary>
        /// <param name="guid">Client GUID.</param>
        public void DisconnectClient(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            ServerClient client = _ConnMgr.GetClientByGUID(guid);
            if (client == null) return;

            _ChannelMgr.RemoveClientChannels(guid, out List<Channel> deletedChannels);
            DestroyClient(client.IpPort);
        }

        #endregion

        #region Private-Watson-Callback-Methods

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private async Task WatsonTcpClientConnected(string ipPort)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            ServerClient currentClient = new ServerClient(ipPort, ConnectionType.Tcp);
            _ConnMgr.AddClient(currentClient);
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private async Task WatsonTcpSslClientConnected(string ipPort)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            ServerClient currentClient = new ServerClient(ipPort, ConnectionType.TcpSsl);
            _ConnMgr.AddClient(currentClient);
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private async Task<bool> WatsonWebsocketClientConnected(string ipPort, HttpListenerRequest req)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        { 
            ServerClient currentClient = new ServerClient(ipPort, ConnectionType.Websocket);
            _ConnMgr.AddClient(currentClient);
            return true;
        }
         
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private async Task<bool> WatsonWebsocketSslClientConnected(string ipPort, HttpListenerRequest req)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        { 
            ServerClient currentClient = new ServerClient(ipPort, ConnectionType.WebsocketSsl);
            _ConnMgr.AddClient(currentClient);
            return true;
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private async Task WatsonClientDisconnected(string ipPort)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        { 
            DestroyClient(ipPort); 
        }

        private async Task WatsonMessageReceived(string ipPort, byte[] data)
        {
            ServerClient currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(ServerClient)) return;

            Message currentMessage = new Message(data);
            MessageProcessor(currentClient, currentMessage);
            if (Callbacks.MessageReceived != null)
            {
                await Task.Run(() => Callbacks.MessageReceived(currentMessage));
            }
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
             
            #region Set-Class-Variables
             
            _MsgBuilder = new MessageBuilder(Config.GUID);
            _ConnMgr = new ConnectionManager(Config);
            _ChannelMgr = new ChannelManager(Config); 
            _ClientActiveSendMap = new Dictionary<string, DateTime>();
            _AuthMgr = new AuthManager(Config);
             
            if (Config.Persistence != null && Config.Persistence.EnablePersistence)
            {
                _PersistenceMgr = new PersistenceManager(Config);
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
                ServerClient currClient = new ServerClient();
                currClient.Email = null;
                currClient.Password = null;
                currClient.ClientGUID = Config.GUID;
                currClient.IpPort = "127.0.0.1:0";
                currClient.CreatedUtc = DateTime.Now.ToUniversalTime(); 

                foreach (Channel curr in Config.ServerChannels)
                {
                    AddChannel(currClient, curr);
                }
            }

            #endregion

            #region Start-Watson-Servers

            if (Config.TcpServer.Enable)
            {
                #region Start-TCP-Server
                 
                _WTcpServer = new WatsonTcpServer(
                    Config.TcpServer.Ip,
                    Config.TcpServer.Port);

                _WTcpServer.ClientConnected = WatsonTcpClientConnected;
                _WTcpServer.ClientDisconnected = WatsonClientDisconnected;
                _WTcpServer.MessageReceived = WatsonMessageReceived;
                _WTcpServer.Debug = Config.TcpServer.Debug;
                _WTcpServer.Start();

                #endregion
            }

            if (Config.TcpSslServer.Enable)
            {
                #region Start-TCP-SSL-Server
                 
                _WTcpServer = new WatsonTcpServer(
                    Config.TcpSslServer.Ip,
                    Config.TcpSslServer.Port,
                    Config.TcpSslServer.PfxCertFile,
                    Config.TcpSslServer.PfxCertPassword);

                _WTcpServer.AcceptInvalidCertificates = Config.TcpSslServer.AcceptInvalidCerts;
                _WTcpServer.ClientConnected = WatsonTcpSslClientConnected;
                _WTcpServer.ClientDisconnected = WatsonTcpClientConnected;
                _WTcpServer.MessageReceived = WatsonMessageReceived;
                _WTcpServer.Debug = Config.TcpSslServer.Debug;
                _WTcpServer.Start();

                #endregion
            }

            if (Config.WebsocketServer.Enable)
            {
                #region Start-Websocket-Server

                _WWsServer = new WatsonWsServer(
                    Config.WebsocketServer.Ip,
                    Config.WebsocketServer.Port,
                    false);

                _WWsServer.ClientConnected = WatsonWebsocketClientConnected;
                _WWsServer.ClientDisconnected = WatsonClientDisconnected;
                _WWsServer.MessageReceived = WatsonMessageReceived;
                _WWsServer.Debug = Config.WebsocketServer.Debug;
                _WWsServer.Start();

                #endregion
            }

            if (Config.WebsocketSslServer.Enable)
            {
                #region Start-Websocket-SSL-Server

                _WWsServer = new WatsonWsServer(
                    Config.WebsocketSslServer.Ip,
                    Config.WebsocketSslServer.Port,
                    true);

                _WWsServer.ClientConnected = WatsonWebsocketSslClientConnected;
                _WWsServer.ClientDisconnected = WatsonClientDisconnected;
                _WWsServer.MessageReceived = WatsonMessageReceived;
                _WWsServer.AcceptInvalidCertificates = Config.WebsocketSslServer.AcceptInvalidCerts;
                _WWsServer.Debug = Config.WebsocketSslServer.Debug;
                _WWsServer.Start();

                #endregion
            }
             
            #endregion
        }
         
        private void DestroyClient(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort)) return;
            ServerClient currentClient = _ConnMgr.GetByIpPort(ipPort);
            if (currentClient == null || currentClient == default(ServerClient)) return;

            ServerLeaveEvent(currentClient);

            _ConnMgr.RemoveClient(ipPort);
            if (!String.IsNullOrEmpty(currentClient.ClientGUID)) _ChannelMgr.RemoveClient(currentClient.ClientGUID);

            if (currentClient.Connection == ConnectionType.Tcp 
                || currentClient.Connection == ConnectionType.TcpSsl)
            {
                _WTcpServer.DisconnectClient(ipPort);
            }

            if (currentClient.Connection == ConnectionType.Websocket 
                || currentClient.Connection == ConnectionType.WebsocketSsl)
            {
                _WWsServer.DisconnectClient(ipPort);
            }

            currentClient.Dispose(); 
            return;
        }
           
        #endregion
         
        #region Private-Senders-and-Queues

        private bool SendMessage(ServerClient client, Message message)
        {
            bool locked = false;

            try
            {  
                lock (_ClientActiveSendMapLock)
                {
                    _ClientActiveSendMap.Add(message.RecipientGUID, DateTime.Now.ToUniversalTime());
                }
                 
                locked = true;

                byte[] data = message.ToBytes();

                if (client.Connection == ConnectionType.Tcp || client.Connection == ConnectionType.TcpSsl)
                {
                    return _WTcpServer.Send(client.IpPort, data);
                }
                else if (client.Connection == ConnectionType.Websocket || client.Connection == ConnectionType.WebsocketSsl)
                {
                    return _WWsServer.SendAsync(client.IpPort, data).Result;
                }
                else
                {
                    return false;
                } 
            }
            catch (Exception)
            { 
                return false;
            }
            finally
            { 
                if (locked)
                {
                    lock (_ClientActiveSendMapLock)
                    {
                        if (_ClientActiveSendMap.ContainsKey(message.RecipientGUID))
                            _ClientActiveSendMap.Remove(message.RecipientGUID);
                    }
                } 
            }
        }

        private bool ChannelDataSender(ServerClient client, Channel channel, Message message)
        {
            if (channel.Type == ChannelType.Broadcast)
            {
                #region Broadcast-Channel

                List<ServerClient> currChannelMembers = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
                if (currChannelMembers == null || currChannelMembers.Count < 1) return true;

                message.SenderGUID = client.ClientGUID;
                foreach (ServerClient curr in currChannelMembers)
                { 
                    message.RecipientGUID = curr.ClientGUID;
                    bool respSuccess = false;
                    respSuccess = QueueClientMessage(curr, message); 
                }

                return true;

                #endregion
            }
            else if (channel.Type == ChannelType.Multicast)
            {
                #region Multicast-Channel-to-Subscribers

                List<ServerClient> currChannelSubscribers = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
                if (currChannelSubscribers == null || currChannelSubscribers.Count < 1) return true;

                message.SenderGUID = client.ClientGUID;
                foreach (ServerClient curr in currChannelSubscribers)
                { 
                    message.RecipientGUID = curr.ClientGUID;
                    bool respSuccess = false;
                    respSuccess = QueueClientMessage(curr, message);  
                }

                return true;

                #endregion
            }
            else if (channel.Type == ChannelType.Unicast)
            {
                #region Unicast-Channel-to-Subscriber

                List<ServerClient> currChannelSubscribers = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
                if (currChannelSubscribers == null || currChannelSubscribers.Count < 1) return true;

                message.SenderGUID = client.ClientGUID;
                ServerClient recipient = currChannelSubscribers[_Random.Next(0, currChannelSubscribers.Count)];
                message.RecipientGUID = recipient.ClientGUID;
                bool respSuccess = false;
                respSuccess = QueueClientMessage(recipient, message);

                return true;

                #endregion
            }
            else
            {
                #region Unknown

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
             
            Task.Run(() => ProcessClientRamQueue(client, client.RamQueueToken), client.RamQueueToken);
            Task.Run(() => ProcessClientDiskQueue(client, client.RamQueueToken), client.DiskQueueToken); 
        }

        private bool QueueClientMessage(ServerClient client, Message message)
        { 
            if (message.Persist)
            { 
                if (!_PersistenceMgr.PersistMessage(message)) return false;
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
                        if (!String.IsNullOrEmpty(currMessage.RecipientGUID))
                        {
                            if (!SendMessage(client, currMessage))
                            {
                                ServerClient tempClient = _ConnMgr.GetClientByGUID(currMessage.RecipientGUID);
                                if (tempClient == null)
                                {
                                    client.Dispose();
                                    return;
                                }
                                else
                                {
                                    client.MessageQueue.Add(currMessage);
                                }
                            }
                        }
                    } 
                }

                #endregion
            }
            catch (Exception)
            { 
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
                                ServerClient tempClient = _ConnMgr.GetClientByGUID(client.ClientGUID);
                                if (tempClient == null)
                                { 
                                    client.Dispose();
                                    return;
                                }
                                else
                                { 
                                    break;
                                }
                            }
                            else
                            {
                                _PersistenceMgr.ExpireMessage(curr.Key); 
                            }
                        }
                    }
                }

                #endregion
            }
            catch (Exception)
            {
                return;
            }
        }

        #endregion
        
        #region Private-Event-Methods

        private bool ServerJoinEvent(ServerClient client)
        {  
            List<ServerClient> currentClients = _ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1) return true;

            Message msg = _MsgBuilder.ServerJoinEvent(client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                { 
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                }
            }

            return true;
        }

        private bool ServerLeaveEvent(ServerClient client)
        {  
            List<ServerClient> currentClients = _ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1) return true;

            Message msg = _MsgBuilder.ServerLeaveEvent(client);

            foreach (ServerClient curr in currentClients)
            {
                if (!String.IsNullOrEmpty(curr.ClientGUID))
                {
                    if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                    }
                }
            }

            return true;
        }

        private bool ChannelJoinEvent(ServerClient client, Channel channel)
        {
            try
            {
                List<ServerClient> currentClients = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
                if (currentClients == null || currentClients.Count < 1) return true;

                Message msg = _MsgBuilder.ChannelJoinEvent(channel, client);

                foreach (ServerClient curr in currentClients)
                {
                    if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                    {
                        msg.RecipientGUID = curr.ClientGUID;
                        bool responseSuccess = QueueClientMessage(curr, msg);
                    }
                }

                return true;
            }
            catch (Exception)
            { 
                return false;
            }
        }

        private bool ChannelLeaveEvent(ServerClient client, Channel channel)
        {  
            List<ServerClient> currentClients = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1) return true;

            Message msg = _MsgBuilder.ChannelLeaveEvent(channel, client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                }
            }

            return true;
        }

        private bool ChannelCreateEvent(ServerClient client, Channel channel)
        {
            if (channel.Visibility == ChannelVisibility.Private) return true;

            List<ServerClient> currentClients = _ConnMgr.GetClients();
            if (currentClients == null || currentClients.Count < 1) return true;

            foreach (ServerClient curr in currentClients)
            {
                Message msg = _MsgBuilder.ChannelCreateEvent(client, channel);
                msg.RecipientGUID = curr.ClientGUID;
                bool responseSuccess = QueueClientMessage(curr, msg);
            }

            return true;
        }
         
        private bool ChannelDestroyEvent(ServerClient client, Channel channel)
        {
            if (channel.Visibility == ChannelVisibility.Private) return true;

            List<ServerClient> currentClients = _ChannelMgr.GetChannelMembers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1) return true;

            foreach (ServerClient curr in currentClients)
            {
                Message msg = _MsgBuilder.ChannelDestroyEvent(client, channel);
                msg.RecipientGUID = curr.ClientGUID;
                bool responseSuccess = QueueClientMessage(curr, msg);
            }

            return true;
        }

        private bool SubscriberJoinEvent(ServerClient client, Channel channel)
        {  
            List<ServerClient> currentClients = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1) return true;

            Message msg = _MsgBuilder.ChannelSubscriberJoinEvent(channel, client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
                }
            }

            return true;
        }
         
        private bool SubscriberLeaveEvent(ServerClient client, Channel channel)
        {  
            List<ServerClient> currentClients = _ChannelMgr.GetChannelSubscribers(channel.ChannelGUID);
            if (currentClients == null || currentClients.Count < 1) return true;

            Message msg = _MsgBuilder.ChannelSubscriberLeaveEvent(channel, client);

            foreach (ServerClient curr in currentClients)
            {
                if (String.Compare(curr.ClientGUID, client.ClientGUID) != 0)
                {
                    msg.RecipientGUID = curr.ClientGUID;
                    bool responseSuccess = QueueClientMessage(curr, msg);
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

                    lock (_ClientActiveSendMapLock)
                    {
                        foreach (KeyValuePair<string, DateTime> curr in _ClientActiveSendMap)
                        {
                            if (String.IsNullOrEmpty(curr.Key)) continue;
                            if (DateTime.Compare(DateTime.Now.ToUniversalTime(), curr.Value) > 0)
                            {
                                _ClientActiveSendMap.Remove(curr.Key);
                            }
                        } 
                    } 
                    
                    #endregion
                }
            }
            catch (ThreadAbortException)
            {
                // do nothing
            }
            catch (Exception)
            {

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
            lock (_ClientActiveSendMapLock)
            {
                return new Dictionary<string, DateTime>(_ClientActiveSendMap);
            }
        }
        
        private void AddChannel(ServerClient client, Channel channel)
        { 
            DateTime timestamp = DateTime.Now.ToUniversalTime();
            if (channel.CreatedUtc == null) channel.CreatedUtc = timestamp; 
            channel.Members = new List<ServerClient>();
            channel.Members.Add(client);
            channel.Subscribers = new List<ServerClient>();
            channel.OwnerGUID = client.ClientGUID;

            if (_ChannelMgr.ChannelExists(channel.ChannelGUID))
            { 
                return;
            }

            _ChannelMgr.AddChannel(channel);
            return;
        }

        private bool RemoveChannel(Channel channel)
        { 
            channel = _ChannelMgr.GetChannelByGUID(channel.ChannelGUID);
            if (channel == null) return false;

            if (channel.OwnerGUID.Equals(Config.GUID)) return true;

            _ChannelMgr.RemoveChannel(channel.ChannelGUID); 

            if (channel.Members != null)
            {
                if (channel.Members.Count > 0)
                {
                    //
                    // create another reference in case list is modified
                    //
                    Channel tempChannel = channel;
                    List<ServerClient> tempMembers = new List<ServerClient>(channel.Members);
                     
                    foreach (ServerClient currentClient in tempMembers)
                    { 
                        SendSystemMessage(_MsgBuilder.ChannelDeletedByOwner(currentClient, tempChannel)); 
                    } 
                }
            }
             
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
                    ChannelLeaveEvent(client, channel);
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
            if (_ChannelMgr.RemoveChannelSubscriber(channel, client))
            { 
                #region Send-Notifications

                if (Config.Notification.ChannelJoinNotification)
                {
                    ChannelLeaveEvent(client, channel);
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
            ServerClient currentRecipient = null;
            Channel currentChannel = null;
            Message responseMessage = new Message();
            bool responseSuccess = false;
             
            if (String.IsNullOrEmpty(client.ClientGUID))
            {
                if (message.Command != MessageCommand.Login)
                {  
                    QueueClientMessage(client, _MsgBuilder.LoginRequired());
                    return false;
                }
            }
            else
            { 
                if (String.Compare(client.ClientGUID, Config.GUID) != 0)
                {
                    ServerClient verifyClient = _ConnMgr.GetClientByGUID(client.ClientGUID);
                    if (verifyClient == null)
                    {
                        QueueClientMessage(client, _MsgBuilder.LoginRequired());
                        return false;
                    }
                } 
            }
             
            if (!_AuthMgr.AuthorizeMessage(message))
            { 
                responseMessage = _MsgBuilder.AuthorizationFailed(message);
                QueueClientMessage(client, responseMessage);
                return false;
            }
             
            if (message.Persist)
            {
                if (Config.Persistence == null || !Config.Persistence.EnablePersistence) return false;
            }
              
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
                responseMessage = _MsgBuilder.RecipientNotFound(client, message);
                QueueClientMessage(client, responseMessage);
                return false; 
            }
             
            if (currentRecipient != null)
            {
                return QueueClientMessage(currentRecipient, message);
            }
            else if (currentChannel != null)
            { 
                if (currentChannel.Type == ChannelType.Broadcast) return SendChannelMembersMessage(client, currentChannel, message);
                else if (currentChannel.Type == ChannelType.Multicast) return SendChannelSubscribersMessage(client, currentChannel, message);
                else if (currentChannel.Type == ChannelType.Unicast) return SendChannelSubscriberMessage(client, currentChannel, message);
                else
                {
                    RemoveChannel(currentChannel);
                    return false;
                } 
            }
            else
            {
                responseMessage = _MsgBuilder.RecipientNotFound(client, message);
                QueueClientMessage(client, responseMessage);
                return false;
            } 
        }

        private bool SendPrivateMessage(ServerClient sender, ServerClient rcpt, Message message)
        {  
            bool responseSuccess = false;
            Message responseMessage = new Message();
             
            responseSuccess = QueueClientMessage(rcpt, message.Redact());
              
            if (message.SyncRequest)
            { 
                //
                // do not send notifications for success/fail on a sync message
                //

                return true; 
            }
            else if (message.SyncResponse)
            { 
                //
                // do not send notifications for success/fail on a sync message
                //

                return true; 
            }
            else
            { 
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
            } 
        }

        private bool SendChannelMembersMessage(ServerClient sender, Channel channel, Message message)
        { 
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
              
            bool responseSuccess = false;
            Message responseMessage = new Message(); 

            if (!IsChannelMember(sender, channel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, message, channel);
                QueueClientMessage(sender, responseMessage);
                return false;
            }

            responseSuccess = ChannelDataSender(sender, channel, message.Redact());

            if (Config.Notification.MsgAcknowledgement)
            {
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, message);
                QueueClientMessage(sender, responseMessage);
            }

            return true;
        }

        private bool SendChannelSubscribersMessage(ServerClient sender, Channel channel, Message message)
        {  
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
              
            bool responseSuccess = false;
            Message responseMessage = new Message();
             
            if (!IsChannelMember(sender, channel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, message, channel);
                QueueClientMessage(sender, responseMessage);
                return false;
            }
              
            responseSuccess = ChannelDataSender(sender, channel, message.Redact());

            if (Config.Notification.MsgAcknowledgement)
            {
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, message);
                QueueClientMessage(sender, responseMessage);
            }

            return true;
        }
        
        private bool SendChannelSubscriberMessage(ServerClient sender, Channel channel, Message message)
        {  
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
              
            bool responseSuccess = false;
            Message responseMessage = new Message();
             
            if (!IsChannelMember(sender, channel))
            {
                responseMessage = _MsgBuilder.NotChannelMember(sender, message, channel);
                QueueClientMessage(sender, responseMessage);
                return false;
            }
             
            responseSuccess = ChannelDataSender(sender, channel, message.Redact());

            if (Config.Notification.MsgAcknowledgement)
            {
                responseMessage = _MsgBuilder.MessageQueueSuccess(sender, message);
                responseSuccess = QueueClientMessage(sender, responseMessage);
            }

            return true;
        }

        private bool SendSystemMessage(Message message)
        {
            try
            {
                ServerClient currentClient = new ServerClient();
                currentClient.Email = null;
                currentClient.Password = null;
                currentClient.ClientGUID = Config.GUID;
                currentClient.Name = "Server";
                currentClient.IpPort = "127.0.0.1:0";
                currentClient.CreatedUtc = DateTime.Now.ToUniversalTime(); 

                ServerClient currentRecipient = new ServerClient();
                Channel currentChannel = new Channel();
                Message responseMessage = new Message();

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
                    return false;
                }

                if (currentRecipient != null)
                {
                    return QueueClientMessage(currentRecipient, message.Redact());
                }
                else if (currentChannel != null)
                {
                    return ChannelDataSender(currentClient, currentChannel, message.Redact());
                }
                else
                {
                    responseMessage = _MsgBuilder.RecipientNotFound(currentClient, message);
                    QueueClientMessage(currentClient, responseMessage);
                    return false;
                }
            }
            catch (Exception)
            { 
                return false;
            }
        }

        private bool SendSystemPrivateMessage(ServerClient rcpt, Message message)
        { 
            ServerClient currentClient = new ServerClient();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = Config.GUID;
            currentClient.Name = "Server";
            currentClient.IpPort = "127.0.0.1:0";
            currentClient.CreatedUtc = DateTime.Now.ToUniversalTime(); 
             
            Channel currentChannel = new Channel(); 
            return QueueClientMessage(rcpt, message.Redact());
        }

        private bool SendSystemChannelMessage(Channel channel, Message message)
        {  
            if (String.IsNullOrEmpty(message.ChannelName)) message.ChannelName = channel.ChannelName;
              
            ServerClient currentClient = new ServerClient();
            currentClient.Email = null;
            currentClient.Password = null;
            currentClient.ClientGUID = Config.GUID;
            currentClient.Name = "Server";
            currentClient.IpPort = "127.0.0.1:0";
            currentClient.CreatedUtc = DateTime.Now.ToUniversalTime();

            //
            // This is necessary so the message goes to members instead of subscribers
            // in case the channel is configured as a multicast channel
            //
            channel.Type = ChannelType.Broadcast;

            return ChannelDataSender(currentClient, channel, message);
        }

        #endregion

        #region Private-Message-Handlers

        private Message ProcessEchoMessage(ServerClient client, Message message)
        { 
            message = message.Redact();
            message.SyncResponse = message.SyncRequest;
            message.SyncRequest = false;
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
                message.SyncRequest = false;
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
            catch (Exception)
            { 
                return null;
            }
            finally
            { 
                if (runClientLoginTask)
                {
                    if (Callbacks.ClientLogin != null)
                    {
                        Callbacks.ClientLogin(client).Wait();
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
            message.SyncRequest = false;
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
            try
            {
                Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);

                if (currentChannel == null)
                { 
                    return _MsgBuilder.ChannelNotFound(client, message);
                }
                else
                { 
                    // AddChannelMember handles notifications
                    if (!AddChannelMember(client, currentChannel))
                    { 
                        Message ret = _MsgBuilder.ChannelJoinFailure(client, message, currentChannel); 
                        return ret;
                    }
                    else
                    { 
                        Message ret = _MsgBuilder.ChannelJoinSuccess(client, message, currentChannel); 
                        return ret;
                    }
                }
            }
            catch (Exception)
            { 
                return null;
            }
        }

        private Message ProcessSubscribeChannelMessage(ServerClient client, Message message)
        {
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            if (currentChannel == null) return _MsgBuilder.ChannelNotFound(client, message);
            if (currentChannel.Type == ChannelType.Broadcast) return ProcessJoinChannelMessage(client, message);
            // AddChannelMember and AddChannelSubscriber handle notifications
            if (!AddChannelMember(client, currentChannel)) return  _MsgBuilder.ChannelJoinFailure(client, message, currentChannel);
            if (!AddChannelSubscriber(client, currentChannel)) return _MsgBuilder.ChannelSubscribeFailure(client, message, currentChannel); 
            return _MsgBuilder.ChannelSubscribeSuccess(client, message, currentChannel);
        }

        private Message ProcessLeaveChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID); 

            if (currentChannel == null)return _MsgBuilder.ChannelNotFound(client, message);
            else
            {
                if (client.ClientGUID.Equals(currentChannel.OwnerGUID)) 
                {
                    if (!RemoveChannel(currentChannel)) return _MsgBuilder.ChannelLeaveFailure(client, message, currentChannel);
                    else return _MsgBuilder.ChannelDeleteSuccess(client, message, currentChannel);
                }
                else
                { 
                    if (!RemoveChannelMember(client, currentChannel)) return _MsgBuilder.ChannelLeaveFailure(client, message, currentChannel);
                    else
                    {
                        if (Config.Notification.ChannelJoinNotification) ChannelLeaveEvent(client, currentChannel);
                        return _MsgBuilder.ChannelLeaveSuccess(client, message, currentChannel);
                    }
                }
            } 
        }

        private Message ProcessUnsubscribeChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);

            if (currentChannel == null) return _MsgBuilder.ChannelNotFound(client, message);
            if (currentChannel.Type == ChannelType.Broadcast) return ProcessLeaveChannelMessage(client, message);
            if (client.ClientGUID.Equals(currentChannel.OwnerGUID))
            { 
                if (!RemoveChannel(currentChannel)) return _MsgBuilder.ChannelUnsubscribeFailure(client, message, currentChannel); 
                else return _MsgBuilder.ChannelDeleteSuccess(client, message, currentChannel); 
            }
            else
            {
                SubscriberLeaveEvent(client, currentChannel);

                if (!RemoveChannelSubscriber(client, currentChannel)) return _MsgBuilder.ChannelUnsubscribeFailure(client, message, currentChannel); 
                else
                { 
                    Message ret = _MsgBuilder.ChannelUnsubscribeSuccess(client, message, currentChannel);
                    return ret;
                } 
            } 
        }

        private Message ProcessCreateChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);

            if (currentChannel == null)
            {
                Channel requestChannel = Channel.FromMessage(client, message); 
                if (requestChannel == null) return _MsgBuilder.DataError(client, message, "Unable to create channel from supplied message data");
                else
                { 
                    currentChannel = _ChannelMgr.GetChannelByName(requestChannel.ChannelName);
                    if (currentChannel != null) return _MsgBuilder.ChannelAlreadyExists(client, message, currentChannel);
                    else
                    { 
                        if (String.IsNullOrEmpty(requestChannel.ChannelGUID)) requestChannel.ChannelGUID = Guid.NewGuid().ToString();
                        requestChannel.OwnerGUID = client.ClientGUID;
                         
                        AddChannel(client, requestChannel); 
                        ChannelCreateEvent(client, requestChannel);
                         
                        if (!AddChannelSubscriber(client, requestChannel))
                        { 
                            return _MsgBuilder.ChannelJoinFailure(client, message, currentChannel);
                        } 
                        return _MsgBuilder.ChannelCreateSuccess(client, message, requestChannel);
                    }
                }
            }
            else
            {
                return _MsgBuilder.ChannelAlreadyExists(client, message, currentChannel);
            } 
        }

        private Message ProcessDeleteChannelMessage(ServerClient client, Message message)
        { 
            Channel currentChannel = _ChannelMgr.GetChannelByGUID(message.ChannelGUID);
            
            if (currentChannel == null) return _MsgBuilder.ChannelNotFound(client, message);
            if (String.Compare(currentChannel.OwnerGUID, client.ClientGUID) != 0) return _MsgBuilder.ChannelDeleteFailure(client, message, currentChannel);
            if (!RemoveChannel(currentChannel)) return _MsgBuilder.ChannelDeleteFailure(client, message, currentChannel);
            else
            {
                ChannelDestroyEvent(client, currentChannel);
                return _MsgBuilder.ChannelDeleteSuccess(client, message, currentChannel);
            }
        }

        private Message ProcessListChannelsMessage(ServerClient client, Message message)
        { 
            List<Channel> ret = new List<Channel>();
            List<Channel> filtered = new List<Channel>();
            Channel currentChannel = new Channel();

            ret = _ChannelMgr.GetChannels();
            if (ret == null || ret.Count < 1)
            { 
                message = message.Redact();
                message.SyncResponse = message.SyncRequest;
                message.SyncRequest = false;
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
                    currentChannel.Visibility = curr.Visibility;
                    currentChannel.Type = curr.Type;

                    if (currentChannel.OwnerGUID.Equals(client.ClientGUID))
                    {
                        filtered.Add(currentChannel);
                        continue;
                    }

                    if (currentChannel.Visibility == ChannelVisibility.Public)
                    {
                        filtered.Add(currentChannel);
                        continue;
                    }
                } 
            }
                
            message = message.Redact();
            message.SyncResponse = message.SyncRequest;
            message.SyncRequest = false;
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
            List<ServerClient> clients = new List<ServerClient>();
            List<ServerClient> ret = new List<ServerClient>();

            if (currentChannel == null) return _MsgBuilder.ChannelNotFound(client, message);

            clients = _ChannelMgr.GetChannelMembers(currentChannel.ChannelGUID);
            if (clients == null || clients.Count < 1) return _MsgBuilder.ChannelNoMembers(client, message, currentChannel);
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
                    temp.IpPort = curr.IpPort;
                    temp.Connection = curr.Connection; 
                    
                    ret.Add(temp);
                }
                 
                message = message.Redact();
                message.SyncResponse = message.SyncRequest;
                message.SyncRequest = false;
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

            if (currentChannel == null) return _MsgBuilder.ChannelNotFound(client, message);
            if (currentChannel.Type == ChannelType.Broadcast)
            { 
                return ProcessListChannelMembersMessage(client, message);
            }

            clients = _ChannelMgr.GetChannelSubscribers(currentChannel.ChannelGUID);
            if (clients == null || clients.Count < 1) return _MsgBuilder.ChannelNoSubscribers(client, message, currentChannel);
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
                    temp.IpPort = curr.IpPort;
                    temp.Connection = curr.Connection;
                    
                    ret.Add(temp);
                }
                 
                message = message.Redact();
                message.SyncResponse = message.SyncRequest;
                message.SyncRequest = false;
                message.RecipientGUID = message.SenderGUID;
                message.SenderGUID = Config.GUID;
                message.ChannelGUID = currentChannel.ChannelGUID;
                message.CreatedUtc = DateTime.Now.ToUniversalTime();
                message.Success = true;
                message.Data = SuccessData.ToBytes(null, ret);
                return message;
            } 
        }

        private Message ProcessListClientsMessage(ServerClient client, Message message)
        { 
            List<ServerClient> clients = new List<ServerClient>();
            List<ServerClient> ret = new List<ServerClient>();

            clients = _ConnMgr.GetClients();
            if (clients == null || clients.Count < 1) return null;
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
                    
                    ret.Add(temp);
                } 
            }

            message = message.Redact();
            message.SyncResponse = message.SyncRequest;
            message.SyncRequest = false;
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
