using BigQ.Core;
using Newtonsoft.Json.Linq; 
using System; 
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WatsonTcp;
using WatsonWebsocket;

namespace BigQ.Client
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
        /// Callback methods used when certain events occur.
        /// </summary>
        public ClientCallbacks Callbacks;

        /// <summary>
        /// The type of connection.
        /// </summary>
        public ConnectionType Connection { get; private set; }
         
        /// <summary>
        /// Indicates whether or not the client is connected to the server.  Do not modify this field.
        /// </summary>
        public bool Connected { get; private set; }

        /// <summary>
        /// Indicates whether or not the client is logged in to the server.  Do not modify this field.
        /// </summary>
        public bool LoggedIn { get; private set; }
         
        #endregion

        #region Private-Members
         
        private CancellationTokenSource _CleanupSyncTokenSource = null;
        private CancellationToken _CleanupSyncToken;

        private readonly object _SyncRequestsLock = new object();
        private Dictionary<string, DateTime> _SyncRequests = new Dictionary<string, DateTime>();

        private readonly object _SyncResponsesLock = new object();
        private Dictionary<string, Message> _SyncResponses = new Dictionary<string, Message>();

        private WatsonTcpClient _WTcpClient; 
        private WatsonWsClient _WWsClient;
        private WatsonWsClient _WWsSslClient;

        private Random _Random;

        #endregion
        
        #region Constructors-and-Factories

        /// <summary>
        /// Start an instance of the BigQ client process with default configuration.
        /// </summary>
        public Client()
        {
            Config = ClientConfiguration.Default();
            InitializeClient();
        }

        /// <summary>
        /// Start an instance of the BigQ client process.
        /// </summary>
        /// <param name="configFile">The full path and filename of the configuration file.  Leave null for a default configuration.</param>
        public Client(string configFile)
        {
            #region Load-Config

            Config = null;

            if (String.IsNullOrEmpty(configFile))
            {
                Config = ClientConfiguration.Default();
            }
            else
            {
                Config = ClientConfiguration.LoadConfig(configFile);
            }

            if (Config == null) throw new Exception("Unable to initialize configuration.");

            Config.ValidateConfig();

            #endregion

            InitializeClient();
        }

        /// <summary>
        /// Start an instance of the BigQ client process.
        /// </summary>
        /// <param name="config">Populated client configuration object.</param>
        public Client(ClientConfiguration config)
        {
            #region Load-Config

            if (config == null) throw new ArgumentNullException(nameof(config));
            config.ValidateConfig();
            Config = config;

            #endregion

            InitializeClient();
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
            if (message == null) throw new ArgumentNullException(nameof(message));

            byte[] data = message.ToBytes();

            if (Config.TcpServer.Enable || Config.TcpSslServer.Enable)
            {
                return _WTcpClient.SendAsync(data).Result;
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.Echo;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.RecipientGUID = Config.ServerGUID;
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.Login;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                LoggedIn = true;
                if (Callbacks.ServerConnected != null)
                {
                    new Thread(delegate ()
                    {
                        Callbacks.ServerConnected();
                    }).Start();
                    // Task.Run(() => Callbacks.ServerConnected());
                }
                return true;
            }
        }

        /// <summary>
        /// Retrieve a list of all clients on the server.
        /// </summary>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="clients">The list of clients received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListClients(out Message response, out List<ServerClient> clients)
        {
            response = null;
            clients = null;

            Message request = new Message();
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.ListClients;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = BigQ.Core.Common.DeserializeJson<SuccessData>(response.Data);
                    clients = ((JArray)resp.Data).ToObject<List<ServerClient>>();
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.ListChannels;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = BigQ.Core.Common.DeserializeJson<SuccessData>(response.Data);
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
        public bool ListChannelMembers(string guid, out Message response, out List<ServerClient> clients)
        {
            response = null;
            clients = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.ListChannelMembers;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = BigQ.Core.Common.DeserializeJson<SuccessData>(response.Data);
                    if (resp != null && resp.Data != null)
                    {
                        clients = ((JArray)resp.Data).ToObject<List<ServerClient>>();
                    }
                    else
                    {
                        clients = new List<ServerClient>();
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
        public bool ListChannelSubscribers(string guid, out Message response, out List<ServerClient> clients)
        {
            response = null;
            clients = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            Message request = new Message();
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.ListChannelSubscribers;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = BigQ.Core.Common.DeserializeJson<SuccessData>(response.Data);
                    clients = ((JArray)resp.Data).ToObject<List<ServerClient>>();
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.JoinChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.SubscribeChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.LeaveChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.UnsubscribeChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Create a broadcast channel on the server.  Messages sent to broadcast channels are sent to all members.
        /// </summary>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="isPrivate">Whether or not the channel is private (true) or public (false).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool CreateBroadcastChannel(string name, bool isPrivate, out Message response)
        {
            response = null;
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name)); 

            Channel currentChannel = new Channel();
            currentChannel.ChannelName = name;
            currentChannel.OwnerGUID = Config.ClientGUID;
            currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            currentChannel.Private = isPrivate;
            currentChannel.Broadcast = true;
            currentChannel.Multicast = false;
            currentChannel.Unicast = false;

            Message request = new Message();
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.CreateChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = currentChannel.ChannelGUID;
            request.Data = Encoding.UTF8.GetBytes(BigQ.Core.Common.SerializeJson(currentChannel));

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Create a unicast channel on the server.  Messages sent to unicast channels are sent only to one subscriber randomly.
        /// </summary>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="isPrivate">Whether or not the channel is private (true) or public (false).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool CreateUnicastChannel(string name, bool isPrivate, out Message response)
        {
            response = null;
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name)); 

            Channel currentChannel = new Channel();
            currentChannel.ChannelName = name;
            currentChannel.OwnerGUID = Config.ClientGUID;
            currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            currentChannel.Private = isPrivate;
            currentChannel.Broadcast = false;
            currentChannel.Multicast = false;
            currentChannel.Unicast = true;

            Message request = new Message();
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.CreateChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = currentChannel.ChannelGUID;
            request.Data = Encoding.UTF8.GetBytes(BigQ.Core.Common.SerializeJson(currentChannel));

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Create a multicast channel on the server.  Messages sent to multicast channels are sent only to subscribers.
        /// </summary>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="isPrivate">Whether or not the channel is private (true) or public (false).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool CreateMulticastChannel(string name, bool isPrivate, out Message response)
        {
            response = null;
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name)); 

            Channel currentChannel = new Channel();
            currentChannel.ChannelName = name;
            currentChannel.OwnerGUID = Config.ClientGUID;
            currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            currentChannel.Private = isPrivate;
            currentChannel.Broadcast = false;
            currentChannel.Multicast = true;
            currentChannel.Unicast = false;

            Message request = new Message();
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.CreateChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = currentChannel.ChannelGUID;
            request.Data = Encoding.UTF8.GetBytes(BigQ.Core.Common.SerializeJson(currentChannel));

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.DeleteChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
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
            return SendPrivateMessageAsyncInternal(guid, null, Encoding.UTF8.GetBytes(data), false);
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
            return SendPrivateMessageAsyncInternal(guid, null, data, false);
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously, and queue persistently until expiration.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="contentType">The content type of the data.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="persist">True to persist until expiration.</param>
        /// <returns>Boolean indicating whether or not the message was stored persistently.</returns>
        public bool SendPrivateMessageAsync(string guid, string contentType, string data, bool persist)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return SendPrivateMessageAsyncInternal(guid, contentType, Encoding.UTF8.GetBytes(data), persist);
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously, and queue persistently until expiration.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="contentType">The content type of the data.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="persist">True to persist until expiration.</param>
        /// <returns>Boolean indicating whether or not the message was stored persistently.</returns>
        public bool SendPrivateMessageAsync(string guid, string contentType, byte[] data, bool persist)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));
            return SendPrivateMessageAsyncInternal(guid, contentType, data, persist);
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
            message.Email = Config.Email;
            message.Password = Config.Password;
            message.Command = MessageCommand.MessagePrivate;
            message.CreatedUtc = DateTime.Now.ToUniversalTime();
            message.MessageID = Guid.NewGuid().ToString();
            message.SenderGUID = Config.ClientGUID;
            message.SenderName = Config.Name;
            message.RecipientGUID = guid;
            message.ChannelGUID = null;
            message.SyncRequest = true;
            message.SyncTimeoutMs = Config.SyncTimeoutMs;
            message.SyncResponse = false;
            message.Data = data;

            if (!AddSyncRequest(message.MessageID)) return false;
            if (!SendMessage(message)) return false;
            int timeoutMs = Config.SyncTimeoutMs;
            if (message.SyncTimeoutMs > 0) timeoutMs = Convert.ToInt32(message.SyncTimeoutMs);

            return GetSyncResponse(message.MessageID, timeoutMs, out response);
        }

        /// <summary>
        /// Send a private message to the server asynchronously.
        /// </summary>
        /// <param name="request">The message object you wish to send to the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendServerMessageAsync(Message request)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));
            request.RecipientGUID = Config.ServerGUID;
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
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.RecipientGUID = Config.ServerGUID;

            if (!AddSyncRequest(request.MessageID)) return false;
            if (!SendMessage(request)) return false;
            int timeoutMs = Config.SyncTimeoutMs;
            if (request.SyncTimeoutMs > 0) timeoutMs = request.SyncTimeoutMs;

            return GetSyncResponse(request.MessageID, timeoutMs, out response);
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.MessageChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = null;
            request.ChannelGUID = guid;
            request.SyncRequest = false;
            request.SyncResponse = false;
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.MessageChannel;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = null;
            request.ChannelGUID = guid;
            request.SyncRequest = true;
            request.SyncResponse = false;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.Data = data;

            if (!AddSyncRequest(request.MessageID)) return false;
            if (!SendMessage(request)) return false;
            int timeoutMs = Config.SyncTimeoutMs;
            if (request.SyncTimeoutMs > 0) timeoutMs = request.SyncTimeoutMs;

            return GetSyncResponse(request.MessageID, timeoutMs, out response);
        }

        /// <summary>
        /// Retrieve the list of synchronous requests awaiting responses.
        /// </summary>
        /// <param name="response">A dictionary containing the GUID of the synchronous request (key) and the timestamp it was sent (value).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool PendingSyncRequests(out Dictionary<string, DateTime> response)
        {
            response = null;
            lock (_SyncRequestsLock)
            {
                if (_SyncRequests == null || _SyncRequests.Count < 1) return true;
                response = new Dictionary<string, DateTime>(_SyncRequests);
                return true;
            }
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
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.IsClientConnected;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = Config.ServerGUID;
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.SyncResponse = false;
            request.ChannelGUID = null;
            request.Data = Encoding.UTF8.GetBytes(guid);

            if (!SendServerMessageSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                if (response.Data != null)
                {
                    SuccessData ret = BigQ.Core.Common.DeserializeJson<SuccessData>(Encoding.UTF8.GetString(response.Data));
                    return (ret.Success && Convert.ToBoolean(ret.Data));
                }

                return false;
            }
        }

        #endregion

        #region Private-Watson-Methods

        private bool WatsonServerConnected()
        { 
            Connected = true;
            return true;
        }

        private bool WatsonServerDisconnected()
        { 
            Connected = false;
            LoggedIn = false;
            if (Callbacks.ServerDisconnected != null)
            {
                new Thread(delegate ()
                {
                    Callbacks.ServerDisconnected();
                }).Start();
                // Task.Run(() => Callbacks.ServerDisconnected());
            }
            return true;
        }

        private bool WatsonMessageReceived(byte[] data)
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
            if (String.IsNullOrEmpty(guid)) return false;

            lock (_SyncResponsesLock)
            {
                if (_SyncResponses.Count < 1) return false;
                if (_SyncResponses.ContainsKey(guid)) return true;
            }

            return false;
        }

        private bool AddSyncRequest(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return false;

            lock (_SyncRequestsLock)
            {
                if (_SyncRequests.ContainsKey(guid)) return false;
                _SyncRequests.Add(guid, DateTime.Now);
            }

            return true;
        }

        private bool RemoveSyncRequest(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return false;

            lock (_SyncRequestsLock)
            {
                if (_SyncRequests.ContainsKey(guid)) _SyncRequests.Remove(guid);
            }

            return true;
        }

        private bool SyncRequestExists(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return false;

            lock (_SyncRequestsLock)
            {
                return _SyncRequests.ContainsKey(guid);
            }
        }

        private bool AddSyncResponse(Message response)
        {
            if (response == null) return false;
            if (String.IsNullOrEmpty(response.MessageID)) return false;

            lock (_SyncResponsesLock)
            {
                if (_SyncResponses.ContainsKey(response.MessageID)) return false;
                _SyncResponses.Add(response.MessageID, response);
            }

            return true;
        }

        private bool GetSyncResponse(string guid, int timeoutMs, out Message response)
        {
            response = new Message();
            DateTime start = DateTime.Now;

            if (String.IsNullOrEmpty(guid)) return false; 
            if (timeoutMs < 1000) timeoutMs = 1000;
             
            int iterations = 0;
            while (true)
            {
                lock (_SyncResponsesLock)
                {
                    if (_SyncResponses.ContainsKey(guid))
                    {
                        response = _SyncResponses[guid];
                        response.Success = true;
                        return true;
                    }
                }
                  
                TimeSpan ts = DateTime.Now - start;
                if (ts.TotalMilliseconds > timeoutMs)
                { 
                    response = null;
                    return false;
                }

                iterations++;
                continue;
            } 
        }

        private void CleanupSyncRequests()
        {
            while (true)
            {
                Task.Delay(Config.SyncTimeoutMs).Wait();
                List<string> expiredIds = new List<string>(); 

                lock (_SyncRequestsLock)
                {
                    foreach (KeyValuePair<string, DateTime> currRequest in _SyncRequests)
                    {
                        DateTime expiryTimestamp = currRequest.Value.AddMilliseconds(Config.SyncTimeoutMs);

                        if (DateTime.Compare(expiryTimestamp, DateTime.Now) < 0)
                        {
                            expiredIds.Add(currRequest.Key);
                            _SyncResponses.Remove(currRequest.Key);
                        }
                    }
                }

                if (expiredIds != null && expiredIds.Count > 0)
                {
                    lock (_SyncResponsesLock)
                    {
                        foreach (string curr in expiredIds)
                        {
                            if (_SyncResponses.ContainsKey(curr)) _SyncResponses.Remove(curr);
                        }
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
                try
                {
                    if (_WTcpClient != null) _WTcpClient.Dispose();
                    if (_WWsClient != null) _WWsClient.Dispose();
                    if (_WWsSslClient != null) _WWsSslClient.Dispose();
                }
                catch (Exception)
                {

                }

                if (_CleanupSyncTokenSource != null)
                {
                    _CleanupSyncTokenSource.Cancel(true);
                } 

                return;
            }
        }

        private void InitializeClient()
        {
            _Random = new Random((int)DateTime.Now.Ticks);
            Connected = false;
            LoggedIn = false;
             
            if (String.IsNullOrEmpty(Config.Email)) Config.Email = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.ClientGUID)) Config.ClientGUID = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.Name)) Config.Name = RandomName();
            if (String.IsNullOrEmpty(Config.Password)) Config.Password = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(Config.ServerGUID)) Config.ServerGUID = "00000000-0000-0000-0000-000000000000";
               
            _SyncRequests = new Dictionary<string, DateTime>();
            _SyncResponses = new Dictionary<string, Message>();

            Connected = false;
            LoggedIn = false;
            Callbacks = new ClientCallbacks(); 
            
            if (Config.TcpServer.Enable)
            {
                _WTcpClient = new WatsonTcpClient(
                    Config.TcpServer.Ip,
                    Config.TcpServer.Port);

                _WTcpClient.ServerConnected = WatsonServerConnected;
                _WTcpClient.ServerDisconnected = WatsonServerDisconnected;
                _WTcpClient.MessageReceived = WatsonMessageReceived;
                _WTcpClient.Debug = Config.TcpServer.Debug;
                _WTcpClient.Start();

                Connection = ConnectionType.Tcp;
                Connected = true;
            }
            else if (Config.TcpSslServer.Enable)
            {
                _WTcpClient = new WatsonTcpClient(
                    Config.TcpSslServer.Ip,
                    Config.TcpSslServer.Port,
                    Config.TcpSslServer.PfxCertFile,
                    Config.TcpSslServer.PfxCertPassword);

                _WTcpClient.AcceptInvalidCertificates = Config.TcpSslServer.AcceptInvalidCerts;  
                _WTcpClient.ServerConnected = WatsonServerConnected;
                _WTcpClient.ServerDisconnected = WatsonServerDisconnected;
                _WTcpClient.MessageReceived = WatsonMessageReceived;
                _WTcpClient.Debug = Config.TcpServer.Debug;
                _WTcpClient.Start();

                Connection = ConnectionType.TcpSsl;
                Connected = true;
            }
            else if (Config.WebsocketServer.Enable)
            {
                _WWsClient = new WatsonWsClient(
                    Config.WebsocketServer.Ip,
                    Config.WebsocketServer.Port,
                    false,
                    false,
                    WatsonServerConnected,
                    WatsonServerDisconnected,
                    WatsonMessageReceived,
                    Config.WebsocketServer.Debug);

                Connection = ConnectionType.Websocket;
                Connected = true;
            }
            else if (Config.WebsocketSslServer.Enable)
            {
                _WWsSslClient = new WatsonWsClient(
                    Config.WebsocketSslServer.Ip,
                    Config.WebsocketSslServer.Port,
                    true,
                    Config.WebsocketSslServer.AcceptInvalidCerts,
                    WatsonServerConnected,
                    WatsonServerDisconnected,
                    WatsonMessageReceived,
                    Config.WebsocketSslServer.Debug);

                Connection = ConnectionType.WebsocketSsl;
                Connected = true;
            }
            else
            {
                throw new ArgumentException("Exactly one server must be enabled in the configuration file.");
            }
            
            _CleanupSyncTokenSource = new CancellationTokenSource();
            _CleanupSyncToken = _CleanupSyncTokenSource.Token;
            Task.Run(() => CleanupSyncRequests(), _CleanupSyncToken);
        }

        private void HandleMessage(Message currentMessage)
        {
            if (currentMessage.SenderGUID.Equals(Config.ServerGUID)
                && currentMessage.Command == MessageCommand.Event)
            {
                #region Server-Event-Message

                if (currentMessage.Data != null)
                {
                    #region Data-Exists

                    EventData ev = null;
                    try
                    {
                        ev = BigQ.Core.Common.DeserializeJson<EventData>(currentMessage.Data);
                    }
                    catch (Exception)
                    { 
                        return;
                    }
                     
                    switch (ev.EventType)
                    {
                        case EventTypes.ClientJoinedServer:
                            if (Callbacks.ClientJoinedServer != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.ClientJoinedServer(ev.Data.ToString());
                                }).Start();
                                // Task.Run(() => Callbacks.ClientJoinedServer(ev.Data.ToString()));
                            }
                            return;

                        case EventTypes.ClientLeftServer:
                            if (Callbacks.ClientLeftServer != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.ClientLeftServer(ev.Data.ToString());
                                }).Start();
                                // Task.Run(() => Callbacks.ClientLeftServer(ev.Data.ToString()));
                            }
                            return;

                        case EventTypes.ClientJoinedChannel:
                            if (Callbacks.ClientJoinedChannel != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.ClientJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID);
                                }).Start();
                                // Task.Run(() => Callbacks.ClientJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.ClientLeftChannel:
                            if (Callbacks.ClientLeftChannel != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.ClientLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID);
                                }).Start();
                                // Task.Run(() => Callbacks.ClientLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.SubscriberJoinedChannel:
                            if (Callbacks.SubscriberJoinedChannel != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.SubscriberJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID);
                                }).Start();
                                // Task.Run(() => Callbacks.SubscriberJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.SubscriberLeftChannel:
                            if (Callbacks.SubscriberLeftChannel != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.SubscriberLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID);
                                }).Start();
                                // Task.Run(() => Callbacks.SubscriberLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.ChannelCreated:
                            if (Callbacks.ChannelCreated != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.ChannelCreated(ev.Data.ToString());
                                }).Start();
                                // Task.Run(() => Callbacks.ChannelCreated(ev.Data.ToString()));
                            }
                            return;

                        case EventTypes.ChannelDestroyed:
                            if (Callbacks.ChannelDestroyed != null)
                            {
                                new Thread(delegate ()
                                {
                                    Callbacks.ChannelDestroyed(ev.Data.ToString());
                                }).Start();
                                // Task.Run(() => Callbacks.ChannelDestroyed(ev.Data.ToString()));
                            }
                            return;

                        default: 
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
            else if (currentMessage.SyncRequest)
            {
                #region Handle-Incoming-Sync-Request
                 
                if (Callbacks.SyncMessageReceived != null)
                {
                    byte[] ResponseData = Callbacks.SyncMessageReceived(currentMessage);

                    currentMessage.Success = true;
                    currentMessage.SyncRequest = false;
                    currentMessage.SyncResponse = true;
                    currentMessage.Data = ResponseData;
                    string tempGuid = String.Copy(currentMessage.SenderGUID);
                    currentMessage.SenderGUID = Config.ClientGUID;
                    currentMessage.RecipientGUID = tempGuid;

                    SendMessage(currentMessage); 
                }
                else
                { 
                    if (Callbacks.AsyncMessageReceived != null)
                    {
                        new Thread(delegate ()
                        {
                            Callbacks.AsyncMessageReceived(currentMessage);
                        }).Start();
                        // Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage));
                    } 
                }

                #endregion
            }
            else if (currentMessage.SyncResponse)
            {
                #region Handle-Incoming-Sync-Response
                 
                if (SyncRequestExists(currentMessage.MessageID))
                { 
                    if (!AddSyncResponse(currentMessage)) 
                    {
                        if (Callbacks.AsyncMessageReceived != null)
                        {
                            new Thread(delegate ()
                            {
                                Callbacks.AsyncMessageReceived(currentMessage);
                            }).Start();
                            // Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage));
                        } 
                    }
                }
                else
                { 
                    if (Callbacks.AsyncMessageReceived != null)
                    {
                        new Thread(delegate ()
                        {
                            Callbacks.AsyncMessageReceived(currentMessage);
                        }).Start();
                        // Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage));
                    } 
                }

                #endregion
            }
            else
            {
                #region Handle-Async
                 
                if (Callbacks.AsyncMessageReceived != null)
                {
                    new Thread(delegate ()
                    {
                        Callbacks.AsyncMessageReceived(currentMessage);
                    }).Start();
                    // Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage));
                } 

                #endregion
            }
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

        private bool SendPrivateMessageAsyncInternal(string guid, string contentType, byte[] data, bool persist)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));

            Message request = new Message();
            request.Email = Config.Email;
            request.Password = Config.Password;
            request.Command = MessageCommand.MessagePrivate;
            request.CreatedUtc = DateTime.Now.ToUniversalTime();
            request.MessageID = Guid.NewGuid().ToString();
            request.SenderGUID = Config.ClientGUID;
            request.SenderName = Config.Name;
            request.RecipientGUID = guid;
            request.ChannelGUID = null;
            request.Persist = persist;
            request.SyncRequest = false;
            request.SyncResponse = false;
            request.ContentType = contentType;
            request.ContentLength = data.Length;
            request.Data = data;

            return SendMessage(request);
        }

        #endregion
    }
}
