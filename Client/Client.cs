using System; 
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WatsonTcp;
using WatsonWebsocket;

using Newtonsoft.Json.Linq;

using BigQ.Client.Classes;

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
            if (_WTcpClient != null)
            {
                _WTcpClient.Dispose();
                _WTcpClient = null;
            }

            if (_WWsClient != null)
            {
                _WWsClient.Dispose();
                _WWsClient = null;
            }

            if (_CleanupSyncTokenSource != null)
            {
                if (!_CleanupSyncTokenSource.IsCancellationRequested) _CleanupSyncTokenSource.Cancel();
                _CleanupSyncTokenSource.Dispose();
                _CleanupSyncTokenSource = null;
            }

            Connected = false; 
            return;
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
            return SendMessage(request).Result;
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                LoggedIn = true;
                if (Callbacks.ServerConnected != null) Task.Run(() => Callbacks.ServerConnected());
                return true;
            }
        }

        /// <summary>
        /// Retrieve a list of all clients on the server.
        /// </summary>  
        /// <returns>The list of clients received from the server.</returns>
        public List<ServerClient> ListClients()
        {
            Message response = null;
            List<ServerClient> clients = new List<ServerClient>();

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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerSync(request, out response)) return null;
            if (response == null) return null;
            if (!response.Success) return null;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = Common.DeserializeJson<SuccessData>(response.Data);
                    clients = ((JArray)resp.Data).ToObject<List<ServerClient>>();
                }
                return clients;
            }
        }

        /// <summary>
        /// Retrieve a list of all channels on the server.
        /// </summary> 
        /// <returns>The list of channels received from the server.</returns>
        public List<Channel> ListChannels()
        {
            Message response = null;
            List<Channel> channels = new List<Channel>();

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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = null;
            request.Data = null;

            if (!SendServerSync(request, out response)) return null;
            if (response == null) return null;
            if (!response.Success) return null;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = Common.DeserializeJson<SuccessData>(response.Data);
                    channels = ((JArray)resp.Data).ToObject<List<Channel>>();
                }
                return channels;
            }
        }

        /// <summary>
        /// Retrieve a list of all members in a specific channel.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param> 
        /// <returns>The list of clients that are members in the specified channel on the server.</returns>
        public List<ServerClient> ListMembers(string guid)
        {
            Message response = null;
            List<ServerClient> clients = new List<ServerClient>();

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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerSync(request, out response)) return null;
            if (response == null) return null;
            if (!response.Success) return null;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = Common.DeserializeJson<SuccessData>(response.Data);
                    if (resp != null && resp.Data != null)
                    {
                        clients = ((JArray)resp.Data).ToObject<List<ServerClient>>();
                    }
                    else
                    {
                        clients = new List<ServerClient>();
                    }
                }

                return clients;
            }
        }

        /// <summary>
        /// Retrieve a list of all subscribers in a specific channel.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param> 
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public List<ServerClient> ListSubscribers(string guid)
        {
            Message response = null;
            List<ServerClient> clients = new List<ServerClient>();

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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerSync(request, out response)) return null;
            if (response == null) return null;
            if (!response.Success) return null;
            else
            {
                if (response.Data != null)
                {
                    SuccessData resp = Common.DeserializeJson<SuccessData>(response.Data);
                    clients = ((JArray)resp.Data).ToObject<List<ServerClient>>();
                }

                return clients;
            }
        }

        /// <summary>
        /// Join a specified channel on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool Join(string guid, out Message response)
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Subscribe to multicast messages on a specified channel on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool Subscribe(string guid, out Message response)
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Leave a channel on the server to which you are joined.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool Leave(string guid, out Message response)
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Unsubscribe from multicast messages on a specified channel on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool Unsubscribe(string guid, out Message response)
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Create a channel.
        /// </summary>
        /// <param name="channelType">ChannelType.</param>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="isPrivate">Whether or not the channel is private (true) or public (false).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool Create(ChannelType channelType, string name, bool isPrivate, out Message response)
        {
            response = null;
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name)); 

            Channel currentChannel = new Channel();
            currentChannel.ChannelName = name;
            currentChannel.OwnerGUID = Config.ClientGUID;
            currentChannel.ChannelGUID = Guid.NewGuid().ToString();
            currentChannel.Type = channelType;

            if (isPrivate) currentChannel.Visibility = ChannelVisibility.Private;
            else currentChannel.Visibility = ChannelVisibility.Public;
            
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = currentChannel.ChannelGUID;
            request.Data = Encoding.UTF8.GetBytes(Common.SerializeJson(currentChannel));

            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }
         
        /// <summary>
        /// Delete a channel you own on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool Delete(string guid, out Message response)
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.ChannelGUID = guid;
            request.Data = null;

            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            return response.Success;
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public async Task<bool> Send(string guid, string data)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return await SendInternal(guid, null, Encoding.UTF8.GetBytes(data), false);
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public async Task<bool> Send(string guid, byte[] data)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));
            return await SendInternal(guid, null, data, false);
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously, and queue persistently until expiration.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="contentType">The content type of the data.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="persist">True to persist until expiration.</param>
        /// <returns>Boolean indicating whether or not the message was stored persistently.</returns>
        public async Task<bool> Send(string guid, string contentType, string data, bool persist)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return await SendInternal(guid, contentType, Encoding.UTF8.GetBytes(data), persist);
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously, and queue persistently until expiration.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="contentType">The content type of the data.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="persist">True to persist until expiration.</param>
        /// <returns>Boolean indicating whether or not the message was stored persistently.</returns>
        public async Task<bool> Send(string guid, string contentType, byte[] data, bool persist)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (data == null) throw new ArgumentNullException(nameof(data));
            return await SendInternal(guid, contentType, data, persist);
        }

        /// <summary>
        /// Send a private message to another user on this server synchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendSync(string guid, string data, out string response)
        {
            response = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));

            byte[] responseBytes = null;
            if (!SendSync(guid, Encoding.UTF8.GetBytes(data), out responseBytes))
            {
                return false;
            }
            else
            {
                if (responseBytes != null)
                {
                    response = Encoding.UTF8.GetString(responseBytes);
                }

                return true;
            }
        }

        /// <summary>
        /// Send a private message to another user on this server synchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendSync(string guid, byte[] data, out byte[] response)
        {
            response = null;

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
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.SyncResponse = false;
            request.Data = data;

            if (!AddSyncRequest(request.MessageID)) return false;
            if (!SendMessage(request).Result) return false;
            int timeoutMs = Config.SyncTimeoutMs;
            if (request.SyncTimeoutMs > 0) timeoutMs = Convert.ToInt32(request.SyncTimeoutMs);

            Message responseMsg = null;
            if (!GetSyncResponse(request.MessageID, timeoutMs, out responseMsg))
            {
                return false;
            }
            else
            {
                if (responseMsg.Data != null && responseMsg.Data.Length > 0)
                {
                    response = new byte[responseMsg.Data.Length];
                    Buffer.BlockCopy(responseMsg.Data, 0, response, 0, responseMsg.Data.Length);
                }

                return true;
            }
        }

        /// <summary>
        /// Send an async message to a channel, which is in turn sent to recipients based on channel configuration.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public async Task<bool> SendChannel(string guid, string data)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return await SendChannel(guid, Encoding.UTF8.GetBytes(data));
        }

        /// <summary>
        /// Send an async message to a channel, which is in turn sent to recipients based on channel configuration.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public async Task<bool> SendChannel(string guid, byte[] data)
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
            return await SendMessage(request);
        }

        /// <summary>
        /// Send a sync message to a channel, which is in turn sent to recipients based on channel configuration. 
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelSync(string guid, string data, out string response)
        {
            response = null;

            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));

            byte[] responseBytes = null; 
            if (!SendChannelSync(guid, Encoding.UTF8.GetBytes(data), out responseBytes))
            {
                return false;
            }
            else
            {
                if (responseBytes != null && responseBytes.Length > 0)
                {
                    response = Encoding.UTF8.GetString(responseBytes);
                }

                return true;
            }
        }

        /// <summary>
        /// Send a sync message to a channel, which is in turn sent to recipients based on channel configuration. 
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelSync(string guid, byte[] data, out byte[] response)
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
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.Data = data;

            if (!AddSyncRequest(request.MessageID)) return false;
            if (!SendMessage(request).Result) return false;
            int timeoutMs = Config.SyncTimeoutMs;
            if (request.SyncTimeoutMs > 0) timeoutMs = request.SyncTimeoutMs;

            Message responseMsg = null;
            if (!GetSyncResponse(request.MessageID, timeoutMs, out responseMsg))
            {
                return false;
            }
            else
            {
                if (responseMsg.Data != null && responseMsg.Data.Length > 0)
                {
                    response = new byte[responseMsg.Data.Length];
                    Buffer.BlockCopy(responseMsg.Data, 0, response, 0, responseMsg.Data.Length);
                }

                return true;
            }
        }
         
        /// <summary>
        /// Discern whether or not a given client is connected.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param> 
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool IsClientConnected(string guid)
        { 
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

            Message response = null;
            if (!SendServerSync(request, out response)) return false;
            if (response == null) return false;
            if (!response.Success) return false;
            else
            {
                if (response.Data != null)
                {
                    SuccessData ret = Common.DeserializeJson<SuccessData>(Encoding.UTF8.GetString(response.Data));
                    return (ret.Success && Convert.ToBoolean(ret.Data));
                }

                return false;
            }
        }

        #endregion

        #region Private-Watson-Methods

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private async Task WatsonServerConnected()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        { 
            Connected = true;
        }

        private async Task WatsonServerDisconnected()
        { 
            Connected = false;
            LoggedIn = false;
            if (Callbacks.ServerDisconnected != null)
            {
                await Task.Run(() => Callbacks.ServerDisconnected());
            }
        }

        private async Task WatsonMessageReceived(byte[] data)
        {
            Message curr = new Message(data); 
            await HandleMessage(curr);
        }
        
        #endregion

        #region Private-Sync-Methods

        //
        // Ensure that none of these methods call another method within this region
        // otherwise you have a lock within a lock!  There should be NO methods
        // outside of this region that have a lock statement
        //
         
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
                Connection = ConnectionType.Tcp;

                _WTcpClient = new WatsonTcpClient(
                    Config.TcpServer.Ip,
                    Config.TcpServer.Port);

                _WTcpClient.ServerConnected = WatsonServerConnected;
                _WTcpClient.ServerDisconnected = WatsonServerDisconnected;
                _WTcpClient.MessageReceived = WatsonMessageReceived;
                _WTcpClient.Debug = Config.TcpServer.Debug;
                _WTcpClient.Start();

                Connected = true;
            }
            else if (Config.TcpSslServer.Enable)
            {
                Connection = ConnectionType.TcpSsl;

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

                Connected = true;
            }
            else if (Config.WebsocketServer.Enable)
            {
                Connection = ConnectionType.Websocket;

                _WWsClient = new WatsonWsClient(
                    Config.WebsocketServer.Ip,
                    Config.WebsocketServer.Port,
                    false);

                _WWsClient.ServerConnected = WatsonServerConnected;
                _WWsClient.ServerDisconnected = WatsonServerDisconnected;
                _WWsClient.MessageReceived = WatsonMessageReceived;
                _WWsClient.Debug = Config.WebsocketServer.Debug;
                _WWsClient.Start();

                Connected = true;
            }
            else if (Config.WebsocketSslServer.Enable)
            {
                Connection = ConnectionType.WebsocketSsl;

                _WWsClient = new WatsonWsClient(
                    Config.WebsocketSslServer.Ip,
                    Config.WebsocketSslServer.Port,
                    true);

                _WWsClient.ServerConnected = WatsonServerConnected;
                _WWsClient.ServerDisconnected = WatsonServerDisconnected;
                _WWsClient.MessageReceived = WatsonMessageReceived;

                _WWsClient.AcceptInvalidCertificates = Config.WebsocketSslServer.AcceptInvalidCerts;
                _WWsClient.Debug = Config.WebsocketSslServer.Debug;
                _WWsClient.Start();
                 
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

        private async Task HandleMessage(Message currentMessage)
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
                        ev = Common.DeserializeJson<EventData>(currentMessage.Data);
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
                                await Task.Run(() => Callbacks.ClientJoinedServer(ev.Data.ToString()));
                            }
                            return;

                        case EventTypes.ClientLeftServer:
                            if (Callbacks.ClientLeftServer != null)
                            {
                                await Task.Run(() => Callbacks.ClientLeftServer(ev.Data.ToString()));
                            }
                            return;

                        case EventTypes.ClientJoinedChannel:
                            if (Callbacks.ClientJoinedChannel != null)
                            {
                                await Task.Run(() => Callbacks.ClientJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.ClientLeftChannel:
                            if (Callbacks.ClientLeftChannel != null)
                            {
                                await Task.Run(() => Callbacks.ClientLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.SubscriberJoinedChannel:
                            if (Callbacks.SubscriberJoinedChannel != null)
                            {
                                await Task.Run(() => Callbacks.SubscriberJoinedChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.SubscriberLeftChannel:
                            if (Callbacks.SubscriberLeftChannel != null)
                            {
                                await Task.Run(() => Callbacks.SubscriberLeftChannel(ev.Data.ToString(), currentMessage.ChannelGUID));
                            }
                            return;

                        case EventTypes.ChannelCreated:
                            if (Callbacks.ChannelCreated != null)
                            {
                                await Task.Run(() => Callbacks.ChannelCreated(ev.Data.ToString()));
                            }
                            return;

                        case EventTypes.ChannelDestroyed:
                            if (Callbacks.ChannelDestroyed != null)
                            {
                                await Task.Run(() => Callbacks.ChannelDestroyed(ev.Data.ToString()));
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
                    byte[] ResponseData = await Callbacks.SyncMessageReceived(currentMessage);

                    currentMessage.Success = true;
                    currentMessage.SyncRequest = false;
                    currentMessage.SyncResponse = true;
                    currentMessage.Data = ResponseData;
                    string tempGuid = String.Copy(currentMessage.SenderGUID);
                    currentMessage.SenderGUID = Config.ClientGUID;
                    currentMessage.RecipientGUID = tempGuid;

                    await SendMessage(currentMessage); 
                }
                else
                { 
                    if (Callbacks.AsyncMessageReceived != null)
                    {
                        await Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage)); 
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
                            await Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage));
                        } 
                    }
                }
                else
                { 
                    if (Callbacks.AsyncMessageReceived != null)
                    {
                        await Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage));
                    } 
                }

                #endregion
            }
            else
            {
                #region Handle-Async
                 
                if (Callbacks.AsyncMessageReceived != null)
                {
                    await Task.Run(() => Callbacks.AsyncMessageReceived(currentMessage)); 
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

        private async Task<bool> SendInternal(string guid, string contentType, byte[] data, bool persist)
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

            return await SendMessage(request);
        }

        private async Task<bool> SendMessage(Message message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));

            byte[] data = message.ToBytes();

            if (Config.TcpServer.Enable || Config.TcpSslServer.Enable)
            {
                return await _WTcpClient.SendAsync(data);
            }
            else if (Config.WebsocketServer.Enable || Config.WebsocketSslServer.Enable)
            {
                return await _WWsClient.SendAsync(data);
            }
            else
            {
                return false;
            }
        }
          
        private bool SendServerSync(Message request, out Message response)
        {
            response = null;
            if (request == null) throw new ArgumentNullException(nameof(request));
            if (String.IsNullOrEmpty(request.MessageID)) request.MessageID = Guid.NewGuid().ToString();
            request.SyncRequest = true;
            request.SyncTimeoutMs = Config.SyncTimeoutMs;
            request.ExpirationUtc = DateTime.Now.ToUniversalTime().AddMilliseconds(Config.SyncTimeoutMs);
            request.RecipientGUID = Config.ServerGUID;

            if (!AddSyncRequest(request.MessageID)) return false;
            if (!SendMessage(request).Result) return false;
            int timeoutMs = Config.SyncTimeoutMs;
            if (request.SyncTimeoutMs > 0) timeoutMs = request.SyncTimeoutMs;

            return GetSyncResponse(request.MessageID, timeoutMs, out response);
        }

        #endregion
    }
}
