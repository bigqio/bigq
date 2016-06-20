﻿using System;
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
    /// <summary>
    /// Object containing metadata about a client on BigQ.
    /// </summary>
    [Serializable]
    public class BigQClient
    {
        #region Public-Class-Members

        /// <summary>
        /// The email address associated with the client.
        /// </summary>
        public string Email;

        /// <summary>
        /// The password associated with the client.  Reserved for future use.
        /// </summary>
        public string Password;

        /// <summary>
        /// The GUID associated with the client.
        /// </summary>
        public string ClientGuid;

        /// <summary>
        /// The client's source IP address.
        /// </summary>
        public string SourceIp;

        /// <summary>
        /// The source TCP port number used by the client.
        /// </summary>
        public int SourcePort;

        /// <summary>
        /// The server IP address or hostname to which this client connects.
        /// </summary>
        public string ServerIp;

        /// <summary>
        /// The server TCP port to which this client connects.
        /// </summary>
        public int ServerPort;

        /// <summary>
        /// The UTC timestamp of when this client object was created.
        /// </summary>
        public DateTime? CreatedUTC;

        /// <summary>
        /// The UTC timestamp of when this client object was last updated.
        /// </summary>
        public DateTime? UpdatedUTC;

        /// <summary>
        /// Indicates whether or not the client is using raw TCP sockets for messaging.
        /// </summary>
        public bool IsTCP;

        /// <summary>
        /// Indicates whether or not the client is using websockets for messaging.
        /// </summary>
        public bool IsWebsocket;

        /// <summary>
        /// The TcpClient for this client.  Provides direct access to the underlying socket.
        /// </summary>
        public TcpClient ClientTCPInterface;

        /// <summary>
        /// The HttpListenerContext for this client.  Provides direct access to the underlying HTTP listener context object.
        /// </summary>
        public HttpListenerContext ClientHTTPContext;

        /// <summary>
        /// The WebSocketContext for this client.  Provides direct access to the underlying WebSocket context.
        /// </summary>
        public WebSocketContext ClientWSContext;

        /// <summary>
        /// The WebSocket for this client.  Provides direct access to the underlying WebSocket object.
        /// </summary>
        public WebSocket ClientWSInterface;

        /// <summary>
        /// Indicates whether or not the client is connected to the server.
        /// </summary>
        public bool Connected;

        /// <summary>
        /// Indicates whether or not the client is logged in to the server.
        /// </summary>
        public bool LoggedIn;
        
        /// <summary>
        /// Set this value to true to send log messages containing response time for processed messages (requires that ou enable console debugging or message logging).
        /// </summary>
        public bool LogMessageResponseTime = false;

        /// <summary>
        /// Set this value to true to allow BigQ to send messages to the console for logging purposes.
        /// </summary>
        public bool ConsoleDebug;

        #endregion

        #region Private-Class-Members

        private int HeartbeatIntervalMsec;
        private int MaxHeartbeatFailures;
        private CancellationTokenSource DataReceiverTokenSource = null;
        private CancellationToken DataReceiverToken;
        private CancellationTokenSource CleanupSyncTokenSource = null;
        private CancellationToken CleanupSyncToken;
        private CancellationTokenSource HeartbeatTokenSource = null;
        private CancellationToken HeartbeatToken;
        private ConcurrentDictionary<string, DateTime> SyncRequests;
        private ConcurrentDictionary<string, BigQMessage> SyncResponses;
        private int SyncTimeoutMsec;

        #endregion

        #region Public-Delegates

        public Func<BigQMessage, bool> AsyncMessageReceived;
        public Func<BigQMessage, byte[]> SyncMessageReceived;
        public Func<bool> ServerDisconnected;
        public Func<string, bool> LogMessage;

        #endregion

        #region Public-Constructors

        /// <summary>
        /// This constructor is used by BigQServer.  Do not use it in client applications!
        /// </summary>
        public BigQClient()
        {
        }

        /// <summary>
        /// Create a BigQ client instance and connect to the server.
        /// </summary>
        /// <param name="email">The email address for the client (null is acceptable).</param>
        /// <param name="guid">The GUID for the client (null is acceptable).</param>
        /// <param name="ip">The IP address or hostname for the server (must not be null).</param>
        /// <param name="port">The TCP port number of the server (must be greater than zero).</param>
        /// <param name="syncTimeoutMsec">The number of milliseconds before timing out a synchronous message request.</param>
        /// <param name="heartbeatIntervalMsec">The number of milliseconds before timing out a heartbeat message to the server.</param>
        /// <param name="debug">Indicates whether or not console debugging is used.</param>
        public BigQClient(
            string email, 
            string guid, 
            string ip, 
            int port, 
            int syncTimeoutMsec,
            int heartbeatIntervalMsec,
            bool debug)
        {
            #region Check-for-Null-or-Invalid-Values

            if (String.IsNullOrEmpty(ip)) throw new ArgumentNullException("ip");
            if (port < 1) throw new ArgumentOutOfRangeException("port");
            if (syncTimeoutMsec < 1000) throw new ArgumentOutOfRangeException("syncTimeoutMsec");
            if (heartbeatIntervalMsec < 100 && heartbeatIntervalMsec != 0) throw new ArgumentOutOfRangeException("heartbeatIntervalMsec");

            #endregion

            #region Set-Class-Variables

            if (String.IsNullOrEmpty(guid)) ClientGuid = Guid.NewGuid().ToString(); 
            else ClientGuid = guid;

            if (String.IsNullOrEmpty(email)) Email = ClientGuid;
            else Email = email;

            ServerIp = ip;
            ServerPort = port;
            ConsoleDebug = debug;
            SyncRequests = new ConcurrentDictionary<string, DateTime>();
            SyncResponses = new ConcurrentDictionary<string, BigQMessage>();
            SyncTimeoutMsec = syncTimeoutMsec;
            HeartbeatIntervalMsec = heartbeatIntervalMsec;
            MaxHeartbeatFailures = 5;
            Connected = false;
            LoggedIn = false;

            #endregion

            #region Set-Delegates-to-Null

            AsyncMessageReceived = null;
            SyncMessageReceived = null;
            ServerDisconnected = null;
            LogMessage = null;

            #endregion

            #region Start-Client

            //
            // see https://social.msdn.microsoft.com/Forums/vstudio/en-US/2281199d-cd28-4b5c-95dc-5a888a6da30d/tcpclientconnect-timeout?forum=csharpgeneral
            // removed using statement since resources are managed by the caller
            //
            ClientTCPInterface = new TcpClient();
            IAsyncResult ar = ClientTCPInterface.BeginConnect(ip, port, null, null);
            WaitHandle wh = ar.AsyncWaitHandle;

            try
            {
                if (!ar.AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(5), false))
                {
                    ClientTCPInterface.Close();
                    throw new TimeoutException("Timeout connecting to " + ip + ":" + port);
                }

                ClientTCPInterface.EndConnect(ar);

                SourceIp = ((IPEndPoint)ClientTCPInterface.Client.LocalEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)ClientTCPInterface.Client.LocalEndPoint).Port;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                wh.Close();
            }
            
            Connected = true;

            #endregion

            #region Stop-Existing-Tasks

            if (DataReceiverTokenSource != null) DataReceiverTokenSource.Cancel();
            if (CleanupSyncTokenSource != null) CleanupSyncTokenSource.Cancel();
            if (HeartbeatTokenSource != null) HeartbeatTokenSource.Cancel();

            #endregion

            #region Start-Tasks

            DataReceiverTokenSource = new CancellationTokenSource();
            DataReceiverToken = DataReceiverTokenSource.Token;
            Task.Run(() => TCPDataReceiver(), DataReceiverToken);
            
            CleanupSyncTokenSource = new CancellationTokenSource();
            CleanupSyncToken = CleanupSyncTokenSource.Token;
            Task.Run(() => CleanupSyncRequests(), CleanupSyncToken);

            //
            //
            //
            // do not start heartbeat until successful login
            // design goal: server needs to free up connections fast
            // client just needs to keep socket open
            //
            //
            //
            // HeartbeatTokenSource = new CancellationTokenSource();
            // HeartbeatToken = HeartbeatTokenSource.Token;
            // Task.Run(() => TCPHeartbeatManager());

            #endregion
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Sends a message; makes the assumption that you have populated the object fully and correctly.  In general, this method should not be used.
        /// </summary>
        /// <param name="message">The populated message object to send.</param>
        /// <returns></returns>
        public bool SendRawMessage(BigQMessage message)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (message == null) throw new ArgumentNullException("message");
                return TCPDataSender(message);
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendRawMessage " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Generates an echo request to the server, which should result in an asynchronous echo response.  Typically used to validate connectivity.
        /// </summary>
        /// <returns></returns>
        public bool Echo()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "Echo";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = null;
                request.Data = null;
                return TCPDataSender(request);
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("Echo " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Login to the server.
        /// </summary>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating if the login was successful.</returns>
        public bool Login(out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "Login";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = null;
                request.Data = null;

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** Login unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** Login null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** Login failed with response data " + response.Data.ToString());
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

                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("Login " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Retrieve a list of all clients on the server.
        /// </summary>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="clients">The list of clients received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListClients(out BigQMessage response, out List<BigQClient> clients)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                clients = null;

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "ListClients";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = null;
                request.Data = null;

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** ListClients unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** ListClients null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** ListClients failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    if (response.Data != null)
                    {
                        if (LogMessageResponseTime) Log("ListClients deserialize start " + sw.Elapsed.TotalMilliseconds + "ms");
                        clients = BigQHelper.DeserializeJson<List<BigQClient>>(response.Data, false);
                    }
                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ListClients " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Retrieve a list of all channels on the server.
        /// </summary>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="channels">The list of channels received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListChannels(out BigQMessage response, out List<BigQChannel> channels)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                channels = null;

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "ListChannels";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = null;
                request.Data = null;

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** ListChannels unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** ListChannels null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** ListChannels failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    if (response.Data != null)
                    {
                        if (LogMessageResponseTime) Log("ListChanels deserialize start " + sw.Elapsed.TotalMilliseconds + "ms");
                        channels = BigQHelper.DeserializeJson<List<BigQChannel>>(response.Data, false);
                    }
                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ListChannels " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Retrieve a list of all subscribers in a specific channel.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <param name="clients">The list of clients subscribed to the specified channel on the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool ListChannelSubscribers(string guid, out BigQMessage response, out List<BigQClient> clients)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                clients = null;

                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "ListChannelSubscribers";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = guid;
                request.Data = null;

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** ListChannelSubscribers unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** ListChannelSubscribers null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** ListChannelSubscribers failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    if (response.Data != null)
                    {
                        if (LogMessageResponseTime) Log("ListChannelSubscribers deserialize start " + sw.Elapsed.TotalMilliseconds + "ms");
                        clients = BigQHelper.DeserializeJson<List<BigQClient>>(response.Data, false);
                    }
                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("ListChannelSubscribers " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Join a specified channel on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool JoinChannel(string guid, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "JoinChannel";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = guid;
                request.Data = null;

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** JoinChannel unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** JoinChannel null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** JoinChannel failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("JoinChannel " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Leave a channel on the server to which you are joined.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool LeaveChannel(string guid, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "LeaveChannel";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = guid;
                request.Data = null;

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** LeaveChannel unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** LeaveChannel null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** LeaveChannel failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("LeaveChannel " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Create a channel on the server.
        /// </summary>
        /// <param name="name">The name you wish to assign to the new channel.</param>
        /// <param name="priv">Whether or not the channel is private (1) or public (0).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool CreateChannel(string name, int priv, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                if (String.IsNullOrEmpty(name)) throw new ArgumentNullException("name");
                if (priv != 0 && priv != 1) throw new ArgumentOutOfRangeException("Value for priv must be 0 or 1");

                BigQChannel CurrentChannel = new BigQChannel();
                CurrentChannel.ChannelName = name;
                CurrentChannel.OwnerGuid = ClientGuid;
                CurrentChannel.Guid = Guid.NewGuid().ToString();
                CurrentChannel.Private = priv;

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "CreateChannel";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = CurrentChannel.Guid;
                request.Data = Encoding.UTF8.GetBytes(BigQHelper.SerializeJson(CurrentChannel));

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** CreateChannel unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** CreateChannel null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** CreateChannel failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("CreateChannel " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Delete a channel you own on the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool DeleteChannel(string guid, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "DeleteChannel";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = null;
                request.Data = null;

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** DeleteChannel unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** DeleteChannel null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** DeleteChannel failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    return true;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("DeleteChannel " + sw.Elapsed.TotalMilliseconds + "ms");
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
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
                if (String.IsNullOrEmpty(data)) throw new ArgumentNullException("data");
                return SendPrivateMessageAsync(guid, Encoding.UTF8.GetBytes(data));
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendPrivateMessageAsync (string) " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Send a private message to another user on this server asynchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendPrivateMessageAsync(string guid, byte[] data)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
                if (data == null) throw new ArgumentNullException("data");

                BigQMessage CurrentMessage = new BigQMessage();
                CurrentMessage.Email = Email;
                CurrentMessage.Password = Password;
                CurrentMessage.Command = null;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.MessageId = Guid.NewGuid().ToString();
                CurrentMessage.SenderGuid = ClientGuid;
                CurrentMessage.RecipientGuid = guid;
                CurrentMessage.ChannelGuid = null;
                CurrentMessage.Data = data;
                return TCPDataSender(CurrentMessage);
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendPrivateMessageAsync (byte) " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Send a private message to another user on this server synchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendPrivateMessageSync(string guid, string data, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
                if (String.IsNullOrEmpty(data)) throw new ArgumentNullException("data");
                return SendPrivateMessageSync(guid, Encoding.UTF8.GetBytes(data), out response);
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendPrivateMessageSync (string) " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Send a private message to another user on this server synchronously.
        /// </summary>
        /// <param name="guid">The GUID of the recipient user.</param>
        /// <param name="data">The data you wish to send to the user (string or byte array).</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendPrivateMessageSync(string guid, byte[] data, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            BigQMessage CurrentMessage = new BigQMessage();
            
            try
            {
                response = null;

                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
                if (data == null) throw new ArgumentNullException("data");

                CurrentMessage = new BigQMessage();
                CurrentMessage.Email = Email;
                CurrentMessage.Password = Password;
                CurrentMessage.Command = null;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.MessageId = Guid.NewGuid().ToString();
                CurrentMessage.SenderGuid = ClientGuid;
                CurrentMessage.RecipientGuid = guid;
                CurrentMessage.ChannelGuid = null;
                CurrentMessage.SyncRequest = true;
                CurrentMessage.Data = data;

                if (!AddSyncRequest(CurrentMessage.MessageId))
                {
                    Log("*** SendPrivateMessageSync unable to register sync request GUID " + CurrentMessage.MessageId);
                    return false;
                }

                if (!TCPDataSender(CurrentMessage))
                {
                    Log("*** SendPrivateMessage unable to send message GUID " + CurrentMessage.MessageId + " to recipient " + CurrentMessage.RecipientGuid);
                    return false;
                }

                BigQMessage ResponseMessage = new BigQMessage();
                if (!GetSyncResponse(CurrentMessage.MessageId, out ResponseMessage))
                {
                    Log("*** SendPrivateMessage unable to get response for message GUID " + CurrentMessage.MessageId);
                    return false;
                }

                if (ResponseMessage != null)
                {
                    response = ResponseMessage;
                }
                return true;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendPrivateMessageSync (byte) " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Send a private message to the server asynchronously.
        /// </summary>
        /// <param name="request">The message object you wish to send to the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendServerMessageAsync(BigQMessage request)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (request == null) throw new ArgumentNullException("request");
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                return TCPDataSender(request);
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendServerMessageAsync " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Send a private message to the server synchronously.
        /// </summary>
        /// <param name="request">The message object you wish to send to the server.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendServerMessageSync(BigQMessage request, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            
            try
            {
                response = null;
            
                if (request == null) throw new ArgumentNullException("request");
                if (String.IsNullOrEmpty(request.MessageId)) request.MessageId = Guid.NewGuid().ToString();
                request.SyncRequest = true;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";

                if (!AddSyncRequest(request.MessageId))
                {
                    Log("*** SendServerMessageSync unable to register sync request GUID " + request.MessageId);
                    return false;
                }
                else
                {
                    // Log("SendServerMessageSync registered sync request GUID " + request.MessageId);
                }

                if (!TCPDataSender(request))
                {
                    Log("*** SendServerMessageSync unable to send message GUID " + request.MessageId + " to server");
                    return false;
                }
                else
                {
                    // Log("SendServerMessageSync sent message GUID " + request.MessageId + " to server");
                }

                BigQMessage ResponseMessage = new BigQMessage();
                if (!GetSyncResponse(request.MessageId, out ResponseMessage))
                {
                    Log("*** SendServerMessageSync unable to get response for message GUID " + request.MessageId);
                    return false;
                }
                else
                {
                    // Log("SendServerMessageSync received response for message GUID " + request.MessageId);
                }
                
                if (ResponseMessage != null) response = ResponseMessage;
                return true;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendServerMessageSync " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Send a message to a channel, which is in turn sent to each subscriber of that channel.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelMessage(string guid, string data)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
                if (String.IsNullOrEmpty(data)) throw new ArgumentNullException("data");
                return SendChannelMessage(guid, Encoding.UTF8.GetBytes(data));
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendChannelMessage (string) " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Send a message to a channel, which is in turn sent to each subscriber of that channel.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <param name="data">The data you wish to send to the channel (string or byte array).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool SendChannelMessage(string guid, byte[] data)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
                if (data == null) throw new ArgumentNullException("data");

                BigQMessage CurrentMessage = new BigQMessage();
                CurrentMessage.Email = Email;
                CurrentMessage.Password = Password;
                CurrentMessage.Command = null;
                CurrentMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
                CurrentMessage.MessageId = Guid.NewGuid().ToString();
                CurrentMessage.SenderGuid = ClientGuid;
                CurrentMessage.RecipientGuid = null;
                CurrentMessage.ChannelGuid = guid;
                CurrentMessage.Data = data;
                return TCPDataSender(CurrentMessage);
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("SendChannelMessage (byte) " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Retrieve the list of synchronous requests awaiting responses.
        /// </summary>
        /// <param name="response">A dictionary containing the GUID of the synchronous request (key) and the timestamp it was sent (value).</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool PendingSyncRequests(out Dictionary<string, DateTime> response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;
                if (SyncRequests == null) return true;
                if (SyncRequests.Count < 1) return true;

                response = SyncRequests.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                return true;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("PendingSyncRequests " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Discern whether or not a given client is connected.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param>
        /// <param name="response">The full response message received from the server.</param>
        /// <returns>Boolean indicating whether or not the call succeeded.</returns>
        public bool IsClientConnected(string guid, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                response = null;

                BigQMessage request = new BigQMessage();
                request.Email = Email;
                request.Password = Password;
                request.Command = "IsClientConnected";
                request.CreatedUTC = DateTime.Now.ToUniversalTime();
                request.MessageId = Guid.NewGuid().ToString();
                request.SenderGuid = ClientGuid;
                request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
                request.SyncRequest = true;
                request.ChannelGuid = null;
                request.Data = Encoding.UTF8.GetBytes(guid);

                if (!SendServerMessageSync(request, out response))
                {
                    Log("*** ListClients unable to retrieve server response");
                    return false;
                }

                if (response == null)
                {
                    Log("*** ListClients null response from server");
                    return false;
                }

                if (!BigQHelper.IsTrue(response.Success))
                {
                    Log("*** ListClients failed with response data " + response.Data.ToString());
                    return false;
                }
                else
                {
                    if (response.Data != null)
                    {
                        return BigQHelper.IsTrue(Encoding.UTF8.GetString(response.Data));
                    }
                    return false;
                }
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("IsClientConnected " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }

        /// <summary>
        /// Retrieve a structured string containing the IP address and port of the client in the format of 10.1.142.12:31763.
        /// </summary>
        /// <returns>Formatted string containing the IP address and port of the client.</returns>
        public string IpPort()
        {
            return SourceIp + ":" + SourcePort;
        }

        /// <summary>
        /// Close and dispose of client resources.
        /// </summary>
        public void Close()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                if (DataReceiverTokenSource != null) DataReceiverTokenSource.Cancel();
                if (CleanupSyncTokenSource != null) CleanupSyncTokenSource.Cancel();
                if (HeartbeatTokenSource != null) HeartbeatTokenSource.Cancel();

                if (ClientTCPInterface != null)
                {
                    if (ClientTCPInterface.Connected)
                    {
                        //
                        // close the TCP stream
                        //
                        if (ClientTCPInterface.GetStream() != null)
                        {
                            ClientTCPInterface.GetStream().Close();
                        }
                    }

                    // 
                    // close the client
                    //

                    if (ClientTCPInterface != null) ClientTCPInterface.Close();
                }

                ClientTCPInterface = null;
                return;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("Close " + sw.Elapsed.TotalMilliseconds + "ms");
            }
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
                Log("*** SyncResponseReady null GUID supplied");
                return false;
            }

            if (SyncResponses == null)
            {
                Log("*** SyncResponseReady null sync responses list, initializing");
                SyncResponses = new ConcurrentDictionary<string, BigQMessage>();
                return false;
            }

            if (SyncResponses.Count < 1)
            {
                Log("*** SyncResponseReady no entries in sync responses list");
                return false;
            }

            if (SyncResponses.ContainsKey(guid))
            {
                Log("SyncResponseReady found sync response for GUID " + guid);
                return true;
            }

            Log("*** SyncResponseReady no sync response for GUID " + guid);
            return false;
        }

        private bool AddSyncRequest(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                Log("*** AddSyncRequest null GUID supplied");
                return false;
            }

            if (SyncRequests == null)
            {
                Log("*** AddSyncRequest null sync requests list, initializing");
                SyncRequests = new ConcurrentDictionary<string, DateTime>();
            }

            if (SyncRequests.ContainsKey(guid))
            {
                Log("*** AddSyncRequest already contains an entry for GUID " + guid);
                return false;
            }

            SyncRequests.TryAdd(guid, DateTime.Now);
            Log("AddSyncRequest added request for GUID " + guid + ": " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss"));
            return true;
        }

        private bool RemoveSyncRequest(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                Log("*** RemoveSyncRequest null GUID supplied");
                return false;
            }

            if (SyncRequests == null)
            {
                Log("*** RemoveSyncRequest null sync requests list, initializing");
                SyncRequests = new ConcurrentDictionary<string, DateTime>();
                return false;
            }

            DateTime TempDateTime;

            if (SyncRequests.ContainsKey(guid))
            {
                SyncRequests.TryRemove(guid, out TempDateTime);
            }

            Log("RemoveSyncRequest removed sync request for GUID " + guid);
            return true;
        }

        private bool SyncRequestExists(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                Log("*** SyncRequestExists null GUID supplied");
                return false;
            }
            
            if (SyncRequests == null)
            {
                Log("*** SyncRequestExists null sync requests list, initializing");
                SyncRequests = new ConcurrentDictionary<string, DateTime>();
                return false;
            }

            if (SyncRequests.Count < 1)
            {
                Log("*** SyncRequestExists empty sync requests list, returning false");
                return false;
            }

            if (SyncRequests.ContainsKey(guid))
            {
                Log("SyncRequestExists found sync request for GUID " + guid);
                return true;
            }

            Log("*** SyncRequestExists unable to find sync request for GUID " + guid);
            return false;
        }

        private bool AddSyncResponse(BigQMessage response)
        {
            if (response == null)
            {
                Log("*** AddSyncResponse null BigQMessage supplied");
                return false;
            }

            if (String.IsNullOrEmpty(response.MessageId))
            {
                Log("*** AddSyncResponse null MessageId within supplied message");
                return false;
            }
            
            if (SyncResponses.ContainsKey(response.MessageId))
            {
                Log("*** AddSyncResponse response already awaits for MessageId " + response.MessageId);
                return false;
            }

            SyncResponses.TryAdd(response.MessageId, response);
            Log("AddSyncResponse added sync response for MessageId " + response.MessageId);
            return true;
        }

        private bool GetSyncResponse(string guid, out BigQMessage response)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            response = new BigQMessage();
            DateTime start = DateTime.Now;
            bool timeoutExceeded = false;
            bool messageReceived = false;

            try
            {
                if (String.IsNullOrEmpty(guid))
                {
                    Log("*** GetSyncResponse null GUID supplied");
                    return false;
                }

                if (SyncResponses == null)
                {
                    Log("*** GetSyncResponse null sync responses list, initializing");
                    SyncResponses = new ConcurrentDictionary<string, BigQMessage>();
                    return false;
                }

                int iterations = 0;
                while (true)
                {
                    // if (LogMessageResponseTime) Log("GetSyncResponse iteration " + iterations + " " + sw.Elapsed.TotalMilliseconds + "ms");

                    if (SyncResponses.ContainsKey(guid))
                    {
                        if (!SyncResponses.TryGetValue(guid, out response))
                        {
                            Log("*** GetSyncResponse unable to retrieve sync response for GUID " + guid + " though one exists");
                            return false;
                        }

                        messageReceived = true;
                        Log("GetSyncResponse returning response for message GUID " + guid);
                        return true;
                    }

                    //
                    // Check if timeout exceeded
                    //
                    TimeSpan ts = DateTime.Now - start;
                    if (ts.TotalMilliseconds > SyncTimeoutMsec)
                    {
                        Log("*** GetSyncResponse timeout waiting for response for message GUID " + guid);
                        timeoutExceeded = true;
                        response = null;
                        return false;
                    }

                    iterations++;
                    continue;
                }
            }
            finally
            {
                if (messageReceived) Task.Run(() => RemoveSyncRequest(guid));

                sw.Stop();
                if (LogMessageResponseTime)
                {
                    Log("GetSyncResponse " + sw.Elapsed.TotalMilliseconds + "ms (timeout " + timeoutExceeded + ")");
                }
            }
        }
        
        private void CleanupSyncRequests()
        {
            while (true)
            {
                Thread.Sleep(SyncTimeoutMsec);
                List<string> ExpiredMessageIDs = new List<string>();
                DateTime TempDateTime;
                BigQMessage TempMessage;

                foreach (KeyValuePair<string, DateTime> CurrentRequest in SyncRequests)
                {
                    DateTime ExpirationDateTime = CurrentRequest.Value.AddMilliseconds(SyncTimeoutMsec);

                    if (DateTime.Compare(ExpirationDateTime, DateTime.Now) < 0)
                    {
                        #region Expiration-Earlier-Than-Current-Time

                        Log("*** CleanupSyncRequests adding MessageId " + CurrentRequest.Key + " (added " + CurrentRequest.Value.ToString("MM/dd/yyyy hh:mm:ss") + ") to cleanup list (past expiration time " + ExpirationDateTime.ToString("MM/dd/yyyy hh:mm:ss") + ")");
                        ExpiredMessageIDs.Add(CurrentRequest.Key);

                        #endregion
                    }
                }

                foreach (string CurrentRequestGuid in ExpiredMessageIDs)
                {
                    // 
                    // remove from sync requests
                    //
                    SyncRequests.TryRemove(CurrentRequestGuid, out TempDateTime);
                }
               
                foreach (string CurrentRequestGuid in ExpiredMessageIDs)
                {
                    if (SyncResponses.ContainsKey(CurrentRequestGuid))
                    {
                        //
                        // remove from sync responses
                        //
                        SyncResponses.TryRemove(CurrentRequestGuid, out TempMessage);
                    }
                }
              
            }
        }

        #endregion

        #region Private-Methods

        private bool TCPDataSender(BigQMessage Message)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                #region Check-for-Null-Values

                if (Message == null)
                {
                    Log("*** TCPDataSender null message supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                if (!BigQHelper.IsTCPPeerConnected(ClientTCPInterface))
                {
                    Log("Server " + ServerIp + ":" + ServerPort + " not connected");
                    Connected = false;

                    // 
                    //
                    // Do not fire event here; allow TCPDataReceiver to do it
                    //
                    //
                    // if (ServerDisconnected != null) ServerDisconnected();
                    return false;
                }

                #endregion

                #region Send-Message

                if (!BigQHelper.TCPMessageWrite(ClientTCPInterface, Message, LogMessageResponseTime))
                {
                    Log("TCPDataSender unable to send data to server " + ServerIp + ":" + ServerPort);
                    return false;
                }
                else
                {
                    Log("TCPDataSender successfully sent message to server " + ServerIp + ":" + ServerPort);
                }

                #endregion

                return true;
            }
            finally
            {
                sw.Stop();
                if (LogMessageResponseTime) Log("TCPDataSender " + sw.Elapsed.TotalMilliseconds + "ms");
            }
        }
        
        private void TCPDataReceiver()
        {
            bool disconnectDetected = false;

            try
            {
                #region Attach-to-Stream

                if (!ClientTCPInterface.Connected)
                {
                    Log("*** TCPDataReceiver server " + ServerIp + ":" + ServerPort + " is no longer connected");
                    return;
                }

                NetworkStream ClientStream = ClientTCPInterface.GetStream();

                #endregion

                #region Wait-for-Data

                while (true)
                {
                    #region Check-if-Client-Connected-to-Server

                    if (!ClientTCPInterface.Connected || !BigQHelper.IsTCPPeerConnected(ClientTCPInterface))
                    {
                        Log("*** TCPDataReceiver server " + ServerIp + ":" + ServerPort + " disconnected");
                        Connected = false;

                        disconnectDetected = true;
                        break;
                    }
                    else
                    {
                        // Log("TCPDataReceiver server " + ServerIp + ":" + ServerPort + " is still connected");
                    }

                    #endregion

                    #region Read-Message
                    
                    BigQMessage CurrentMessage = BigQHelper.TCPMessageRead(ClientTCPInterface, LogMessageResponseTime);
                    if (CurrentMessage == null)
                    {
                        // Log("TCPDataReceiver unable to read message from server " + ServerIp + ":" + ServerPort);
                        Thread.Sleep(30);
                        continue;
                    }
                    else
                    {
                        /*
                        Console.WriteLine("");
                        Console.WriteLine(CurrentMessage.ToString());
                        Console.WriteLine("");
                        */
                    }

                    #endregion

                    #region Handle-Message

                    if (String.Compare(CurrentMessage.SenderGuid, "00000000-0000-0000-0000-000000000000") == 0
                        && !String.IsNullOrEmpty(CurrentMessage.Command)
                        && String.Compare(CurrentMessage.Command.ToLower().Trim(), "heartbeatrequest") == 0)
                    {
                        #region Handle-Incoming-Server-Heartbeat

                        // 
                        //
                        // do nothing, just continue
                        //
                        //

                        #endregion
                    }
                    else if (BigQHelper.IsTrue(CurrentMessage.SyncRequest))
                    {
                        #region Handle-Incoming-Sync-Request

                        Log("TCPDataReceiver sync request detected for message GUID " + CurrentMessage.MessageId);

                        if (SyncMessageReceived != null)
                        {
                            byte[] ResponseData = SyncMessageReceived(CurrentMessage);

                            CurrentMessage.SyncRequest = false;
                            CurrentMessage.SyncResponse = true;
                            CurrentMessage.Data = ResponseData;

                            string TempGuid = String.Copy(CurrentMessage.SenderGuid);
                            CurrentMessage.SenderGuid = ClientGuid;
                            CurrentMessage.RecipientGuid = TempGuid;

                            TCPDataSender(CurrentMessage);
                            Log("TCPDataReceiver sent response message for message GUID " + CurrentMessage.MessageId);
                        }
                        else
                        {
                            Log("*** TCPDataReceiver sync request received for MessageId " + CurrentMessage.MessageId + " but no handler specified, sending async");
                            if (AsyncMessageReceived != null)
                            {
                                Task.Run(() => AsyncMessageReceived(CurrentMessage));
                            }
                            else
                            {
                                Log("*** TCPDataReceiver no method defined for AsyncMessageReceived");
                            }
                        }

                        #endregion
                    }
                    else if (BigQHelper.IsTrue(CurrentMessage.SyncResponse))
                    {
                        #region Handle-Incoming-Sync-Response

                        Log("TCPDataReceiver sync response detected for message GUID " + CurrentMessage.MessageId);

                        if (SyncRequestExists(CurrentMessage.MessageId))
                        {
                            Log("TCPDataReceiver sync request exists for message GUID " + CurrentMessage.MessageId);

                            if (AddSyncResponse(CurrentMessage))
                            {
                                Log("TCPDataReceiver added sync response for message GUID " + CurrentMessage.MessageId);
                            }
                            else
                            {
                                Log("*** TCPDataReceiver unable to add sync response for MessageId " + CurrentMessage.MessageId + ", sending async");
                                if (AsyncMessageReceived != null)
                                {
                                    Task.Run(() => AsyncMessageReceived(CurrentMessage));
                                }
                                else
                                {
                                    Log("*** TCPDataReceiver no method defined for AsyncMessageReceived");
                                }

                            }
                        }
                        else
                        {
                            Log("*** TCPDataReceiver message marked as sync response but no sync request found for MessageId " + CurrentMessage.MessageId + ", sending async");
                            if (AsyncMessageReceived != null)
                            {
                                Task.Run(() => AsyncMessageReceived(CurrentMessage));
                            }
                            else
                            {
                                Log("*** TCPDataReceiver no method defined for AsyncMessageReceived");
                            }
                        }

                        #endregion
                    }
                    else
                    {
                        #region Handle-Async

                        Log("TCPDataReceiver async message GUID " + CurrentMessage.MessageId);

                        if (AsyncMessageReceived != null)
                        {
                            Task.Run(() => AsyncMessageReceived(CurrentMessage));
                        }
                        else
                        {
                            Log("*** TCPDataReceiver no method defined for AsyncMessageReceived");
                        }

                        #endregion
                    }

                    #endregion
                }

                #endregion
            }
            catch (ObjectDisposedException)
            {
                Log("*** TCPDataReceiver no longer connected (object disposed exception)");
            }
            catch (Exception EOuter)
            {
                Log("*** TCPDataReceiver outer exception detected");
                LogException("TCPDataReceiver", EOuter);
            }
            finally
            {
                if (disconnectDetected || !Connected)
                {
                    if (ServerDisconnected != null)
                    {
                        Task.Run(() => ServerDisconnected());
                    }
                }
            }
        }

        private void HeartbeatManager()
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

                if (ClientTCPInterface == null)
                {
                    Log("*** TCPHeartbeatManager null client supplied");
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

                    if (!BigQHelper.IsTCPPeerConnected(ClientTCPInterface))
                    {
                        Log("TCPHeartbeatManager client disconnected from server " + ServerIp + ":" + ServerPort);
                        Connected = false;
                        return;
                    }

                    #endregion

                    #region Send-Heartbeat-Message

                    lastHeartbeatAttempt = DateTime.Now;
                    BigQMessage HeartbeatMessage = HeartbeatRequestMessage();
                    
                    if (!SendServerMessageAsync(HeartbeatMessage))
                    { 
                        numConsecutiveFailures++;
                        lastFailure = DateTime.Now;

                        Log("*** TCPHeartbeatManager failed to send heartbeat to server " + ServerIp + ":" + ServerPort + " (" + numConsecutiveFailures + "/" + MaxHeartbeatFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= MaxHeartbeatFailures)
                        {
                            Log("*** TCPHeartbeatManager maximum number of failed heartbeats reached");
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

                #endregion
            }
            catch (Exception EOuter)
            {
                LogException("TCPHeartbeatManager", EOuter);
            }
            finally
            {
            }
        }

        private BigQMessage HeartbeatRequestMessage()
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.MessageId = Guid.NewGuid().ToString();
            ResponseMessage.RecipientGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Command = "HeartbeatRequest";
            ResponseMessage.SenderGuid = ClientGuid;
            ResponseMessage.CreatedUTC = DateTime.Now.ToUniversalTime();
            ResponseMessage.Data = null;
            return ResponseMessage;
        }

        #endregion

        #region Private-Utility-Methods

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
