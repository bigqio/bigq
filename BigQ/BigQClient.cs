using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ
{
    [Serializable]
    public class BigQClient
    {
        #region Class-Members

        public int UserId;
        public string Email;
        public string Password;
        public string ClientGuid;
        public string SourceIp;
        public int SourcePort;
        public string ServerIp;
        public int ServerPort;
        public DateTime? CreatedUTC;
        public DateTime? UpdatedUTC;
        public TcpClient Client;
        
        public bool Connected;
        public bool LoggedIn;
        public bool ConsoleDebug;

        private int HeartbeatIntervalMsec;
        private int MaxHeartbeatFailures;
        private CancellationTokenSource CdrTokenSource = null;
        private CancellationToken CdrToken;
        private CancellationTokenSource CsrTokenSource = null;
        private CancellationToken CsrToken;
        private CancellationTokenSource HbTokenSource = null;
        private CancellationToken HbToken;
        private ConcurrentDictionary<string, DateTime> SyncRequests;
        private ConcurrentDictionary<string, BigQMessage> SyncResponses;
        private int SyncTimeoutMsec;

        #endregion

        #region Delegates

        public Func<BigQMessage, bool> AsyncMessageReceived;
        public Func<BigQMessage, byte[]> SyncMessageReceived;
        public Func<bool> ServerDisconnected;
        public Func<string, bool> LogMessage;

        #endregion

        #region Constructors

        // used by the server
        public BigQClient()
        {
        }

        // used by the client
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
            Client = new TcpClient();
            IAsyncResult ar = Client.BeginConnect(ip, port, null, null);
            WaitHandle wh = ar.AsyncWaitHandle;

            try
            {
                if (!ar.AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(5), false))
                {
                    Client.Close();
                    throw new TimeoutException("Timeout connecting to " + ip + ":" + port);
                }

                Client.EndConnect(ar);

                SourceIp = ((IPEndPoint)Client.Client.LocalEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.LocalEndPoint).Port;
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

            if (CdrTokenSource != null) CdrTokenSource.Cancel();
            if (CsrTokenSource != null) CsrTokenSource.Cancel();
            if (HbTokenSource != null) HbTokenSource.Cancel();

            #endregion

            #region Start-Tasks

            CdrTokenSource = new CancellationTokenSource();
            CdrToken = CdrTokenSource.Token;
            Task.Factory.StartNew(() => ConnectionDataReceiver(), CdrToken);

            CsrTokenSource = new CancellationTokenSource();
            CsrToken = CsrTokenSource.Token;
            Task.Factory.StartNew(() => CleanupSyncRequests(), CsrToken);

            //
            //
            //
            // do not start heartbeat until successful login
            // design goal: server needs to free up connections fast
            // client just needs to keep socket open
            //
            //
            //
            // HbTokenSource = new CancellationTokenSource();
            // HbToken = HbTokenSource.Token;
            // Task.Factory.StartNew(() => HeartbeatManager());

            #endregion
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
            response = new BigQMessage();

            if (String.IsNullOrEmpty(guid))
            {
                Log("*** GetSyncResponse null guid supplied");
                return false;
            }

            DateTime start = DateTime.Now;
            
            if (SyncResponses == null)
            {
                Log("*** GetSyncResponse null sync responses list, initializing");
                SyncResponses = new ConcurrentDictionary<string, BigQMessage>();
                return false;
            }

            BigQMessage TempMessage;

            while (true)
            {
                if (SyncResponses.ContainsKey(guid))
                {
                    response = SyncResponses[guid];
                    SyncResponses.TryRemove(guid, out TempMessage);

                    if (response.Data != null)
                    {
                        Log("GetSyncResponse returning response for message GUID " + guid + ": " + response.Data.ToString());
                    }
                    else
                    {
                        Log("GetSyncResponse returning response for message GUID " + guid + ": (nul)");
                    }
                    return true;
                }

                TimeSpan ts = DateTime.Now - start;
                if (ts.TotalMilliseconds > SyncTimeoutMsec)
                {
                    Log("*** GetSyncResponse timeout waiting for response for message GUID " + guid);
                    response = null;
                    return false;
                }

                continue;
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
                    SyncRequests.TryRemove(CurrentRequestGuid, out TempDateTime);
                }
               
                foreach (string CurrentRequestGuid in ExpiredMessageIDs)
                {
                    if (SyncResponses.ContainsKey(CurrentRequestGuid))
                    {
                        SyncResponses.TryRemove(CurrentRequestGuid, out TempMessage);
                    }
                }
              
            }
        }

        #endregion

        #region Private-Methods

        private bool ConnectionDataSender(BigQMessage Message)
        {
            #region Check-for-Null-Values

            if (Message == null)
            {
                Log("*** ConnectionDataSender null message supplied");
                return false;
            }

            #endregion

            #region Check-if-Client-Connected

            if (!BigQHelper.IsPeerConnected(Client))
            {
                Log("Server " + ServerIp + ":" + ServerPort + " not connected");
                Connected = false;

                if (ServerDisconnected != null) ServerDisconnected();
                return false;
            }

            #endregion
            
            #region Send-Message

            if (!BigQHelper.MessageWrite(Client, Message))
            {
                Log("Unable to send data to server " + ServerIp + ":" + ServerPort);
                return false;
            }
            else
            {
                Log("Successfully sent message to server " + ServerIp + ":" + ServerPort);
            }

            #endregion
            
            return true;
        }
        
        private void ConnectionDataReceiver()
        {
            try
            {
                #region Attach-to-Stream

                if (!Client.Connected)
                {
                    Log("*** ConnectionDataReceiver server " + ServerIp + ":" + ServerPort + " is no longer connected");
                    return;
                }

                NetworkStream ClientStream = Client.GetStream();

                #endregion

                #region Wait-for-Data

                while (true)
                {
                    #region Check-if-Client-Connected-to-Server

                    if (!Client.Connected || !BigQHelper.IsPeerConnected(Client))
                    {
                        Log("*** ConnectionDataReceiver server " + ServerIp + ":" + ServerPort + " disconnected");
                        Connected = false;

                        if (ServerDisconnected != null) Task.Factory.StartNew(() => ServerDisconnected());
                        break;
                    }
                    else
                    {
                        // Log("ConnectionDataReceiver server " + ServerIp + ":" + ServerPort + " is still connected");
                    }

                    #endregion

                    #region Read-and-Process-Message

                    if (ClientStream.DataAvailable)
                    {
                        #region Read-Message

                        BigQMessage CurrentMessage = null;
                        if (!BigQHelper.MessageRead(Client, out CurrentMessage))
                        {
                            Log("ConnectionDataReceiver unable to read message from server " + ServerIp + ":" + ServerPort);
                            continue;
                        }

                        if (CurrentMessage == null)
                        {
                            Log("*** ConnectionDataReceiver null message read from server " + ServerIp + ":" + ServerPort);
                            continue;
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

                            Log("ConnectionDataReceiver sync request detected for message GUID " + CurrentMessage.MessageId);

                            if (SyncMessageReceived != null)
                            {
                                byte[] ResponseData = SyncMessageReceived(CurrentMessage);

                                CurrentMessage.SyncRequest = false;
                                CurrentMessage.SyncResponse = true;
                                CurrentMessage.Data = ResponseData;

                                string TempGuid = String.Copy(CurrentMessage.SenderGuid);
                                CurrentMessage.SenderGuid = ClientGuid;
                                CurrentMessage.RecipientGuid = TempGuid;

                                ConnectionDataSender(CurrentMessage);
                                Log("ConnectionDataReceiver sent response message for message GUID " + CurrentMessage.MessageId);
                            }
                            else
                            {
                                Log("*** ConnectionDataReceiver sync request received for MessageId " + CurrentMessage.MessageId + " but no handler specified, sending async");
                                if (AsyncMessageReceived != null)
                                {
                                    Task.Factory.StartNew(() => AsyncMessageReceived(CurrentMessage));
                                }
                                else
                                {
                                    Log("*** ConnectionDataReceiver no method defined for AsyncMessageReceived");
                                }
                            }

                            #endregion
                        }
                        else if (BigQHelper.IsTrue(CurrentMessage.SyncResponse))
                        {
                            #region Handle-Incoming-Sync-Response

                            Log("ConnectionDataReceiver sync response detected for message GUID " + CurrentMessage.MessageId);

                            if (SyncRequestExists(CurrentMessage.MessageId))
                            {
                                Log("ConnectionDataReceiver sync request exists for message GUID " + CurrentMessage.MessageId);

                                if (AddSyncResponse(CurrentMessage))
                                {
                                    Log("ConnectionDataReceiver added sync response for message GUID " + CurrentMessage.MessageId);
                                }
                                else
                                {
                                    Log("*** ConnectionDataReceiver unable to add sync response for MessageId " + CurrentMessage.MessageId + ", sending async");
                                    if (AsyncMessageReceived != null)
                                    {
                                        Task.Factory.StartNew(() => AsyncMessageReceived(CurrentMessage));
                                    }
                                    else
                                    {
                                        Log("*** ConnectionDataReceiver no method defined for AsyncMessageReceived");
                                    }

                                }
                            }
                            else
                            {
                                Log("*** ConnectionDataReceiver message marked as sync response but no sync request found for MessageId " + CurrentMessage.MessageId + ", sending async");
                                if (AsyncMessageReceived != null)
                                {
                                    Task.Factory.StartNew(() => AsyncMessageReceived(CurrentMessage));
                                }
                                else
                                {
                                    Log("*** ConnectionDataReceiver no method defined for AsyncMessageReceived");
                                }
                            }

                            #endregion
                        }
                        else
                        {
                            #region Handle-Async

                            Log("ConnectionDataReceiver async message GUID " + CurrentMessage.MessageId);

                            if (AsyncMessageReceived != null)
                            {
                                Task.Factory.StartNew(() => AsyncMessageReceived(CurrentMessage));
                            }
                            else
                            {
                                Log("*** ConnectionDataReceiver no method defined for AsyncMessageReceived");
                            }

                            #endregion
                        }

                        #endregion
                    }

                    #endregion
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                Log("*** ConnectionDataReceiver outer exception detected");
                LogException("ConnectionDataReceiver", EOuter);
                if (ServerDisconnected != null) ServerDisconnected();
            }
        }

        private void HeartbeatManager()
        {
            try
            {
                #region Check-for-Disable

                if (HeartbeatIntervalMsec == 0)
                {
                    Log("*** HeartbeatManager disabled");
                    return;
                }

                #endregion

                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** HeartbeatManager null client supplied");
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

                    if (!BigQHelper.IsPeerConnected(Client))
                    {
                        Log("HeartbeatManager client disconnected from server " + ServerIp + ":" + ServerPort);
                        Connected = false;

                        if (ServerDisconnected != null) ServerDisconnected();
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

                        Log("*** HeartbeatManager failed to send heartbeat to server " + ServerIp + ":" + ServerPort + " (" + numConsecutiveFailures + "/" + MaxHeartbeatFailures + " consecutive failures)");

                        if (numConsecutiveFailures >= MaxHeartbeatFailures)
                        {
                            Log("*** HeartbeatManager maximum number of failed heartbeats reached");
                            Connected = false;

                            if (ServerDisconnected != null) Task.Factory.StartNew(() => ServerDisconnected());
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
                LogException("HeartbeatManager", EOuter);
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

        #region Public-Methods

        public bool SendRawMessage(BigQMessage message)
        {
            if (message == null) throw new ArgumentNullException("message");
            return ConnectionDataSender(message);
        }
        
        public bool Echo()
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
            return ConnectionDataSender(request);
        }

        public bool Login(out BigQMessage response)
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
                if (HbTokenSource != null) HbTokenSource.Cancel();
                
                // start new heartbeat thread
                HbTokenSource = new CancellationTokenSource();
                HbToken = HbTokenSource.Token;
                Task.Factory.StartNew(() => HeartbeatManager());

                return true;
            }
        }

        public bool ListClients(out BigQMessage response, out List<BigQClient> clients)
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
                    clients = BigQHelper.DeserializeJson<List<BigQClient>>(response.Data, false);
                }
                return true;
            }
        }

        public bool ListChannels(out BigQMessage response, out List<BigQChannel> channels)
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
                    channels = BigQHelper.DeserializeJson<List<BigQChannel>>(response.Data, false);
                }
                return true;
            }
        }

        public bool ListChannelSubscribers(string guid, out BigQMessage response, out List<BigQClient> clients)
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
                Console.WriteLine(response.Data);
                if (response.Data != null)
                {
                    clients = BigQHelper.DeserializeJson<List<BigQClient>>(response.Data, false);
                }
                return true;
            }
        }

        public bool JoinChannel(string guid, out BigQMessage response)
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

        public bool LeaveChannel(string guid, out BigQMessage response)
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

        public bool CreateChannel(string name, int priv, out BigQMessage response)
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

        public bool DeleteChannel(string guid, out BigQMessage response)
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
        
        public bool SendPrivateMessageAsync(string guid, string data)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException("data");
            return SendPrivateMessageAsync(guid, Encoding.UTF8.GetBytes(data));
        }

        public bool SendPrivateMessageAsync(string guid, byte[] data)
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
            return ConnectionDataSender(CurrentMessage);
        }
        
        public bool SendPrivateMessageSync(string guid, string data, out BigQMessage response)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException("data");
            return SendPrivateMessageSync(guid, Encoding.UTF8.GetBytes(data), out response);
        }

        public bool SendPrivateMessageSync(string guid, byte[] data, out BigQMessage response)
        {
            response = null;

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
            CurrentMessage.SyncRequest = true;
            CurrentMessage.Data = data;

            if (!AddSyncRequest(CurrentMessage.MessageId))
            {
                Log("*** SendPrivateMessageSync unable to register sync request GUID " + CurrentMessage.MessageId);
                return false;
            }

            if (!ConnectionDataSender(CurrentMessage))
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

            if (!RemoveSyncRequest(CurrentMessage.MessageId))
            {
                Log("*** SendPrivateMessage unable to remove sync request for message GUID " + CurrentMessage.MessageId);
                return false;
            }

            if (ResponseMessage != null) response = ResponseMessage;
            return true;
        }

        public bool SendServerMessageAsync(BigQMessage request)
        {
            if (request == null) throw new ArgumentNullException("request");
            request.RecipientGuid = "00000000-0000-0000-0000-000000000000";
            return ConnectionDataSender(request);
        }

        public bool SendServerMessageSync(BigQMessage request, out BigQMessage response)
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

            if (!ConnectionDataSender(request))
            {
                Log("*** SendServerMessageSync unable to send message GUID " + request.MessageId + " to server");
                return false;
            }

            BigQMessage ResponseMessage = new BigQMessage();
            if (!GetSyncResponse(request.MessageId, out ResponseMessage))
            {
                Log("*** SendServerMessageSync unable to get response for message GUID " + request.MessageId);
                return false;
            }

            if (!RemoveSyncRequest(request.MessageId))
            {
                Log("*** SendServerMessageSync unable to remove sync request for message GUID " + request.MessageId);
                return false;
            }

            if (ResponseMessage != null) response = ResponseMessage;
            return true;
        }

        public bool SendChannelMessage(string guid, string data)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException("guid");
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException("data");
            return SendChannelMessage(guid, Encoding.UTF8.GetBytes(data));
        }

        public bool SendChannelMessage(string guid, byte[] data)
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
            return ConnectionDataSender(CurrentMessage);
        }

        public bool PendingSyncRequests(out Dictionary<string, DateTime> response)
        {
            response = null;
            if (SyncRequests == null) return true;
            if (SyncRequests.Count < 1) return true;

            response = SyncRequests.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            return true;
        }

        public bool IsClientConnected(string guid, out BigQMessage response)
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
        
        public string IpPort()
        {
            return SourceIp + ":" + SourcePort;
        }

        public void Close()
        {
            if (CdrTokenSource != null) CdrTokenSource.Cancel();
            if (CsrTokenSource != null) CsrTokenSource.Cancel();
            if (HbTokenSource != null) HbTokenSource.Cancel();

            if (Client != null)
            {
                if (Client.Connected)
                {
                    //
                    // close the TCP stream
                    //
                    if (Client.GetStream() != null)
                    {
                        Client.GetStream().Close();
                    }
                }

                // 
                // close the client
                //

                if (Client != null) Client.Close();
            }

            Client = null;
            return;
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
