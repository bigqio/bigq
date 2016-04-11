using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ
{
    public class BigQServer
    {
        #region Class-Members

        public List<BigQChannel> Channels;
        public List<BigQClient> Clients;
        public List<TcpClient> TcpClients;
        public DateTime Created;

        private string ListenerIp;
        private IPAddress ListenerIpAddress;
        private int ListenerPort;
        private TcpListener Listener;
        private bool ListenerRunning;
        private bool SendAcknowledgements;
        private bool SendServerJoinNotifications;
        private bool SendChannelJoinNotifications;
        public bool ConsoleDebug;

        private readonly object ChannelsLock;
        private readonly object ClientsLock;

        #endregion

        #region Delegates

        public Func<BigQMessage, bool> MessageReceived;
        public Func<bool> ServerStopped;
        public Func<BigQClient, bool> ClientConnected;
        public Func<BigQClient, bool> ClientLogin;
        public Func<BigQClient, bool> ClientDisconnected;
        public Func<string, bool> LogMessage;

        #endregion

        #region Constructor

        public BigQServer(string ip, int port, bool debug, bool sendAck, bool sendServerJoinNotifications, bool sendChannelJoinNotifications)
        {
            #region Check-for-Invalid-Values

            if (port < 1)
            {
                throw new ArgumentOutOfRangeException("Port must be greater than zero.");
            }

            #endregion

            #region Set-Class-Variables

            this.ListenerIp = ip;
            this.ListenerPort = port;
            Channels = new List<BigQChannel>();
            Clients = new List<BigQClient>();
            Created = DateTime.Now.ToUniversalTime();
            SendAcknowledgements = sendAck;
            SendServerJoinNotifications = sendServerJoinNotifications;
            SendChannelJoinNotifications = sendChannelJoinNotifications;
            ConsoleDebug = debug;

            ChannelsLock = new object();
            ClientsLock = new object();

            #endregion

            #region Set-Delegates-to-Null

            MessageReceived = null;
            ServerStopped = null;
            ClientConnected = null;
            ClientLogin = null;
            ClientDisconnected = null;
            LogMessage = null;

            #endregion

            #region Create-Database

            /*
            if (!BigQSqlite.CreateDatabase())
            {
                Log("*** Database creation failed");
                throw new Exception("Unable to create Sqlite database file bigq.db or its tables.");
            }
             */

            #endregion

            #region Start-Server

            if (String.IsNullOrEmpty(this.ListenerIp))
            {
                Listener = new TcpListener(System.Net.IPAddress.Any, this.ListenerPort);
            }
            else
            {
                this.ListenerIpAddress = IPAddress.Parse(this.ListenerIp);
                Listener = new TcpListener(this.ListenerIpAddress, this.ListenerPort);
            }

            Task.Factory.StartNew(() => AcceptConnections());

            #endregion
        }

        #endregion

        #region Public-Methods

        public List<BigQChannel> ListChannels()
        {
            return GetAllChannels();
        }

        public List<BigQClient> ListChannelSubscribers(string guid)
        {
            return GetChannelSubscribers(guid);
        }

        public List<BigQClient> ListClients()
        {
            return GetAllClients();
        }
        
        #endregion

        #region Private-Connection-Methods

        private void AcceptConnections()
        {
            try
            { 
                #region Prepare

                this.Listener.Start();
                this.ListenerRunning = true;

                #endregion
                
                #region Accept-Connections

                while (this.ListenerRunning)
                {
                    #region Reset-Variables

                    string ClientIp = "";
                    int ClientPort = 0;

                    #endregion

                    #region Accept-Connection

                    TcpClient Client = Listener.AcceptTcpClient();
                    ClientIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                    ClientPort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;

                    #endregion

                    #region Add-to-Client-List

                    BigQClient CurrentClient = new BigQClient();
                    CurrentClient.SourceIp = ClientIp;
                    CurrentClient.SourcePort = ClientPort;
                    CurrentClient.Client = Client;
                    CurrentClient.Created = DateTime.Now.ToUniversalTime();
                    CurrentClient.Updated = DateTime.Now.ToUniversalTime();

                    if (!AddClient(CurrentClient))
                    {
                        Log("*** AcceptConnections unable to add client " + CurrentClient.IpPort());
                    }

                    #endregion

                    #region Pass-to-Connection-Data-Receiver

                    Log("AcceptConnections added " + CurrentClient.IpPort() + " to clients list");
                    Task.Factory.StartNew(() => ConnectionDataReceiver(CurrentClient));

                    #endregion
                }

                #endregion
            }
            catch (Exception e)
            {
                this.ListenerRunning = false;
                LogException("AcceptConnections", e);
                if (ServerStopped != null) ServerStopped();
            }
        }

        private void ConnectionDataReceiver(BigQClient CurrentClient)
        {
            try
            {
                #region Check-for-Null-Values

                if (CurrentClient == null)
                {
                    Log("*** ConnectionDataReceiver null client supplied");
                    return;
                }

                if (CurrentClient.Client == null)
                {
                    Log("*** ConnectionDataReceiver null TcpClient supplied within client");
                    return;
                }

                #endregion

                #region Wait-for-Data

                TcpClient Client = CurrentClient.Client;

                while (true)
                {
                    #region Check-if-Client-Connected

                    if (!BigQHelper.IsPeerConnected(Client))
                    {
                        Log("ConnectionDataReceiver client " + CurrentClient.IpPort() + " disconnected");
                        if (!RemoveClient(CurrentClient))
                        {
                            Log("*** ConnectionDataReceiver unable to remove client " + CurrentClient.IpPort());
                        }

                        if (!RemoveClientChannels(CurrentClient))
                        {
                            Log("*** ConnectionDataReceiver unable to remove channels associated with client " + CurrentClient.IpPort());
                        }

                        if (SendServerJoinNotifications) ServerLeaveEvent(CurrentClient);
                        break;
                    }
                    else
                    {
                        // Log("ConnectionDataReceiver client " + CurrentClient.IpPort() + " is still connected");
                    }

                    #endregion

                    #region Read-Data-from-Client

                    byte[] Data;
                    if (!BigQHelper.SocketRead(Client, out Data))
                    {
                        // Log("ConnectionDataReceiver unable to read from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        if (Data != null && Data.Length > 0)
                        {
                            Log("ConnectionDataReceiver successfully read " + Data.Length + " bytes from client " + CurrentClient.IpPort() + " email " + CurrentClient.Email + " GUID " + CurrentClient.ClientGuid);
                        }
                        else
                        {
                            Log("ConnectionDataReceiver failed to read data from client " + CurrentClient.IpPort());
                            continue;
                        }
                    }

                    #endregion

                    #region Deserialize-Data-to-BigQMessage-and-Validate

                    BigQMessage CurrentMessage = null;

                    try
                    {
                        CurrentMessage = BigQHelper.DeserializeJson<BigQMessage>(Data, ConsoleDebug);
                    }
                    catch (Exception)
                    {
                        Log("*** ConnectionDataReceiver unable to deserialize message from client " + CurrentClient.IpPort());
                        Log(Encoding.UTF8.GetString(Data));
                        // LogException("ConnectionDataReceiver", e);
                        CurrentMessage = null;
                    }

                    if (CurrentMessage == null)
                    {
                        Log("ConnectionDataReceiver unable to convert data to message after read from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        Log("ConnectionDataReceiver successfully converted data to message from client " + CurrentClient.IpPort());
                        Task.Factory.StartNew(() => MessageReceived(CurrentMessage));
                    }

                    if (!CurrentMessage.IsValid())
                    {
                        Log("ConnectionDataReceiver invalid message received from client " + CurrentClient.IpPort());
                        continue;
                    }
                    else
                    {
                        Log("ConnectionDataReceiver valid message received from client " + CurrentClient.IpPort());
                    }

                    #endregion

                    #region Process-Message

                    MessageProcessor(CurrentClient, CurrentMessage);
                    Log("ConnectionDataReceiver finished processing message from client " + CurrentClient.IpPort());

                    #endregion
                }

                #endregion
            }
            catch (Exception EOuter)
            {
                if (CurrentClient != null)
                {
                    LogException("ConnectionDataReceiver (" + CurrentClient.IpPort() + ")", EOuter);
                }
                else
                {
                    LogException("ConnectionDataReceiver (null)", EOuter);
                }
            }
        }

        private bool ConnectionDataSender(BigQClient CurrentClient, BigQMessage Message)
        {
            #region Check-for-Null-Values

            if (CurrentClient == null)
            {
                Log("*** ConnectionDataSender null client supplied");
                return false;
            }

            if (CurrentClient.Client == null)
            {
                Log("*** ConnectionDataSender null TcpClient supplied within client object for client " + CurrentClient.ClientGuid);
                return false;
            }

            if (Message == null)
            {
                Log("*** ConnectionDataSender null message supplied");
                return false;
            }

            #endregion

            #region Check-if-Client-Connected

            if (!BigQHelper.IsPeerConnected(CurrentClient.Client))
            {
                Log("ConnectionDataSender client " + CurrentClient.IpPort() + " not connected");
                return false;
            }

            #endregion

            #region Serialize-Message

            string json = BigQHelper.SerializeJson<BigQMessage>(Message);
            byte[] Data = Encoding.UTF8.GetBytes(json);

            #endregion

            #region Send-Message

            if (!BigQHelper.SocketWrite(CurrentClient.Client, Data))
            {
                Log("ConnectionDataSender unable to send data to client " + CurrentClient.IpPort());
                return false;
            }
            else
            {
                if (!String.IsNullOrEmpty(Message.Command))
                {
                    Log("ConnectionDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command " + Message.Command);
                }
                else
                {
                    Log("ConnectionDataSender successfully sent data to client " + CurrentClient.IpPort() + " for command (null)");
                }
            }

            #endregion

            return true;
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
                Task.Factory.StartNew(() =>
                {
                    Message.RecipientGuid = curr.ClientGuid;
                    bool ResponseSuccess = ConnectionDataSender(curr, Message);
                    if (!ResponseSuccess)
                    {
                        Log("*** ChannelDataSender error sending channel message from " + Message.SenderGuid + " to " + Message.RecipientGuid + " in channel " + Message.ChannelGuid);
                    }
                });
            }

            return true;
        }

        #endregion

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
                    Task.Factory.StartNew(() =>
                    {
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = ConnectionDataSender(curr, Message);
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
                if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) != 0)
                {
                    Task.Factory.StartNew(() =>
                    {
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = ConnectionDataSender(curr, Message);
                        if (!ResponseSuccess)
                        {
                            Log("*** ServerLeaveEvent error sending server leave event to " + Message.RecipientGuid + " (leave by " + CurrentClient.ClientGuid + ")");
                        }
                    });
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
                    Task.Factory.StartNew(() =>
                    {
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = ConnectionDataSender(curr, Message);
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
                    Task.Factory.StartNew(() =>
                    {
                        Message.RecipientGuid = curr.ClientGuid;
                        bool ResponseSuccess = ConnectionDataSender(curr, Message);
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

            BigQClient ret = null;
            lock (ClientsLock)
            {
                foreach (BigQClient curr in Clients)
                {
                    if (String.Compare(curr.ClientGuid, guid) == 0)
                    {
                        ret = curr;
                        break;
                    }
                }
            }

            if (ret == null)
            {
                Log("*** GetClientByGuid unable to find client by GUID " + guid);
                return null;
            }
            else
            {
                Log("GetClientByGuid returning client with GUID " + guid);
                return ret;
            }
        }

        private List<BigQClient> GetAllClients()
        {
            if (Clients == null || Clients.Count < 1)
            {
                Log("*** GetAllClients no clients");
                return null;
            }

            List<BigQClient> ret = new List<BigQClient>();
            lock (ClientsLock)
            {
                foreach (BigQClient curr in Clients)
                {
                    ret.Add(curr);
                }
            }

            Log("GetAllClients returning " + ret.Count + " clients");
            return ret;
        }

        private BigQChannel GetChannelByGuid(string guid)
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
            lock (ChannelsLock)
            {
                foreach (BigQChannel curr in Channels)
                {
                    if (String.Compare(curr.Guid, guid) == 0)
                    {
                        ret = new BigQChannel();
                        ret.Guid = curr.Guid;
                        ret.ChannelName = curr.ChannelName;
                        ret.OwnerGuid = curr.OwnerGuid;
                        ret.Created = curr.Created;
                        ret.Updated = curr.Updated;
                        ret.Private = curr.Private;
                        ret.Subscribers = curr.Subscribers;
                        break;
                    }
                }
            }

            if (ret == null)
            {
                Log("*** GetChannelByGuid unable to find channel with GUID " + guid);
                return null;
            }
            else
            {
                Log("GetChannelByGuid returning channel " + guid);
                return ret;
            }
        }

        private List<BigQChannel> GetAllChannels()
        {
            if (Channels == null || Channels.Count < 1)
            {
                Log("*** GetAllChannels no Channels");
                return null;
            }

            List<BigQChannel> ret = new List<BigQChannel>();
            lock (ChannelsLock)
            {
                foreach (BigQChannel curr in Channels)
                {
                    ret.Add(curr);
                }
            }

            Log("GetAllChannels returning " + ret.Count + " channels");
            return ret;
        }

        private List<BigQClient> GetChannelSubscribers(string guid)
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
            lock (ChannelsLock)
            {
                foreach (BigQChannel curr in Channels)
                {
                    if (String.Compare(curr.Guid, guid) == 0)
                    {
                        foreach (BigQClient CurrentClient in curr.Subscribers)
                        {
                            ret.Add(CurrentClient);
                        }
                    }
                }
            }

            Log("GetChannelSubscribers returning " + ret.Count + " subscribers");
            return ret;
        }

        private BigQChannel GetChannelByName(string name)
        {
            if (String.IsNullOrEmpty(name))
            {
                Log("*** GetChannelByName null name supplied");
                return null;
            }

            if (Channels == null || Channels.Count < 1) return null;
            BigQChannel ret = null;
            lock (ChannelsLock)
            {
                foreach (BigQChannel curr in Channels)
                {
                    if (String.Compare(curr.ChannelName.ToLower(), name.ToLower()) == 0)
                    {
                        ret = curr;
                        break;
                    }
                }
            }

            return ret;
        }

        private bool AddClient(BigQClient CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** AddClient null client supplied");
                return false;
            }

            lock (ClientsLock)
            {
                List<BigQClient> NewClientsList = new List<BigQClient>();
                
                foreach (BigQClient curr in Clients)
                {
                    if (String.Compare(curr.SourceIp, CurrentClient.SourceIp) == 0)
                    {
                        if (curr.SourcePort == CurrentClient.SourcePort)
                        {
                            // 
                            // do not add, this is a duplicate
                            //
                            continue;
                        }
                    }

                    if (!String.IsNullOrEmpty(CurrentClient.ClientGuid))
                    {
                        if (!String.IsNullOrEmpty(curr.ClientGuid))
                        {
                            if (String.Compare(CurrentClient.ClientGuid, curr.ClientGuid) == 0)
                            {
                                //
                                // do not add, this is a duplicate
                                //
                                continue;
                            }
                        }
                    }

                    NewClientsList.Add(curr);
                }
                
                Log("AddClient adding client " + CurrentClient.IpPort());
                CurrentClient.Created = DateTime.Now.ToUniversalTime();
                CurrentClient.Updated = CurrentClient.Created;
                NewClientsList.Add(CurrentClient);
                Clients = NewClientsList;

                if (ClientConnected != null) ClientConnected(CurrentClient);
            }

            return true;
        }

        private bool RemoveClient(BigQClient CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** RemoveClient null client supplied");
                return false;
            }

            Log("RemoveClient removing client " + CurrentClient.IpPort() + " " + CurrentClient.ClientGuid);

            lock (ClientsLock)
            {
                if (Clients == null || Clients.Count < 1)
                {
                    Log("*** RemoveClient no clients");
                    return false;
                }

                List<BigQClient> UpdatedList = new List<BigQClient>();

                foreach (BigQClient curr in Clients)
                {
                    bool found = false;

                    if (!String.IsNullOrEmpty(CurrentClient.ClientGuid))
                    {
                        if (!String.IsNullOrEmpty(curr.ClientGuid))
                        {
                            if (String.Compare(CurrentClient.ClientGuid, curr.ClientGuid) == 0)
                            {
                                Log("RemoveClient matched client GUID in client list, removing: " + CurrentClient.ClientGuid);
                                found = true;
                            }
                        }
                    }

                    if (String.Compare(curr.SourceIp, CurrentClient.SourceIp) == 0)
                    {
                        if (curr.SourcePort == CurrentClient.SourcePort)
                        {
                            Log("RemoveClient matched client IP:port in client list, removing: " + CurrentClient.IpPort());
                            found = true;
                        }
                    }

                    if (!found)
                    {
                        UpdatedList.Add(curr);
                    }
                    else
                    {
                        // do not add, we want to remove this element
                    }
                }

                Clients = UpdatedList;
            }

            if (ClientDisconnected != null) ClientDisconnected(CurrentClient);
            return true;
        }

        private bool RemoveClientChannels(BigQClient CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** RemoveClientChannels null client supplied");
                return false;
            }

            lock (ChannelsLock)
            {
                if (Channels == null || Channels.Count < 1)
                {
                    Log("RemoveClientChannels no channels");
                    return false;
                }

                List<BigQChannel> UpdatedChannelsList = new List<BigQChannel>();

                foreach (BigQChannel curr in Channels)
                {
                    if (String.Compare(curr.OwnerGuid, CurrentClient.ClientGuid) != 0)
                    {
                        UpdatedChannelsList.Add(curr);
                    }
                    else
                    {
                        Log("RemoveClientChannels removing channel " + curr.Guid + " (owned by client " + curr.OwnerGuid + ")");

                        if (curr.Subscribers != null)
                        {
                            if (curr.Subscribers.Count > 0)
                            {
                                foreach (BigQClient Client in curr.Subscribers)
                                {
                                    if (String.Compare(Client.ClientGuid, curr.OwnerGuid) != 0)
                                    {
                                        Log("RemoveClientChannels notifying channel " + curr.Guid + " subscriber " + Client.ClientGuid + " of channel deletion");
                                        Task.Factory.StartNew(() =>
                                        {
                                            SendSystemMessage(ChannelDeletedByOwnerMessage(Client, curr));
                                        });
                                    }
                                }
                            }
                        }

                        Log("RemoveClientChannels removing channel " + curr.Guid);
                    }
                }

                Channels = UpdatedChannelsList;
            }

            return true;
        }

        private bool UpdateClient(BigQClient CurrentClient)
        {
            if (CurrentClient == null)
            {
                Log("*** UpdateClient null client supplied");
                return false;
            }

            lock (ClientsLock)
            {
                List<BigQClient> UpdatedList = new List<BigQClient>();
                BigQClient UpdatedClient = new BigQClient();

                foreach (BigQClient curr in Clients)
                {
                    Log("UpdateClient comparing current client " + CurrentClient.IpPort() +
                        " with list item " + curr.IpPort());

                    if (String.Compare(curr.SourceIp, CurrentClient.SourceIp) == 0)
                    {
                        if (curr.SourcePort == CurrentClient.SourcePort)
                        {
                            Log("UpdateClient matched existing client " + curr.IpPort() + ", updating");
                            Log("UpdateClient existing client details: Email " + curr.Email + " GUID " + curr.ClientGuid);
                            Log("UpdateClient new details: Email " + CurrentClient.Email + " GUID " + curr.ClientGuid);

                            UpdatedClient = new BigQClient();
                            UpdatedClient.Email = CurrentClient.Email;
                            UpdatedClient.Password = CurrentClient.Password;
                            UpdatedClient.ClientGuid = CurrentClient.ClientGuid;
                            UpdatedClient.Created = CurrentClient.Created;
                            UpdatedClient.Updated = DateTime.Now.ToUniversalTime();
                            UpdatedClient.SourceIp = CurrentClient.SourceIp;
                            UpdatedClient.SourcePort = CurrentClient.SourcePort;
                            UpdatedClient.Client = CurrentClient.Client;

                            UpdatedList.Add(UpdatedClient);
                        }
                        else
                        {
                            // IP match but not port match
                            Log("UpdateClient IP match but no port match for current client " + CurrentClient.IpPort());
                            UpdatedList.Add(curr);
                        }
                    }
                    else
                    {
                        // No match on either IP or port
                        Log("UpdateClient no IP or port match for current client " + CurrentClient.IpPort());
                        UpdatedList.Add(curr);
                    }
                }

                Clients = UpdatedList;
            }

            return true;
        }

        private bool AddChannel(BigQClient CurrentClient, BigQChannel CurrentChannel)
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

            lock (ChannelsLock)
            {
                bool found = false;

                foreach (BigQChannel curr in Channels)
                {
                    if (String.Compare(curr.Guid, CurrentChannel.Guid) == 0)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    Log("AddChannel adding channel " + CurrentChannel.ChannelName + " GUID " + CurrentChannel.Guid);
                    if (String.IsNullOrEmpty(CurrentChannel.ChannelName)) CurrentChannel.ChannelName = CurrentChannel.Guid;
                    CurrentChannel.Created = DateTime.Now.ToUniversalTime();
                    CurrentChannel.Updated = CurrentClient.Created;
                    CurrentChannel.Subscribers = new List<BigQClient>();
                    CurrentChannel.Subscribers.Add(CurrentClient);
                    CurrentChannel.OwnerGuid = CurrentClient.ClientGuid;
                    Channels.Add(CurrentChannel);
                }
                else
                {
                    Log("*** Channel with GUID " + CurrentChannel.Guid + " already exists");
                }
            }

            return true;
        }

        private bool RemoveChannel(BigQChannel CurrentChannel)
        {
            if (CurrentChannel == null)
            {
                Log("*** RemoveChannel null channel supplied");
                return false;
            }

            lock (ChannelsLock)
            {
                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** RemoveChannel no channels");
                    return false;
                }

                List<BigQChannel> UpdatedChannelsList = new List<BigQChannel>();

                foreach (BigQChannel Channel in Channels)
                {
                    if (String.Compare(Channel.Guid, CurrentChannel.Guid) != 0)
                    {
                        UpdatedChannelsList.Add(Channel);
                    }
                    else
                    {
                        // do not add, we want to remove this element
                        Log("RemoveChannel notifying channel members of channel removal");

                        if (Channel.Subscribers != null)
                        {
                            if (Channel.Subscribers.Count > 0)
                            {
                                foreach (BigQClient Client in Channel.Subscribers)
                                {
                                    if (String.Compare(Client.ClientGuid, Channel.OwnerGuid) != 0)
                                    {
                                        Log("RemoveChannel notifying channel " + Channel.Guid + " subscriber " + Client.ClientGuid + " of channel deletion by owner");
                                        SendSystemMessage(ChannelDeletedByOwnerMessage(Client, Channel));
                                    }
                                }
                            }
                        }

                        Log("RemoveChannel removing channel " + Channel.Guid);
                    }
                }

                Channels = UpdatedChannelsList;
            }

            return true;
        }

        private bool AddChannelSubscriber(BigQClient CurrentClient, BigQChannel CurrentChannel)
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

            lock (ChannelsLock)
            {
                #region Check-for-Null-or-Empty-Channels

                if (Channels == null || Channels.Count < 1)
                {
                    Log("*** AddChannelSubscriber no channels");
                    return false;
                }

                #endregion

                #region Variables

                BigQChannel UpdatedChannel = new BigQChannel();
                List<BigQChannel> UpdatedChannelsList = new List<BigQChannel>();

                #endregion

                #region Iterate

                foreach (BigQChannel Channel in Channels)
                {
                    Log("AddChannelSubscriber comparing existing channel GUID " + Channel.Guid + " with match " + CurrentChannel.Guid);

                    if (String.Compare(Channel.Guid, CurrentChannel.Guid) == 0)
                    {
                        #region Rebuild-Matched-Channel

                        Log("AddChannelSubscriber found channel " + CurrentChannel.Guid);

                        // cannot use CopyObject, the object is locked
                        UpdatedChannel = new BigQChannel();
                        UpdatedChannel.Guid = Channel.Guid;
                        UpdatedChannel.ChannelName = Channel.ChannelName;
                        UpdatedChannel.OwnerGuid = Channel.OwnerGuid;
                        UpdatedChannel.Created = Channel.Created;
                        UpdatedChannel.Updated = DateTime.Now.ToUniversalTime();
                        UpdatedChannel.Private = Channel.Private;
                        UpdatedChannel.Subscribers = new List<BigQClient>();

                        if (Channel.Subscribers == null || Channel.Subscribers.Count < 1)
                        {
                            #region First-Subscriber

                            Log("AddChannelSubscriber first member " + CurrentClient.ClientGuid + " in channel " + CurrentChannel.Guid);
                            UpdatedChannel.Subscribers.Add(CurrentClient);

                            #endregion
                        }
                        else
                        {
                            #region Subsequent-Subscriber

                            bool found = false;

                            foreach (BigQClient Client in Channel.Subscribers)
                            {
                                Log("AddChannelSubscriber comparing client GUID " + CurrentClient.ClientGuid + " with existing member " + Client.ClientGuid);

                                if (String.Compare(Client.ClientGuid, CurrentClient.ClientGuid) == 0)
                                {
                                    Log("AddChannelSubscriber client " + CurrentClient.IpPort() + " already a member in channel " + CurrentChannel.Guid);
                                    found = true;
                                }

                                UpdatedChannel.Subscribers.Add(Client);
                            }

                            if (!found)
                            {
                                // in case the client wasn't already a member
                                Log("AddChannelSubscriber adding member " + CurrentClient.ClientGuid + " in channel " + CurrentChannel.Guid);
                                UpdatedChannel.Subscribers.Add(CurrentClient);
                            }

                            #endregion
                        }

                        Log("AddChannelSubscriber updated channel " + Channel.Guid + " to " + UpdatedChannel.Subscribers.Count + " subscribers");
                        UpdatedChannelsList.Add(UpdatedChannel);

                        #endregion
                    }
                    else
                    {
                        #region Add-Nonmatching-Channel

                        Log("AddChannelSubscriber adding channel " + Channel.Guid + " (no change)");
                        UpdatedChannelsList.Add(Channel);

                        #endregion
                    }
                }

                #endregion
                
                Channels = UpdatedChannelsList;
            }

            #endregion

            return true;
        }

        private bool RemoveChannelSubscriber(BigQClient CurrentClient, BigQChannel CurrentChannel)
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

            lock (ChannelsLock)
            {
                List<BigQChannel> UpdatedChannelsList = new List<BigQChannel>();

                foreach (BigQChannel Channel in Channels)
                {
                    BigQChannel UpdatedChannel = new BigQChannel();

                    if (String.Compare(Channel.Guid, CurrentChannel.Guid) == 0)
                    {
                        List<BigQClient> UpdatedSubscribersList = new List<BigQClient>();

                        foreach (BigQClient Client in Channel.Subscribers)
                        {
                            if (String.Compare(Client.ClientGuid, CurrentClient.ClientGuid) != 0)
                            {
                                UpdatedSubscribersList.Add(Client);
                            }
                        }

                        // cannot use CopyObject, the object is locked
                        UpdatedChannel = new BigQChannel();
                        UpdatedChannel.Guid = Channel.Guid;
                        UpdatedChannel.ChannelName = Channel.ChannelName;
                        UpdatedChannel.OwnerGuid = Channel.OwnerGuid;
                        UpdatedChannel.Created = Channel.Created;
                        UpdatedChannel.Updated = DateTime.Now.ToUniversalTime();
                        UpdatedChannel.Private = Channel.Private;
                        UpdatedChannel.Subscribers = UpdatedSubscribersList;
                        
                        UpdatedChannelsList.Add(UpdatedChannel);
                    }
                    else
                    {
                        UpdatedChannelsList.Add(Channel);
                    }
                }

                Channels = UpdatedChannelsList;
            }

            return true;
        }

        private bool IsChannelSubscriber(BigQClient CurrentClient, BigQChannel CurrentChannel)
        {
            lock (ChannelsLock)
            {
                foreach (BigQClient curr in CurrentChannel.Subscribers)
                {
                    if (String.Compare(curr.SourceIp, CurrentClient.SourceIp) == 0)
                    {
                        if (curr.SourcePort == CurrentClient.SourcePort)
                        {
                            if (String.Compare(curr.ClientGuid, CurrentClient.ClientGuid) == 0)
                            {
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }

        private bool IsClientConnected(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                Log("*** IsClientConnected null GUID supplied");
                return false;
            }

            lock (ClientsLock)
            {
                foreach (BigQClient curr in Clients)
                {
                    if (String.IsNullOrEmpty(curr.ClientGuid)) continue;

                    if (String.Compare(curr.ClientGuid.ToLower().Trim(), guid.ToLower().Trim()) == 0)
                    {
                        Log("IsClientConnected client " + guid + " is connected");
                        return true;
                    }
                }

                Log("IsClientConnected client " + guid + " is not connected");
                return false;
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
                ret = BigQHelper.JObjectToObject<BigQChannel>(CurrentMessage.Data);
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
            ret.Created = DateTime.Now.ToUniversalTime();
            ret.Updated = ret.Created;
            ret.OwnerGuid = CurrentClient.ClientGuid;
            ret.Subscribers = new List<BigQClient>();
            ret.Subscribers.Add(CurrentClient);
            return ret;
        }

        private bool MessageProcessor(BigQClient CurrentClient, BigQMessage CurrentMessage)
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
                        ResponseSuccess = ConnectionDataSender(CurrentClient, LoginRequiredMessage());
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
                        ResponseSuccess = ConnectionDataSender(CurrentClient, LoginRequiredMessage());
                        return ResponseSuccess;
                    }
                }

                #endregion
            }

            #endregion

            #region Verify-TcpClient-Present

            if (String.Compare(CurrentClient.ClientGuid, "00000000-0000-0000-0000-000000000000") != 0)
            {
                // all zeros is the server
                if (CurrentClient.Client == null)
                {
                    Log("*** MessageProcessor null TcpClient within supplied client");
                    return false;
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
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "login":
                        ResponseMessage = ProcessLoginMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "joinchannel":
                        ResponseMessage = ProcessJoinChannelMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "leavechannel":
                        ResponseMessage = ProcessLeaveChannelMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "createchannel":
                        ResponseMessage = ProcessCreateChannelMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "deletechannel":
                        ResponseMessage = ProcessDeleteChannelMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "listchannels":
                        ResponseMessage = ProcessListChannelsMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "listchannelsubscribers":
                        ResponseMessage = ProcessListChannelSubscribersMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "listclients":
                        ResponseMessage = ProcessListClientsMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    case "isclientconnected":
                        ResponseMessage = ProcessIsClientConnectedMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                        return ResponseSuccess;

                    default:
                        ResponseMessage = UnknownCommandMessage(CurrentClient, CurrentMessage);
                        ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
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
                ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
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
                ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                return false;

                #endregion
            }
                
            #endregion
        }

        private bool SendPrivateMessage(BigQClient Sender, BigQClient Recipient, BigQMessage CurrentMessage)
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
                if (Sender.Client == null)
                {
                    Log("*** SendPrivateMessage null TcpClient within supplied Sender");
                    return false;
                }
            }

            if (Recipient == null)
            {
                Log("*** SendPrivateMessage null Recipient supplied");
                return false;
            }

            if (Recipient.Client == null)
            {
                Log("*** SendPrivateMessage null TcpClient within supplied Recipient");
                return false;
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

            ResponseSuccess = ConnectionDataSender(Recipient, RedactMessage(CurrentMessage));

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
                        ResponseSuccess = ConnectionDataSender(Sender, ResponseMessage);
                    }
                    return true;
                }
                else
                {
                    ResponseMessage = MessageSendFailure(Sender, CurrentMessage);
                    ResponseSuccess = ConnectionDataSender(Sender, ResponseMessage);
                    return false;
                }

                #endregion
            }

            #endregion
        }

        private bool SendChannelMessage(BigQClient Sender, BigQChannel CurrentChannel, BigQMessage CurrentMessage)
        {
            #region Check-for-Null-Values

            if (Sender == null)
            {
                Log("*** SendChannelMessage null Sender supplied");
                return false;
            }

            if (String.Compare(Sender.ClientGuid, "00000000-0000-0000-0000-000000000000") != 0)
            {
                // all zeros is the server
                if (Sender.Client == null)
                {
                    Log("*** SendChannelMessage null TcpClient within supplied Sender");
                    return false;
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
                ResponseSuccess = ConnectionDataSender(Sender, ResponseMessage);
                return false;
            }

            #endregion

            #region Send-to-Channel-and-Return-Success

            Task.Factory.StartNew(() =>
            {
                ResponseSuccess = ChannelDataSender(Sender, CurrentChannel, RedactMessage(CurrentMessage));
            });

            if (SendAcknowledgements)
            {
                ResponseMessage = MessageSendSuccess(Sender, CurrentMessage);
                ResponseSuccess = ConnectionDataSender(Sender, ResponseMessage);
            }
            return true;

            #endregion
        }

        private bool SendSystemMessage(BigQMessage CurrentMessage)
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

            if (!String.IsNullOrEmpty(ListenerIp)) CurrentClient.SourceIp = ListenerIp;
            else CurrentClient.SourceIp = "127.0.0.1";

            CurrentClient.SourcePort = ListenerPort;
            CurrentClient.ServerIp = CurrentClient.SourceIp;
            CurrentClient.ServerPort = CurrentClient.SourcePort;
            CurrentClient.Created = DateTime.Now.ToUniversalTime();
            CurrentClient.Updated = CurrentClient.Created;
            
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

                ResponseSuccess = ConnectionDataSender(CurrentRecipient, RedactMessage(CurrentMessage));
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
                ResponseSuccess = ConnectionDataSender(CurrentClient, ResponseMessage);
                return false;

                #endregion
            }

            #endregion
        }

        private bool SendSystemPrivateMessage(BigQClient Recipient, BigQMessage CurrentMessage)
        {
            #region Check-for-Null-Values

            if (Recipient == null)
            {
                Log("*** SendSystemPrivateMessage null recipient supplied");
                return false;
            }

            if (Recipient.Client == null)
            {
                Log("*** SendSystemPrivateMessage null TcpClient found within supplied recipient");
                return false;
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

            if (!String.IsNullOrEmpty(ListenerIp)) CurrentClient.SourceIp = ListenerIp;
            else CurrentClient.SourceIp = "127.0.0.1";

            CurrentClient.SourcePort = ListenerPort;
            CurrentClient.ServerIp = CurrentClient.SourceIp;
            CurrentClient.ServerPort = CurrentClient.SourcePort;
            CurrentClient.Created = DateTime.Now.ToUniversalTime();
            CurrentClient.Updated = CurrentClient.Created;

            #endregion

            #region Variables

            BigQChannel CurrentChannel = new BigQChannel();
            bool ResponseSuccess = false;

            #endregion

            #region Process-Recipient-Messages

            ResponseSuccess = ConnectionDataSender(Recipient, RedactMessage(CurrentMessage));
            return ResponseSuccess;

            #endregion
        }

        private bool SendSystemChannelMessage(BigQChannel Channel, BigQMessage CurrentMessage)
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

            if (!String.IsNullOrEmpty(ListenerIp)) CurrentClient.SourceIp = ListenerIp;
            else CurrentClient.SourceIp = "127.0.0.1";

            CurrentClient.SourcePort = ListenerPort;
            CurrentClient.ServerIp = CurrentClient.SourceIp;
            CurrentClient.ServerPort = CurrentClient.SourcePort;
            CurrentClient.Created = DateTime.Now.ToUniversalTime();
            CurrentClient.Updated = CurrentClient.Created;

            #endregion

            #region Variables

            bool ResponseSuccess = false;

            #endregion

            #region Process-Recipient-Messages

            ResponseSuccess = ChannelDataSender(CurrentClient, Channel, CurrentMessage);
            return ResponseSuccess;

            #endregion
        }

        #endregion

        #region Private-Message-Handlers

        private BigQMessage ProcessEchoMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            return ResponseMessage;
        }

        private BigQMessage ProcessLoginMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            
            CurrentClient.ClientGuid = ResponseMessage.RecipientGuid;
            CurrentClient.Email = CurrentMessage.Email;
            if (String.IsNullOrEmpty(CurrentClient.Email)) CurrentClient.Email = CurrentClient.ClientGuid;

            if (!UpdateClient(CurrentClient))
            {
                ResponseMessage.Success = false;
                ResponseMessage.Data = "Unable to update client details";
            }
            else
            {
                ResponseMessage.Success = true;
                ResponseMessage.Data = "Login successful (authentication not enabled)";

                if (ClientLogin != null) ClientLogin(CurrentClient);
                if (SendServerJoinNotifications) ServerJoinEvent(CurrentClient);
            }

            return ResponseMessage;
        }

        private BigQMessage ProcessIsClientConnectedMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            
            if (CurrentMessage.Data == null)
            {
                ResponseMessage.Success = false;
                ResponseMessage.Data = null;
            }
            else
            {
                ResponseMessage.Success = true;
                ResponseMessage.Data = IsClientConnected(CurrentMessage.Data.ToString());
            }

            return ResponseMessage;
        }

        private BigQMessage ProcessJoinChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
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

        private BigQMessage ProcessLeaveChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
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

        private BigQMessage ProcessCreateChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
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

        private BigQMessage ProcessDeleteChannelMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
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

        private BigQMessage ProcessListChannelsMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            List<BigQChannel> ret = new List<BigQChannel>();
            List<BigQChannel> filtered = new List<BigQChannel>();
            BigQMessage ResponseMessage = new BigQMessage();
            BigQChannel CurrentChannel = new BigQChannel();

            ret = GetAllChannels();
            if (ret == null || ret.Count < 1)
            {
                Log("*** ProcessListChannelsMessage no channels retrieved");

                ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
                ResponseMessage = RedactMessage(ResponseMessage);
                ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
                ResponseMessage.SyncRequest = null;
                ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
                ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                ResponseMessage.ChannelGuid = null;
                ResponseMessage.Created = DateTime.Now.ToUniversalTime();
                ResponseMessage.Success = true;
                ResponseMessage.Data = null;
                return ResponseMessage;
            }
            else
            {
                foreach (BigQChannel curr in ret)
                {
                    CurrentChannel.Subscribers = null;
                    CurrentChannel.Guid = curr.Guid;
                    CurrentChannel.ChannelName = curr.ChannelName;
                    CurrentChannel.OwnerGuid = curr.OwnerGuid;
                    CurrentChannel.Created = curr.Created;
                    CurrentChannel.Updated = curr.Updated;
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

            ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = null;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.Data = filtered;
            return ResponseMessage;
        }

        private BigQMessage ProcessListChannelSubscribersMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
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
                    temp.Client = null;

                    temp.Email = curr.Email;
                    temp.ClientGuid = curr.ClientGuid;
                    temp.Created = curr.Created;
                    temp.Updated = curr.Updated;
                    ret.Add(temp);
                }

                ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
                ResponseMessage = RedactMessage(ResponseMessage);
                ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
                ResponseMessage.SyncRequest = null;
                ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
                ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
                ResponseMessage.ChannelGuid = CurrentChannel.Guid;
                ResponseMessage.Created = DateTime.Now.ToUniversalTime();
                ResponseMessage.Success = true;
                ResponseMessage.Data = ret;
                return ResponseMessage;
            }
        }

        private BigQMessage ProcessListClientsMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
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
                    temp.Client = null;

                    temp.Email = curr.Email;
                    temp.ClientGuid = curr.ClientGuid;
                    temp.Created = curr.Created;
                    temp.Updated = curr.Updated;
                    ret.Add(temp);
                }
            }

            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = null;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.Data = ret;
            return ResponseMessage;
        }

        #endregion

        #region Private-Message-Builders

        private BigQMessage LoginRequiredMessage()
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;
            ResponseMessage.Data = "Login required";
            return ResponseMessage;
        }

        private BigQMessage UnknownCommandMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Unknown command '" + ResponseMessage.Command + "'";
            return ResponseMessage;
        }

        private BigQMessage RecipientNotFoundMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;

            if (!String.IsNullOrEmpty(ResponseMessage.RecipientGuid))
            {
                ResponseMessage.Data = "Unknown recipient '" + ResponseMessage.RecipientGuid + "'";
            }
            else if (!String.IsNullOrEmpty(ResponseMessage.ChannelGuid))
            {
                ResponseMessage.Data = "Unknown channel '" + ResponseMessage.ChannelGuid + "'";
            }
            else
            {
                ResponseMessage.Data = "No recipient or channel GUID supplied";
            }
            return ResponseMessage;
        }

        private BigQMessage NotChannelMemberMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "You are not a member of this channel";
            return ResponseMessage;
        }
        
        private BigQMessage MessageSendSuccess(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;

            if (!String.IsNullOrEmpty(CurrentMessage.RecipientGuid))
            {
                #region Individual-Recipient

                ResponseMessage.Data = "Message delivered to recipient";
                return ResponseMessage;

                #endregion
            }
            else if (!String.IsNullOrEmpty(CurrentMessage.ChannelGuid))
            {
                #region Channel-Recipient

                ResponseMessage.Data = "Message queued for delivery to channel members";
                return ResponseMessage;

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
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Unable to send message";
            return ResponseMessage;
        }

        private BigQMessage ChannelNotFoundMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Channel not found";
            return ResponseMessage;
        }

        private BigQMessage ChannelEmptyMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Channel is empty";
            return ResponseMessage;
        }

        private BigQMessage ChannelAlreadyExistsMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Channel already exists";
            return ResponseMessage;
        }

        private BigQMessage ChannelCreateSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Channel created successfully";
            return ResponseMessage;
        }

        private BigQMessage ChannelCreateFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Unable to create channel";
            return ResponseMessage;
        }

        private BigQMessage ChannelJoinSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Successfully joined channel";
            return ResponseMessage;
        }

        private BigQMessage ChannelLeaveSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Successfully left channel";
            return ResponseMessage;
        }

        private BigQMessage ChannelLeaveFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Unable to leave channel due to error";
            return ResponseMessage;
        }

        private BigQMessage ChannelJoinFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Failed to join channel";
            return ResponseMessage;
        }

        private BigQMessage ChannelDeletedByOwnerMessage(BigQClient CurrentClient, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = CurrentClient.ClientGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Channel deleted by owner";
            return ResponseMessage;
        }

        private BigQMessage ChannelDeleteSuccessMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Successfully deleted channel";
            return ResponseMessage;
        }

        private BigQMessage ChannelDeleteFailureMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, BigQChannel CurrentChannel)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Unable to delete channel";
            return ResponseMessage;
        }

        private BigQMessage DataErrorMessage(BigQClient CurrentClient, BigQMessage CurrentMessage, string message)
        {
            BigQMessage ResponseMessage = BigQHelper.CopyObject<BigQMessage>(CurrentMessage);
            ResponseMessage = RedactMessage(ResponseMessage);
            ResponseMessage.RecipientGuid = ResponseMessage.SenderGuid;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.Data = "Data error encountered in your message: " + message;
            return ResponseMessage;
        }

        private BigQMessage ServerJoinEventMessage(BigQClient NewClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientJoinedServer";
            ResponseEvent.Data = NewClient.ClientGuid;

            ResponseMessage.Data = ResponseEvent;
            return ResponseMessage;
        }

        private BigQMessage ServerLeaveEventMessage(BigQClient LeavingClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientLeftServer";
            ResponseEvent.Data = LeavingClient.ClientGuid;

            ResponseMessage.Data = ResponseEvent;
            return ResponseMessage;
        }

        private BigQMessage ChannelJoinEventMessage(BigQChannel CurrentChannel, BigQClient NewClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = null;
            ResponseMessage.SyncResponse = null;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientJoinedChannel";
            ResponseEvent.Data = NewClient.ClientGuid;

            ResponseMessage.Data = ResponseEvent;
            return ResponseMessage;
        }

        private BigQMessage ChannelLeaveEventMessage(BigQChannel CurrentChannel, BigQClient LeavingClient)
        {
            BigQMessage ResponseMessage = new BigQMessage();
            ResponseMessage.RecipientGuid = null;
            ResponseMessage.SenderGuid = "00000000-0000-0000-0000-000000000000";
            ResponseMessage.ChannelGuid = CurrentChannel.Guid;
            ResponseMessage.Created = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;

            BigQEvent ResponseEvent = new BigQEvent();
            ResponseEvent.EventType = "ClientLeftChannel";
            ResponseEvent.Data = LeavingClient.ClientGuid;

            ResponseMessage.Data = ResponseEvent;
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
