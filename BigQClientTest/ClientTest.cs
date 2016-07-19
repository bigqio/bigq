using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BigQ;

namespace BigQClientTest
{
    class ClientTest
    {
        static Client client;

        static void Main(string[] args)
        {
            bool runForever = true;
            string guid = "";
            string msg = "";
            int priv = 0;
            List<Client> clients = new List<Client>();
            List<Channel> channels = new List<Channel>();
            Message response = new Message();
            Dictionary<string, DateTime> pendingRequests;

            Console.WriteLine("");
            Console.WriteLine(@" $$\       $$\                      ");
            Console.WriteLine(@" $$ |      \__|                     ");
            Console.WriteLine(@" $$$$$$$\  $$\  $$$$$$\   $$$$$$\   ");
            Console.WriteLine(@" $$  __$$\ $$ |$$  __$$\ $$  __$$\  ");
            Console.WriteLine(@" $$ |  $$ |$$ |$$ /  $$ |$$ /  $$ | ");
            Console.WriteLine(@" $$ |  $$ |$$ |$$ |  $$ |$$ |  $$ | ");
            Console.WriteLine(@" $$$$$$$  |$$ |\$$$$$$$ |\$$$$$$$ | ");
            Console.WriteLine(@" \_______/ \__| \____$$ | \____$$ | ");
            Console.WriteLine(@"               $$\   $$ |      $$ | ");
            Console.WriteLine(@"               \$$$$$$  |      $$ | ");
            Console.WriteLine(@"                \______/       \__| ");
            Console.WriteLine("");
            Console.WriteLine("BigQ Client Version " + System.Reflection.Assembly.GetEntryAssembly().GetName().Version.ToString());
            Console.WriteLine("Starting BigQ client at " + DateTime.Now.ToUniversalTime().ToString("MM/dd/yyyy HH:mm:ss"));
            Console.WriteLine("");

            ConnectToServer();
            
            while (runForever)
            {
                if (client == null) Console.Write("[OFFLINE] ");
                Console.Write("Command [? for help]: ");

                string cmd = Console.ReadLine();
                if (String.IsNullOrEmpty(cmd)) continue;
                
                switch (cmd.ToLower())
                {
                    case "?":
                        // Console.WriteLine("34567890123456789012345678901234567890123456789012345678901234567890123456789");
                        Console.WriteLine("Available Commands:");
                        Console.WriteLine("  q  cls  echo  debugon  debugoff");
                        Console.WriteLine("  login  listchannels  listchannelmembers  listchannelsubscribers");
                        Console.WriteLine("  createbroadcastchannel  createmulticastchannel  deletechannel"); 
                        Console.WriteLine("  joinchannel  leavechannel");
                        Console.WriteLine("  subscribechannel unsubscribechannel");
                        Console.WriteLine("  sendprivasync  sendprivsync");
                        Console.WriteLine("  sendchannel  listclients  isclientconnected  whoami  pendingsyncrequests");
                        Console.WriteLine("");
                        break;

                    case "q":
                    case "quit":
                        runForever = false;
                        break;

                    case "c":
                    case "cls":
                        Console.Clear();
                        break;

                    case "echo":
                        if (client == null) break;
                        client.Echo();
                        break;

                    case "debugon":
                        client.Config.Debug.Enable = true;
                        client.Config.Debug.ConsoleLogging = true;
                        client.Config.Debug.MsgResponseTime = true;
                        break;

                    case "debugoff":
                        client.Config.Debug.Enable = false;
                        client.Config.Debug.ConsoleLogging = false;
                        client.Config.Debug.MsgResponseTime = false;
                        break;

                    case "login":
                        if (client == null) break;
                        if (client.Login(out response))
                        {
                            Console.WriteLine("Login success");
                        }
                        else
                        {
                            Console.WriteLine("Login failed");
                        }
                        break;

                    case "listclients":
                        if (client == null) break;
                        if (client.ListClients(out response, out clients))
                        {
                            Console.WriteLine("ListClients success");
                            if (clients == null || clients.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (Client curr in clients)
                                {
                                    string line = "  " + curr.IpPort() + " " + curr.Email + " " + curr.ClientGUID + " ";
                                    if (curr.IsTCP) line += "TCP ";
                                    else if (curr.IsTCPSSL) line += "TCPSSL ";
                                    else if (curr.IsWebsocket) line += "WS ";
                                    else if (curr.IsWebsocketSSL) line += "WSSSL ";
                                    else line += "unknown ";

                                    Console.WriteLine(line);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("ListClients failed");
                        }
                        break;

                    case "isclientconnected":
                        if (client == null) break;
                        Console.Write("Client GUID: ");
                        guid = Console.ReadLine();
                        if (client.IsClientConnected(guid, out response))
                        {
                            Console.WriteLine("Client " + guid + " is connected");
                        }
                        else
                        {
                            Console.WriteLine("Client  " + guid + " is not connected");
                        }
                        break;
                         
                    case "listchannels":
                        if (client == null) break;
                        if (client.ListChannels(out response, out channels))
                        {
                            Console.WriteLine("ListChannels success");
                            if (channels == null || channels.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (Channel curr in channels)
                                {
                                    string line = "  " + curr.Guid + ": " + curr.ChannelName + " owner " + curr.OwnerGuid + " ";
                                    if (curr.Private == 1) line += "priv ";
                                    else line += "pub ";
                                    if (curr.Broadcast == 1) line += "bcast ";
                                    else if (curr.Multicast == 1) line += "mcast ";
                                    else line += "unknown ";

                                    Console.WriteLine(line);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("ListChannels failed");
                        }
                        break;

                    case "listchannelmembers":
                        if (client == null) break;
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        if (client.ListChannelMembers(guid, out response, out clients))
                        {
                            Console.WriteLine("ListChannelMembers success");
                            if (clients == null || clients.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (Client curr in clients)
                                {
                                    string line = "  " + curr.IpPort() + " " + curr.Email + " " + curr.ClientGUID + " ";
                                    if (curr.IsTCP) line += "TCP ";
                                    else if (curr.IsTCPSSL) line += "TCPSSL ";
                                    else if (curr.IsWebsocket) line += "WS ";
                                    else if (curr.IsWebsocketSSL) line += "WSSSL ";
                                    else line += "unknown ";

                                    Console.WriteLine(line);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("ListChannelMembers failed");
                        }
                        break;

                    case "listchannelsubscribers":
                        if (client == null) break;
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        if (client.ListChannelSubscribers(guid, out response, out clients))
                        {
                            Console.WriteLine("ListChannelSubscribers success");
                            if (clients == null || clients.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (Client curr in clients)
                                {
                                    string line = "  " + curr.IpPort() + " " + curr.Email + " " + curr.ClientGUID + " ";
                                    if (curr.IsTCP) line += "TCP ";
                                    else if (curr.IsTCPSSL) line += "TCPSSL ";
                                    else if (curr.IsWebsocket) line += "WS ";
                                    else if (curr.IsWebsocketSSL) line += "WSSSL ";
                                    else line += "unknown ";

                                    Console.WriteLine(line);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("ListChannelSubscribers failed");
                        }
                        break;

                    case "joinchannel":
                        if (client == null) break;
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        if (client.JoinChannel(guid, out response))
                        {
                            Console.WriteLine("JoinChannel success");
                        }
                        else
                        {
                            Console.WriteLine("JoinChannel failed");
                        }
                        break;

                    case "leavechannel":
                        if (client == null) break;
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        if (client.LeaveChannel(guid, out response))
                        {
                            Console.WriteLine("LeaveChannel success");
                        }
                        else
                        {
                            Console.WriteLine("LeaveChannel failed");
                        }
                        break;

                    case "subscribechannel":
                        if (client == null) break;
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        if (client.SubscribeChannel(guid, out response))
                        {
                            Console.WriteLine("SubscribeChannel success");
                        }
                        else
                        {
                            Console.WriteLine("SubscribeChannel failed");
                        }
                        break;

                    case "unsubscribechannel":
                        if (client == null) break;
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        if (client.UnsubscribeChannel(guid, out response))
                        {
                            Console.WriteLine("UnsubscribeChannel success");
                        }
                        else
                        {
                            Console.WriteLine("UnsubscribeChannel failed");
                        }
                        break;

                    case "createbroadcastchannel":
                        if (client == null) break;
                        Console.Write("Name          : ");
                        guid = Console.ReadLine();
                        Console.Write("Private (0/1) : ");
                        priv = Convert.ToInt32(Console.ReadLine());
                        if (client.CreateBroadcastChannel(guid, priv, out response))
                        {
                            Console.WriteLine("CreateBroadcastChannel success");
                        }
                        else
                        {
                            Console.WriteLine("CreateBroadcastChannel failed");
                        }
                        break;

                    case "createmulticastchannel":
                        if (client == null) break;
                        Console.Write("Name          : ");
                        guid = Console.ReadLine();
                        Console.Write("Private (0/1) : ");
                        priv = Convert.ToInt32(Console.ReadLine());
                        if (client.CreateMulticastChannel(guid, priv, out response))
                        {
                            Console.WriteLine("CreateMulticastChannel success");
                        }
                        else
                        {
                            Console.WriteLine("CreateMulticastChannel failed");
                        }
                        break;

                    case "deletechannel":
                        if (client == null) break;
                        Console.Write("GUID: ");
                        guid = Console.ReadLine();
                        if (client.DeleteChannel(guid, out response))
                        {
                            Console.WriteLine("DeleteChannel success");
                        }
                        else
                        {
                            Console.WriteLine("DeleteChannel failed");
                        }
                        break;

                    case "sendprivasync":
                        if (client == null) break;
                        Console.Write("Recipient GUID: ");
                        guid = Console.ReadLine();
                        Console.Write("Message: ");
                        msg = Console.ReadLine();
                        client.SendPrivateMessageAsync(guid, msg);
                        break;

                    case "sendprivsync":
                        if (client == null) break;
                        Console.Write("Recipient GUID: ");
                        guid = Console.ReadLine();
                        Console.Write("Message: ");
                        msg = Console.ReadLine();

                        Message respMsg = new Message();
                        client.SendPrivateMessageSync(guid, msg, out respMsg);

                        if (respMsg != null)
                        {
                            Console.WriteLine("Sync response received for GUID " + respMsg.MessageID);
                            Console.WriteLine(respMsg.ToString());
                        }
                        else
                        {
                            Console.WriteLine("*** No sync response received (null)");
                        }
                        break;

                    case "sendchannel":
                        if (client == null) break;
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        Console.Write("Message: ");
                        msg = Console.ReadLine();
                        client.SendChannelMessage(guid, msg);
                        break;

                    case "whoami":
                        if (client == null) break;
                        Console.Write(client.IpPort());
                        if (!String.IsNullOrEmpty(client.ClientGUID)) Console.WriteLine("  GUID " + client.ClientGUID);
                        else Console.WriteLine("[not logged in]");
                        break;

                    case "pendingsyncrequests":
                        if (client == null) break;
                        if (client.PendingSyncRequests(out pendingRequests))
                        {
                            Console.WriteLine("PendingSyncRequests success");
                            Console.Write("Outstanding requests: ");
                            if (pendingRequests == null) Console.WriteLine("(null)");
                            else if (pendingRequests.Count < 1) Console.WriteLine("(empty)");
                            else
                            {
                                Console.WriteLine(pendingRequests.Count + " requests");
                                foreach (KeyValuePair<string, DateTime> curr in pendingRequests)
                                {
                                    Console.WriteLine("  " + curr.Key + ": " + curr.Value.ToString("MM/dd/yyyy hh:mm:ss"));
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("PendingSyncRequests failed");
                        }
                        break;
                        
                    default:
                        Console.WriteLine("Unknown command");
                        break;
                }
            }
        }

        #region Delegates

        static bool AsyncMessageReceived(Message msg)
        {
            Console.WriteLine(msg.ToString());
            return true;
        }

        static byte[] SyncMessageReceived(Message msg)
        {
            Console.WriteLine("Received sync message: " + msg.ToString());
            Console.WriteLine("Press ENTER and then type your response");
            Console.WriteLine("(The menu command parser is expecting input so press ENTER first!)");
            Console.Write("Response [ENTER for 'hello!']: ");
            string resp = Console.ReadLine();
            if (!String.IsNullOrEmpty(resp)) return Encoding.UTF8.GetBytes(resp);
            return null;
        }

        static bool ClientJoinedServer(string clientGuid)
        {
            Console.WriteLine("Client " + clientGuid + " joined the server");
            return true;
        }

        static bool ClientLeftServer(string clientGuid)
        {
            Console.WriteLine("Client " + clientGuid + " left the server");
            return true;
        }

        static bool ClientJoinedChannel(string clientGuid, string channelGuid)
        {
            Console.WriteLine("Client " + clientGuid + " joined channel " + channelGuid);
            return true;
        }

        static bool ClientLeftChannel(string clientGuid, string channelGuid)
        {
            Console.WriteLine("Client " + clientGuid + " left channel " + channelGuid);
            return true;
        }

        static bool SubscriberJoinedChannel(string subscriberGuid, string channelGuid)
        {
            Console.WriteLine("Subscriber " + subscriberGuid + " joined channel " + channelGuid);
            return true;
        }

        static bool SubscriberLeftChannel(string subscriberGuid, string channelGuid)
        {
            Console.WriteLine("Subscriber " + subscriberGuid + " left channel " + channelGuid);
            return true;
        }

        static bool ConnectToServer()
        {
            try
            {
                Console.WriteLine("Attempting to connect to server");
                if (client != null) client.Close();
                client = null;
                client = new Client(null);

                client.AsyncMessageReceived = AsyncMessageReceived;
                client.SyncMessageReceived = SyncMessageReceived;
                client.ServerDisconnected = ConnectToServer;
                client.ClientJoinedServer = ClientJoinedServer;
                client.ClientLeftServer = ClientLeftServer;
                client.ClientJoinedChannel = ClientJoinedChannel;
                client.ClientLeftChannel = ClientLeftChannel;
                client.SubscriberJoinedChannel = SubscriberJoinedChannel;
                client.SubscriberLeftChannel = SubscriberLeftChannel;
                // client.LogMessage = LogMessage;
                client.LogMessage = null;

                Message response;
                if (!client.Login(out response))
                {
                    Console.WriteLine("Unable to login, retrying in five seconds");
                    Thread.Sleep(5000);
                    return ConnectToServer();
                }

                Console.WriteLine("Successfully connected to server");
                return true;
            }
            catch (SocketException)
            {
                Console.WriteLine("*** Unable to connect to server (port not reachable)");
                Console.WriteLine("*** Retrying in five seconds");
                Thread.Sleep(5000);
                return ConnectToServer();
            }
            catch (TimeoutException)
            {
                Console.WriteLine("*** Timeout connecting to server");
                Console.WriteLine("*** Retrying in five seconds");
                Thread.Sleep(5000);
                return ConnectToServer();
            }
            catch (Exception e)
            {
                Console.WriteLine("*** Unable to connect to server due to the following exception:");
                PrintException("ConnectToServer", e);
                Console.WriteLine("*** Retrying in five seconds");
                Thread.Sleep(5000);
                return ConnectToServer();
            }
        }

        static bool LogMessage(string msg)
        {
            Console.WriteLine("BigQClient message: " + msg);
            return true;
        }

        static void PrintException(string method, Exception e)
        {
            Console.WriteLine("================================================================================");
            Console.WriteLine(" = Method: " + method);
            Console.WriteLine(" = Exception Type: " + e.GetType().ToString());
            Console.WriteLine(" = Exception Data: " + e.Data);
            Console.WriteLine(" = Inner Exception: " + e.InnerException);
            Console.WriteLine(" = Exception Message: " + e.Message);
            Console.WriteLine(" = Exception Source: " + e.Source);
            Console.WriteLine(" = Exception StackTrace: " + e.StackTrace);
            Console.WriteLine("================================================================================");
        }

        #endregion
    }
}
