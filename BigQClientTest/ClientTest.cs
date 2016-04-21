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
        static BigQClient client;
        const bool DEBUG = true;

        static void Main(string[] args)
        {
            Console.Clear();
            Console.WriteLine("");
            Console.WriteLine("");

            Console.ForegroundColor = ConsoleColor.DarkGray;
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
            Console.ResetColor();

            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("BigQ Client");
            Console.WriteLine("");

            string guid = "";
            string msg = "";
            int priv = 0;
            List<BigQClient> clients;
            List<BigQChannel> channels;
            BigQMessage response;
            Dictionary<string, DateTime> pendingRequests;

            ConnectToServer();

            bool RunForever = true;
            while (RunForever)
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
                        Console.WriteLine("  q  cls  echo  login  listchannels  listchannelsubscribers  joinchannel");
                        Console.WriteLine("  leavechannel  createchannel  deletechannel  sendprivasync  sendprivsync");
                        Console.WriteLine("  sendchannel  listclients  isclientconnected  whoami  pendingsyncrequests");
                        Console.WriteLine("");
                        break;

                    case "q":
                    case "quit":
                        RunForever = false;
                        break;

                    case "c":
                    case "cls":
                        Console.Clear();
                        break;

                    case "echo":
                        if (client == null) break;
                        client.Echo();
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
                            else foreach (BigQClient curr in clients) Console.WriteLine("  " + curr.ClientGuid + " " + curr.Email);
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
                            Console.WriteLine("Client " + guid + " is not connected");
                        }
                        break;
                         
                    case "listchannels":
                        if (client == null) break;
                        if (client.ListChannels(out response, out channels))
                        {
                            Console.WriteLine("ListChannels success");
                            if (channels == null || channels.Count < 1) Console.WriteLine("(null)");
                            else foreach (BigQChannel curr in channels) Console.WriteLine("  " + curr.Guid + " " + curr.ChannelName + " " + curr.OwnerGuid);
                        }
                        else
                        {
                            Console.WriteLine("ListChannels failed");
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
                            else foreach (BigQClient curr in clients) Console.WriteLine("  " + curr.ClientGuid + " " + curr.Email);
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

                    case "createchannel":
                        if (client == null) break;
                        Console.Write("Name: ");
                        guid = Console.ReadLine();
                        Console.Write("Private (0/1): ");
                        priv = Convert.ToInt32(Console.ReadLine());
                        if (client.CreateChannel(guid, priv, out response))
                        {
                            Console.WriteLine("CreateChannel success");
                        }
                        else
                        {
                            Console.WriteLine("CreateChannel failed");
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

                        BigQMessage respMsg = new BigQMessage();
                        client.SendPrivateMessageSync(guid, msg, out respMsg);

                        if (respMsg != null)
                        {
                            Console.WriteLine("Sync response received for GUID " + respMsg.MessageId);
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
                        if (!String.IsNullOrEmpty(client.ClientGuid)) Console.WriteLine("  GUID " + client.ClientGuid);
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
                                foreach (KeyValuePair<string, DateTime> curr in pendingRequests) Console.WriteLine("  " + curr.Key + ": " + curr.Value.ToString("MM/dd/yyyy hh:mm:ss"));
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

        static bool AsyncMessageReceived(BigQMessage msg)
        {
            Console.WriteLine(msg.ToString());
            return true;
        }

        static byte[] SyncMessageReceived(BigQMessage msg)
        {
            Console.WriteLine("Received sync message: " + msg.ToString());
            Console.WriteLine("Press ENTER and then type your response");
            Console.WriteLine("(The menu command parser is expecting input so press ENTER first!)");
            Console.Write("Response [ENTER for 'hello!']: ");
            string resp = Console.ReadLine();
            if (!String.IsNullOrEmpty(resp)) return Encoding.UTF8.GetBytes(resp);
            return null;
        }
        
        static bool ConnectToServer()
        {
            try
            {
                Console.WriteLine("Attempting to connect to 127.0.0.1:8000");
                if (client != null) client.Close();
                client = null;
                client = new BigQClient(null, null, "127.0.0.1", 8000, 5000, 0, DEBUG);

                client.AsyncMessageReceived = AsyncMessageReceived;
                client.SyncMessageReceived = SyncMessageReceived;
                client.ServerDisconnected = ConnectToServer;
                // client.LogMessage = LogMessage;

                BigQMessage response;
                if (!client.Login(out response))
                {
                    Console.WriteLine("Unable to login, retrying in five seconds");
                    Thread.Sleep(5000);
                    return ConnectToServer();
                }

                Console.WriteLine("Successfully connected to localhost:8000");
                return true;
            }
            catch (SocketException)
            {
                Console.WriteLine("*** Unable to connect to localhost:8000 (port not reachable)");
                Console.WriteLine("*** Retrying in five seconds");
                Thread.Sleep(5000);
                return ConnectToServer();
            }
            catch (TimeoutException)
            {
                Console.WriteLine("*** Timeout connecting to localhost:8000");
                Console.WriteLine("*** Retrying in five seconds");
                Thread.Sleep(5000);
                return ConnectToServer();
            }
            catch (Exception e)
            {
                Console.WriteLine("*** Unable to connect to localhost:8000 due to the following exception:");
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
