using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BigQ;

namespace BigQClientTest
{
    class ClientTest
    {
        static BigQClient client;

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

            client = new BigQClient(null, null, "127.0.0.1", 8000, 5000, false);
            client.AsyncMessageReceived = AsyncMessageReceived;
            client.SyncMessageReceived = SyncMessageReceived;
            client.ServerDisconnected = ServerDisconnected;
            // client.LogMessage = LogMessage;

            bool RunForever = true;
            while (RunForever)
            {
                // Console.WriteLine("34567890123456789012345678901234567890123456789012345678901234567890123456789");
                Console.WriteLine("---");
                Console.WriteLine("Commands: q quit cls echo login listchannels listchannelsubscribers joinchannel");
                Console.WriteLine("          leavechannel createchannel deletechannel");
                Console.WriteLine("          sendprivasync sendprivsync sendchannel listclients isclientconnected");
                Console.WriteLine("          whoami pendingsyncrequests");
                Console.Write("Command: ");
                string cmd = Console.ReadLine();
                if (String.IsNullOrEmpty(cmd)) continue;

                string guid = "";
                string msg = "";
                int priv = 0;
                List<BigQClient> clients;
                List<BigQChannel> channels;
                BigQMessage response;
                Dictionary<string, DateTime> pendingRequests;

                switch (cmd.ToLower())
                {
                    case "q":
                    case "quit":
                        RunForever = false;
                        break;

                    case "c":
                    case "cls":
                        Console.Clear();
                        break;

                    case "echo":
                        client.Echo();
                        break;

                    case "login":
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
                        Console.Write("Recipient GUID: ");
                        guid = Console.ReadLine();
                        Console.Write("Message: ");
                        msg = Console.ReadLine();
                        client.SendPrivateMessageAsync(guid, msg);
                        break;

                    case "sendprivsync":
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
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        Console.Write("Message: ");
                        msg = Console.ReadLine();
                        client.SendChannelMessage(guid, msg);
                        break;

                    case "whoami":
                        Console.WriteLine("You are GUID: " + client.ClientGuid);
                        break;

                    case "pendingsyncrequests":
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

        static object SyncMessageReceived(BigQMessage msg)
        {
            Console.WriteLine("Received sync message: " + msg.ToString());
            Console.WriteLine("Press ENTER and then type your response");
            Console.WriteLine("(The menu command parser is expecting input so press ENTER first!)");
            Console.Write("Response [ENTER for 'hello!']: ");
            string resp = Console.ReadLine();
            return resp;
        }

        static bool ServerDisconnected()
        {
            Console.WriteLine("*** Disconnection, attempting reconnection ***");
            bool success = false;

            try
            {
                client = new BigQClient(null, null, "127.0.0.1", 8000, 5000, false);
                success = true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Unable to connect to server: " + e.Message);;
            }

            if (!success)
            {
                Console.WriteLine("Retrying in five seconds...");
                Thread.Sleep(5000);
                return ServerDisconnected();                
            }

            BigQMessage response;
            if (client.Login(out response))
            {
                Console.WriteLine("Login success");
                return true;
            }
            else
            {
                Console.WriteLine("Login failed");
                client = null;
                Console.WriteLine("Retrying in five seconds...");
                Thread.Sleep(5000);
                return ServerDisconnected();
            }
        }

        static bool LogMessage(string msg)
        {
            Console.WriteLine("BigQClient message: " + msg);
            return true;
        }

        #endregion
    }
}
