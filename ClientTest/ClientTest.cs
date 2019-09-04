using BigQ.Client;
using BigQ.Core;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClientTest
{
    class Program
    {
        static ClientConfiguration config = null;
        static Client client = null;

        static void Main(string[] args)
        { 
            bool runForever = true;
            string guid = "";
            string msg = "";
            bool priv = false;
            List<ServerClient> clients = new List<ServerClient>();
            List<Channel> channels = new List<Channel>();
            Message response = new Message(); 
             
            Console.WriteLine("");
            Console.WriteLine("BigQ Client");
            Console.WriteLine("");

            Task.Run(() => MaintainConnection());

            while (runForever)
            {
                if (client == null) Console.Write("[OFFLINE] ");
                Console.Write("Command [? for help]: ");

                string cmd = Console.ReadLine();
                if (String.IsNullOrEmpty(cmd)) continue;

                switch (cmd.ToLower())
                {
                    case "?":
                        Menu();
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

                    case "list clients":
                        if (client == null) break;
                        if (client.ListClients(out response, out clients))
                        {
                            Console.WriteLine("ListClients success");
                            if (clients == null || clients.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (ServerClient curr in clients)
                                {
                                    string line =
                                        " " + curr.Name +
                                        " " + curr.Email +
                                        " " + curr.ClientGUID +
                                        " " + curr.Connection.ToString();

                                    Console.WriteLine(line);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("ListClients failed");
                        }
                        break;

                    case "verify client":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.IsClientConnected(guid, out response))
                        {
                            Console.WriteLine("Client " + guid + " is connected");
                        }
                        else
                        {
                            Console.WriteLine("Client " + guid + " is not connected");
                        }
                        break;

                    case "list channels":
                        if (client == null) break;
                        if (client.ListChannels(out response, out channels))
                        {
                            Console.WriteLine("ListChannels success");
                            if (channels == null || channels.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (Channel curr in channels)
                                {
                                    string line = "  " + curr.ChannelGUID + ": " + curr.ChannelName + " owner " + curr.OwnerGUID + " ";
                                    if (curr.Visibility == ChannelVisibility.Private) line += "priv ";
                                    else line += "pub ";
                                    if (curr.Type == ChannelType.Broadcast) line += "bcast ";
                                    else if (curr.Type == ChannelType.Multicast) line += "mcast ";
                                    else if (curr.Type == ChannelType.Unicast) line += "ucast ";
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

                    case "list members":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.ListMembers(guid, out response, out clients))
                        {
                            Console.WriteLine("ListChannelMembers success");
                            if (clients == null || clients.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (ServerClient curr in clients)
                                {
                                    string line =
                                        " " + curr.Name +
                                        " " + curr.Email +
                                        " " + curr.ClientGUID +
                                        " " + curr.Connection.ToString();

                                    Console.WriteLine(line);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("ListChannelMembers failed");
                        }
                        break;

                    case "list subscribers":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.ListSubscribers(guid, out response, out clients))
                        {
                            Console.WriteLine("ListChannelSubscribers success");
                            if (clients == null || clients.Count < 1) Console.WriteLine("(null)");
                            else
                            {
                                foreach (ServerClient curr in clients)
                                {
                                    string line =
                                        " " + curr.Name +
                                        " " + curr.Email +
                                        " " + curr.ClientGUID +
                                        " " + curr.Connection.ToString();

                                    Console.WriteLine(line);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("ListChannelSubscribers failed");
                        }
                        break;

                    case "join channel":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Join(guid, out response))
                        {
                            Console.WriteLine("JoinChannel success");
                        }
                        else
                        {
                            Console.WriteLine("JoinChannel failed");
                        }
                        break;

                    case "leave channel":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Leave(guid, out response))
                        {
                            Console.WriteLine("LeaveChannel success");
                        }
                        else
                        {
                            Console.WriteLine("LeaveChannel failed");
                        }
                        break;

                    case "subscribe":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Subscribe(guid, out response))
                        {
                            Console.WriteLine("SubscribeChannel success");
                        }
                        else
                        {
                            Console.WriteLine("SubscribeChannel failed");
                        }
                        break;

                    case "unsubscribe":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Unsubscribe(guid, out response))
                        {
                            Console.WriteLine("UnsubscribeChannel success");
                        }
                        else
                        {
                            Console.WriteLine("UnsubscribeChannel failed");
                        }
                        break;

                    case "create bcast":
                        if (client == null) break;
                        guid = InputString("Name:", null, false);
                        priv = InputBoolean("Private:", false);
                        if (client.CreateBroadcastChannel(guid, priv, out response))
                        {
                            Console.WriteLine("CreateBroadcastChannel success");
                        }
                        else
                        {
                            Console.WriteLine("CreateBroadcastChannel failed");
                        }
                        break;

                    case "create mcast":
                        if (client == null) break;
                        guid = InputString("Name:", null, false);
                        priv = InputBoolean("Private:", false);
                        if (client.CreateMulticastChannel(guid, priv, out response))
                        {
                            Console.WriteLine("CreateMulticastChannel success");
                        }
                        else
                        {
                            Console.WriteLine("CreateMulticastChannel failed");
                        }
                        break;

                    case "create ucast":
                        if (client == null) break;
                        guid = InputString("Name:", null, false);
                        priv = InputBoolean("Private:", false);
                        if (client.CreateUnicastChannel(guid, priv, out response))
                        {
                            Console.WriteLine("CreateUnicastChannel success");
                        }
                        else
                        {
                            Console.WriteLine("CreateUnicastChannel failed");
                        }
                        break;

                    case "delete channel":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Delete(guid, out response))
                        {
                            Console.WriteLine("DeleteChannel success");
                        }
                        else
                        {
                            Console.WriteLine("DeleteChannel failed");
                        }
                        break;

                    case "send async":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        client.SendPrivateMessageAsync(guid, msg);
                        break;

                    case "send async persist":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        client.SendPrivateMessageAsync(guid, null, msg, true);
                        break;

                    case "send sync":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        client.SendPrivateMessageSync(guid, msg, out response); 
                        if (response != null)
                        {
                            Console.WriteLine("Sync response received for GUID " + response.MessageID);
                            Console.WriteLine(response.ToString());
                        }
                        else
                        {
                            Console.WriteLine("*** No sync response received (null)");
                        }
                        break;

                    case "send channel async":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        client.SendChannelMessageAsync(guid, msg);
                        break;

                    case "send channel sync":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        client.SendChannelMessageSync(guid, msg, out response);
                        if (response != null)
                        {
                            Console.WriteLine("Sync response received for GUID " + response.MessageID);
                            Console.WriteLine(response.ToString());
                        }
                        else
                        {
                            Console.WriteLine("*** No sync response received (null)");
                        }
                        break;

                    case "whoami":
                        if (client == null) break;
                        if (!String.IsNullOrEmpty(client.Config.ClientGUID)) Console.WriteLine("  GUID " + client.Config.ClientGUID);
                        else Console.WriteLine("[not logged in]");
                        break; 

                    default:
                        Console.WriteLine("Unknown command");
                        break;
                }
            }
        }

        static void Menu()
        {                        
            // Console.WriteLine("34567890123456789012345678901234567890123456789012345678901234567890123456789");
            Console.WriteLine("General Commands:");
            Console.WriteLine("  q                    Quit the application");
            Console.WriteLine("  cls                  Clear the screen");
            Console.WriteLine("  echo                 Send an echo message");
            Console.WriteLine("  login                Login to the server");
            Console.WriteLine("  whoami               Display my information");
            Console.WriteLine("");
            Console.WriteLine("Channel Commands:");
            Console.WriteLine("  list channels        Display visible channels");
            Console.WriteLine("  list members         List the members of a channel");
            Console.WriteLine("  list subscribers     List the subscribers of a channel");
            Console.WriteLine("  create bcast         Create a broadcast channel");
            Console.WriteLine("  create mcast         Create a multicast channel");
            Console.WriteLine("  create ucast         Create a unicast channel");
            Console.WriteLine("  delete channel       Delete a channel");
            Console.WriteLine("  join channel         Join a channel");
            Console.WriteLine("  leave channel        Leave a channel");
            Console.WriteLine("  subscribe            Subscribe to a channel");
            Console.WriteLine("  unsubscribe          Unsubscribe from a channel");
            Console.WriteLine("  send channel async   Send a channel message asynchronously");
            Console.WriteLine("  send channel sync    Send a channel message synchronously, expecting a response");
            Console.WriteLine("");
            Console.WriteLine("Messaging Commands:");
            Console.WriteLine("  send async           Send a private message asynchronously");
            Console.WriteLine("  send async persist   Send a private persistent message asynchronously");
            Console.WriteLine("  send sync            Send a private message synchronously, expecting a response");
            Console.WriteLine("  list clients         List connected clients");
            Console.WriteLine("  verify client        Verify if a client is connected"); 
            Console.WriteLine("");
        }

        static bool InputBoolean(string question, bool yesDefault)
        {
            Console.Write(question);

            if (yesDefault) Console.Write(" [Y/n]? ");
            else Console.Write(" [y/N]? ");

            string userInput = Console.ReadLine();

            if (String.IsNullOrEmpty(userInput))
            {
                if (yesDefault) return true;
                return false;
            }

            userInput = userInput.ToLower();

            if (yesDefault)
            {
                if (
                    (String.Compare(userInput, "n") == 0)
                    || (String.Compare(userInput, "no") == 0)
                   )
                {
                    return false;
                }

                return true;
            }
            else
            {
                if (
                    (String.Compare(userInput, "y") == 0)
                    || (String.Compare(userInput, "yes") == 0)
                   )
                {
                    return true;
                }

                return false;
            }
        }

        static List<string> InputStringList(string question)
        {
            Console.WriteLine("Press ENTER with no data to end");
            List<string> ret = new List<string>();
            while (true)
            {
                Console.Write(question + " ");
                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput)) break;
                ret.Add(userInput);
            }
            return ret;
        }

        static string InputString(string question, string defaultAnswer, bool allowNull)
        {
            while (true)
            {
                Console.Write(question);

                if (!String.IsNullOrEmpty(defaultAnswer))
                {
                    Console.Write(" [" + defaultAnswer + "]");
                }

                Console.Write(" ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    if (!String.IsNullOrEmpty(defaultAnswer)) return defaultAnswer;
                    if (allowNull) return null;
                    else continue;
                }

                return userInput;
            }
        }

        static int InputInteger(string question, int defaultAnswer, bool positiveOnly, bool allowZero)
        {
            while (true)
            {
                Console.Write(question);
                Console.Write(" [" + defaultAnswer + "] ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    return defaultAnswer;
                }

                int ret;
                if (!Int32.TryParse(userInput, out ret))
                {
                    Console.WriteLine("Please enter a valid integer.");
                    continue;
                }

                if (ret == 0 && allowZero)
                {
                    return 0;
                }

                if (ret < 0 && positiveOnly)
                {
                    Console.WriteLine("Please enter a value greater than zero.");
                    continue;
                }

                return ret;
            }
        }

        static decimal InputDecimal(string question, decimal defaultAnswer, bool positiveOnly, bool allowZero)
        {
            while (true)
            {
                Console.Write(question);
                Console.Write(" [" + defaultAnswer + "] ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    return defaultAnswer;
                }

                decimal ret;
                if (!Decimal.TryParse(userInput, out ret))
                {
                    Console.WriteLine("Please enter a valid decimal.");
                    continue;
                }

                if (ret == 0 && allowZero)
                {
                    return 0;
                }

                if (ret < 0 && positiveOnly)
                {
                    Console.WriteLine("Please enter a value greater than zero.");
                    continue;
                }

                return ret;
            }
        }

        #region Delegates

        static void MaintainConnection()
        {
            bool firstRun = true;

            while (true)
            {
                try
                {
                    if (firstRun)
                    {

                    }
                    else
                    {
                        firstRun = false;
                        Thread.Sleep(5000);
                    }

                    if (config == null)
                    {
                        config = ClientConfiguration.Default();
                        config.ClientGUID = Guid.NewGuid().ToString();
                        config.Email = config.ClientGUID;
                        config.Name = config.ClientGUID;
                        config.ServerGUID = "00000000-0000-0000-0000-000000000000";
                        config.SyncTimeoutMs = 30000;

                        config.TcpServer = new ClientConfiguration.TcpServerSettings();
                        config.TcpServer.Debug = true;
                        config.TcpServer.Enable = true;
                        config.TcpServer.Ip = "127.0.0.1";
                        config.TcpServer.Port = 8000;
                    }

                    if (client == null || !client.Connected || !client.LoggedIn)
                    {
                        Console.WriteLine("Attempting to connect to server");
                        if (client != null) client.Dispose();

                        client = new Client(config);

                        client.Callbacks.AsyncMessageReceived = AsyncMessageReceived;
                        client.Callbacks.SyncMessageReceived = SyncMessageReceived;
                        client.Callbacks.ServerDisconnected = ServerDisconnected;
                        client.Callbacks.ServerConnected = ServerConnected;
                        client.Callbacks.ClientJoinedServer = ClientJoinedServer;
                        client.Callbacks.ClientLeftServer = ClientLeftServer;
                        client.Callbacks.ClientJoinedChannel = ClientJoinedChannel;
                        client.Callbacks.ClientLeftChannel = ClientLeftChannel;
                        client.Callbacks.SubscriberJoinedChannel = SubscriberJoinedChannel;
                        client.Callbacks.SubscriberLeftChannel = SubscriberLeftChannel;
                        client.Callbacks.ChannelCreated = ChannelCreated;
                        client.Callbacks.ChannelDestroyed = ChannelDestroyed;

                        Console.WriteLine("Client connected, logging in");
                        Message response;
                        if (!client.Login(out response))
                        {
                            Console.WriteLine("Unable to login, retrying");
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    client = null;
                }
            }
        }
         
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ServerDisconnected()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Server disconnected"); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ServerConnected()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Server connected and logged in"); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task AsyncMessageReceived(Message msg)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine(msg.ToString());
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task<byte[]> SyncMessageReceived(Message msg)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Received sync message: " + msg.ToString());
            Console.WriteLine("Press ENTER and then type your response");
            Console.WriteLine("(The menu command parser is expecting input so press ENTER first!)");
            Console.Write("Response [ENTER for 'hello!']: ");
            string resp = Console.ReadLine();
            if (!String.IsNullOrEmpty(resp)) return Encoding.UTF8.GetBytes(resp);
            return null;
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ClientJoinedServer(string clientGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Client " + clientGuid + " joined the server"); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ClientLeftServer(string clientGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Client " + clientGuid + " left the server"); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ClientJoinedChannel(string clientGuid, string channelGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Client " + clientGuid + " joined channel " + channelGuid); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ClientLeftChannel(string clientGuid, string channelGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Client " + clientGuid + " left channel " + channelGuid); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task SubscriberJoinedChannel(string subscriberGuid, string channelGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Subscriber " + subscriberGuid + " joined channel " + channelGuid); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task SubscriberLeftChannel(string subscriberGuid, string channelGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Subscriber " + subscriberGuid + " left channel " + channelGuid); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ChannelCreated(string channelGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Channel " + channelGuid + " created"); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ChannelDestroyed(string channelGuid)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Channel " + channelGuid + " destroyed"); 
        }
           
        #endregion
    }
}
