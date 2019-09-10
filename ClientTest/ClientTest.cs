using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using BigQ.Client;
using BigQ.Client.Classes;

namespace ClientTest
{
    class Program
    {
        static bool debug = false;
        static ClientConfiguration config = null;
        static Client client = null; 
        static Random random = new Random();

        static void Main(string[] args)
        { 
            bool runForever = true;
            ChannelType channelType;
            string guid = "";
            string msg = "";
            bool priv = false;
            List<ServerClient> clients = new List<ServerClient>();
            List<Channel> channels = new List<Channel>();
            Message response = new Message();
            string respString = null;

            Console.WriteLine("");
            Console.WriteLine("BigQ Client");
            Console.WriteLine("");

            Task.Run(() => MaintainConnection());

            while (runForever)
            {
                if (client == null || !client.Connected) Console.Write("[offline] ");
                Console.Write("Command [? for help]: ");

                string cmd = Console.ReadLine();
                if (String.IsNullOrEmpty(cmd)) continue;

                switch (cmd.ToLower())
                {
                    #region General-Commands

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

                    case "dispose":
                        client.Dispose();
                        break;

                    case "echo":
                        if (client == null) break;
                        client.Echo();
                        break;

                    case "login":
                        if (client == null) break;
                        if (client.Login(out response))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "whoami":
                        if (client == null) break;
                        if (!String.IsNullOrEmpty(client.Config.ClientGUID)) Console.WriteLine("  GUID " + client.Config.ClientGUID);
                        else Console.WriteLine("[not logged in]");
                        break;

                    case "ison":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        Console.WriteLine(client.IsClientConnected(guid));
                        break;

                    #endregion

                    #region Channel-Commands

                    case "channels":
                        if (client == null) break;
                        channels = client.ListChannels();
                        if (channels != null && channels.Count > 0)
                        {
                            foreach (Channel curr in channels)
                            {
                                string line =
                                    "  " + curr.ChannelGUID +
                                    ": " + curr.ChannelName +
                                    " owner " + curr.OwnerGUID +
                                    " " + curr.Visibility.ToString() +
                                    " " + curr.Type.ToString();

                                Console.WriteLine(line);
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "members":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        clients = client.ListMembers(guid);
                        if (clients != null && clients.Count > 0)
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
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "subscribers":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        clients = client.ListSubscribers(guid);
                        if (clients != null && clients.Count > 0)
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
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "join":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Join(guid, out response))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "leave":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Leave(guid, out response))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "subscribe":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Subscribe(guid, out response))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "unsubscribe":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Unsubscribe(guid, out response))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "create":
                        if (client == null) break;
                        channelType = (ChannelType)(Enum.Parse(typeof(ChannelType), InputString("Channel type [Broadcast|Unicast|Multicast]:", "Broadcast", false)));
                        guid = InputString("Name:", null, false);
                        priv = InputBoolean("Private:", false);
                        if (client.Create(channelType, guid, priv, out response))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "delete":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        if (client.Delete(guid, out response))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "send channel":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        if (!client.SendChannel(guid, msg).Result)
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "send channel sync":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        client.SendChannelSync(guid, msg, out respString);
                        if (!String.IsNullOrEmpty(respString))
                        {
                            Console.WriteLine("Response: " + respString);
                        }
                        else
                        {
                            Console.WriteLine("*** No response");
                        }
                        break;

                    #endregion

                    #region Messaging-Commands

                    case "clients":
                        if (client == null) break;
                        clients = client.ListClients();
                        if (clients != null && clients.Count > 0)
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
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "send":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        if (!client.Send(guid, msg).Result)
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "send persist":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        if (!client.Send(guid, null, msg, true).Result)
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "send sync":
                        if (client == null) break;
                        guid = InputString("GUID:", null, false);
                        msg = InputString("Message:", null, false);
                        respString = null;
                        client.SendSync(guid, msg, out respString);
                        if (!String.IsNullOrEmpty(respString))
                        {
                            Console.WriteLine("Response: " + respString);
                        }
                        else
                        {
                            Console.WriteLine("*** No response");
                        }
                        break;

                    #endregion 
                }
            }
        }

        static void Menu()
        {                        
            // Console.WriteLine("34567890123456789012345678901234567890123456789012345678901234567890123456789");
            Console.WriteLine("General Commands:");
            Console.WriteLine("  q                    Quit the application");
            Console.WriteLine("  cls                  Clear the screen");
            Console.WriteLine("  dispose              Dispose the client");
            Console.WriteLine("  echo                 Send an echo message");
            Console.WriteLine("  login                Login to the server");
            Console.WriteLine("  whoami               Display my information");
            Console.WriteLine("  ison                 Verify if a client is connected");
            Console.WriteLine("");
            Console.WriteLine("Channel Commands:");
            Console.WriteLine("  channels             Display visible channels");
            Console.WriteLine("  members              List the members of a channel");
            Console.WriteLine("  subscribers          List the subscribers of a channel");
            Console.WriteLine("  create               Create a channel");
            Console.WriteLine("  delete               Delete a channel");
            Console.WriteLine("  join                 Join a channel");
            Console.WriteLine("  leave                Leave a channel");
            Console.WriteLine("  subscribe            Subscribe to a channel");
            Console.WriteLine("  unsubscribe          Unsubscribe from a channel");
            Console.WriteLine("  send channel         Send a channel message asynchronously");
            Console.WriteLine("  send channel sync    Send a channel message synchronously, expecting a response");
            Console.WriteLine("");
            Console.WriteLine("Messaging Commands:");
            Console.WriteLine("  clients              List connected clients");
            Console.WriteLine("  send                 Send a private message asynchronously");
            Console.WriteLine("  send persist         Send a private persistent message asynchronously");
            Console.WriteLine("  send sync            Send a private message synchronously, expecting a response");
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

        static string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        static string RandomName()
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

            int selected = random.Next(0, names.Length - 1);
            return names[selected];
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
                        config.Name = RandomName();
                        config.ClientGUID = RandomString(8);
                        config.Email = config.Name + "@" + config.ClientGUID + ".com";
                        config.ServerGUID = "00000000-0000-0000-0000-000000000000";
                        config.SyncTimeoutMs = 30000;

                        config.TcpServer = new ClientConfiguration.TcpServerSettings();
                        config.TcpServer.Debug = debug;
                        config.TcpServer.Enable = true;
                        config.TcpServer.Ip = "127.0.0.1";
                        config.TcpServer.Port = 8000;
                    }

                    if (client == null || !client.Connected || !client.LoggedIn)
                    { 
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

                        Console.WriteLine("Logging in");
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
            Console.WriteLine("Server connected"); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task AsyncMessageReceived(Message msg)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("[" + msg.SenderGUID + "]: " + Encoding.UTF8.GetString(msg.Data));
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task<byte[]> SyncMessageReceived(Message msg)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("*** Synchronous message ***");
            Console.WriteLine("[" + msg.SenderGUID + "]: " + Encoding.UTF8.GetString(msg.Data));
            Console.WriteLine("");
            Console.WriteLine("Press ENTER FIRST and then type your response and press ENTER again.");
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
