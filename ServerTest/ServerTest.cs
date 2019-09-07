using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using BigQ.Server;
using BigQ.Server.Classes;

namespace ServerTest
{
    class Program
    {
        static ServerConfiguration config = null;
        static Server server = null;

        static void Main(string[] args)
        { 
            bool runForever = true;
            List<ServerClient> clients = new List<ServerClient>();
            List<ServerClient> members = new List<ServerClient>();
            List<ServerClient> subscribers = new List<ServerClient>();
            List<Channel> channels = new List<Channel>();
            Dictionary<string, string> guidMaps = new Dictionary<string, string>();
            Dictionary<string, DateTime> sendMaps = new Dictionary<string, DateTime>();
            List<User> users = new List<User>();
            List<Permission> perms = new List<Permission>();

            Console.WriteLine("");
            Console.WriteLine("BigQ Server");
            Console.WriteLine("");

            StartServer().Wait();

            while (runForever)
            {
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

                    case "channels":
                        channels = server.ListChannels();
                        if (channels != null)
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
                        members = server.ListMembers(
                            InputString("Channel GUID:", null, false));
                        if (members != null)
                        {
                            foreach (ServerClient curr in members)
                            {
                                string line =
                                    "  " + curr.IpPort +
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
                        subscribers = server.ListSubscribers(
                            InputString("Channel GUID:", null, false));
                        if (subscribers != null)
                        {
                            foreach (ServerClient curr in subscribers)
                            {
                                string line =
                                    "  " + curr.IpPort +
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

                    case "create":
                        server.Create(
                            (ChannelType)(Enum.Parse(typeof(ChannelType), InputString("Channel type [Broadcast|Unicast|Multicast]:", "Broadcast", false))),
                            InputString("Channel name:", null, false),
                            InputString("Channel GUID:", null, true),
                            InputBoolean("Private:", true));
                        break;
                         
                    case "delete":
                        if (server.Delete(
                            InputString("Channel GUID:", null, false)))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "clients":
                        clients = server.ListClients();
                        if (clients != null)
                        {
                            foreach (ServerClient curr in clients)
                            {
                                string line =
                                    "  " + curr.IpPort +
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

                    case "users file":
                        users = server.ListUsersFile();
                        if (users != null && users.Count > 0)
                        {
                            foreach (User curr in users)
                            {
                                Console.WriteLine("  Email " + curr.Email + " Password " + curr.Password + " Notes " + curr.Notes + " Permission " + curr.Permission);
                                if (curr.IPWhiteList != null && curr.IPWhiteList.Count > 0)
                                {
                                    string whitelist = "  Accepted IP: ";
                                    foreach (string currIP in curr.IPWhiteList)
                                    {
                                        whitelist += currIP + " ";
                                    }
                                    Console.WriteLine(whitelist);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "perms file":
                        perms = server.ListPermissionsFile();
                        if (perms != null && perms.Count > 0)
                        {
                            foreach (Permission curr in perms)
                            {
                                string permstr = "  Name " + curr.Name + " Login " + curr.Login + " Permissions ";
                                if (curr.Permissions != null && curr.Permissions.Count > 0)
                                {
                                    foreach (string currstr in curr.Permissions)
                                    {
                                        permstr += currstr + " ";
                                    }
                                }
                                else
                                {
                                    permstr += "all";
                                }
                                Console.WriteLine(permstr);
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
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
            Console.WriteLine("  users file           Show the users file");
            Console.WriteLine("  perms file           Show the permissions file"); 
            Console.WriteLine("");
            Console.WriteLine("Channel Commands:");
            Console.WriteLine("  channels             List all channels");
            Console.WriteLine("  members              List channel members");
            Console.WriteLine("  subscribers          List channel subscribers");
            Console.WriteLine("  create               Create a channel");
            Console.WriteLine("  delete               Delete a channel");
            Console.WriteLine("");
            Console.WriteLine("Client Commands:");
            Console.WriteLine("  clients              List connected clients"); 
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

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task StartServer()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Attempting to start server");

            if (server != null) server.Dispose();
            server = null;
              
            config = ServerConfiguration.Default();
            config.TcpServer.Debug = false;

            server = new Server(config);
            server.Callbacks.MessageReceived = MessageReceived;
            server.Callbacks.ServerStopped = StartServer;
            server.Callbacks.ClientConnected = ClientConnected;
            server.Callbacks.ClientLogin = ClientLogin;
            server.Callbacks.ClientDisconnected = ClientDisconnected;  
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task MessageReceived(Message msg)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        { 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ClientConnected(ServerClient client)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Client connected: " + client.IpPort); 
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ClientLogin(ServerClient client)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Client logged in: " + client.ClientGUID + " from " + client.IpPort);  
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        static async Task ClientDisconnected(ServerClient client)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            Console.WriteLine("Client disconnected: GUID " + client.ClientGUID + " from " + client.IpPort); 
        }
         
        #endregion
    }
}
