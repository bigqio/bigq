using BigQ.Core;
using BigQ.Server;
using System;
using System.Collections.Generic;

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

            StartServer();

            while (runForever)
            {
                Console.Write("Command [? for help]: ");
                string cmd = Console.ReadLine();
                if (String.IsNullOrEmpty(cmd)) continue;

                switch (cmd.ToLower())
                {
                    case "?":
                        // Console.WriteLine("34567890123456789012345678901234567890123456789012345678901234567890123456789");
                        Console.WriteLine("General Commands:");
                        Console.WriteLine("  q  quit  cls  listusersfile  listpermissionsfile");
                        Console.WriteLine("");
                        Console.WriteLine("Channel Commands:");
                        Console.WriteLine("  listchannels  listchannelmembers  listchannelsubscribers");
                        Console.WriteLine("  createbcastchanel  createucastchannel  createmcastchannel");
                        Console.WriteLine("  deletechannel");
                        Console.WriteLine("");
                        Console.WriteLine("Client Commands:");
                        Console.WriteLine("  listclients");
                        Console.WriteLine("");
                        Console.WriteLine("Persistence Commands:");
                        Console.WriteLine("  queuedepth  rcptqueuedepth");
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

                    case "listchannels":
                        channels = server.ListChannels();
                        if (channels != null)
                        {
                            foreach (Channel curr in channels)
                            {
                                string line = "  " + curr.ChannelGUID + ": " + curr.ChannelName + " owner " + curr.OwnerGUID + " ";
                                if (curr.Private) line += "priv ";
                                else line += "pub ";
                                if (curr.Broadcast) line += "bcast ";
                                else if (curr.Multicast) line += "mcast ";
                                else if (curr.Unicast) line += "ucast ";
                                else line += "unknown ";

                                Console.WriteLine(line);
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "listchannelmembers":
                        members = server.ListChannelMembers(
                            Common.InputString("Channel GUID:", null, false));
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

                    case "listchannelsubscribers":
                        subscribers = server.ListChannelSubscribers(
                            Common.InputString("Channel GUID:", null, false));
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

                    case "createbcastchannel":
                        server.CreateBroadcastChannel(
                            Common.InputString("Channel name:", null, false),
                            Common.InputString("Channel GUID:", null, true),
                            Common.InputBoolean("Private:", true));
                        break;

                    case "createucastchannel":
                        server.CreateUnicastChannel(
                            Common.InputString("Channel name:", null, false),
                            Common.InputString("Channel GUID:", null, true),
                            Common.InputBoolean("Private:", true)); break;

                    case "createmcastchannel":
                        server.CreateMulticastChannel(
                            Common.InputString("Channel name:", null, false),
                            Common.InputString("Channel GUID:", null, true),
                            Common.InputBoolean("Private:", true));
                        break;

                    case "deletechannel":
                        if (server.DeleteChannel(
                            Common.InputString("Channel GUID:", null, false)))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "listclients":
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
                         
                    case "listusersfile":
                        users = server.ListCurrentUsersFile();
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

                    case "listpermissionsfile":
                        perms = server.ListCurrentPermissionsFile();
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

                    case "queuedepth":
                        Console.WriteLine(server.PersistentQueueDepth());
                        break;

                    case "rcptqueuedepth":
                        string guid = Common.InputString("Recipient GUID:", null, false);
                        Console.WriteLine(server.PersistentQueueDepth(guid));
                        break;

                    default:
                        Console.WriteLine("Unknown command");
                        break;
                }
            }
        }

        #region Delegates
         
        static bool StartServer()
        {
            Console.WriteLine("Attempting to start server");

            if (server != null) server.Dispose();
            server = null;
              
            config = ServerConfiguration.Default();
            config.TcpServer.Debug = true;

            server = new Server(config);
            server.Callbacks.MessageReceived = MessageReceived;
            server.Callbacks.ServerStopped = StartServer;
            server.Callbacks.ClientConnected = ClientConnected;
            server.Callbacks.ClientLogin = ClientLogin;
            server.Callbacks.ClientDisconnected = ClientDisconnected;
            
            server.Callbacks.MessageReceived = MessageReceived;
            server.Callbacks.ServerStopped = StartServer;
            server.Callbacks.ClientConnected = ClientConnected;
            server.Callbacks.ClientLogin = ClientLogin;
            server.Callbacks.ClientDisconnected = ClientDisconnected;

            return true;
        }

        static bool MessageReceived(Message msg)
        {
            // Console.WriteLine(msg.ToString());
            return true;
        }

        static bool ClientConnected(ServerClient client)
        {
            Console.WriteLine("ClientConnected received notice of client connect from " + client.IpPort);
            return true;
        }

        static bool ClientLogin(ServerClient client)
        {
            Console.WriteLine("ClientConnected received notice of client login GUID " + client.ClientGUID + " from " + client.IpPort);
            return true;
        }

        static bool ClientDisconnected(ServerClient client)
        {
            Console.WriteLine("ClientDisconnected received notice of disconnect of client GUID " + client.ClientGUID + " from " + client.IpPort);
            return true;
        }
         
        #endregion
    }
}
