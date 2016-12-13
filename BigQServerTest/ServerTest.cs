using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BigQ;

namespace BigQServerTest
{
    class ServerTest
    {
        static Server server;

        static void Main(string[] args)
        {
            bool runForever = true;
            string guid = "";
            List<Client> clients = new List<Client>();
            List<Client> members = new List<Client>();
            List<Client> subscribers = new List<Client>();
            List<Channel> channels = new List<Channel>();
            Dictionary<string, string> guidMaps = new Dictionary<string, string>();
            Dictionary<string, DateTime> sendMaps = new Dictionary<string, DateTime>();
            List<User> users = new List<User>();
            List<Permission> perms = new List<Permission>();

            Console.WriteLine("BigQ Server Version " + System.Reflection.Assembly.GetEntryAssembly().GetName().Version.ToString());
            Console.WriteLine("Starting BigQ server at " + DateTime.Now.ToUniversalTime().ToString("MM/dd/yyyy HH:mm:ss") + " UTC");
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
                        Console.WriteLine("Available Commands:");
                        Console.WriteLine("  q  quit  cls  debugon  debugoff");
                        Console.WriteLine("  listchannels  listchannelmembers  listchannelsubscribers");
                        Console.WriteLine("  listclients  listclientguidmaps");
                        Console.WriteLine("  listclientactivesend  clearclientactivesend");
                        Console.WriteLine("  listusersfile  listpermissionsfile  connectioncount");
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

                    case "debugon":
                        server.Config.Debug.Enable = true;
                        server.Config.Debug.ConsoleLogging = true;
                        server.Config.Debug.MsgResponseTime = true;
                        break;

                    case "debugoff":
                        server.Config.Debug.Enable = false;
                        server.Config.Debug.ConsoleLogging = false;
                        server.Config.Debug.MsgResponseTime = false;
                        break;

                    case "listchannels":
                        channels = server.ListChannels();
                        if (channels != null)
                        {
                            foreach (Channel curr in channels)
                            {
                                string line = "  " + curr.ChannelGUID + ": " + curr.ChannelName + " owner " + curr.OwnerGUID + " ";
                                if (curr.Private == 1) line += "priv ";
                                else line += "pub ";
                                if (curr.Broadcast == 1) line += "bcast ";
                                else if (curr.Multicast == 1) line += "mcast ";
                                else if (curr.Unicast == 1) line += "ucast ";
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
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        members = server.ListChannelMembers(guid);
                        if (members != null)
                        {
                            foreach (Client curr in members)
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
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "listchannelsubscribers":
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        subscribers = server.ListChannelSubscribers(guid);
                        if (subscribers != null)
                        {
                            foreach (Client curr in subscribers)
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
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "listclients":
                        clients = server.ListClients();
                        if (clients != null)
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
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "listclientguidmaps":
                        guidMaps = server.ListClientGUIDMaps();
                        if (guidMaps != null && guidMaps.Count > 0)
                        {
                            foreach (KeyValuePair<string, string> curr in guidMaps)
                            {
                                Console.WriteLine("  GUID " + curr.Key + "  IP:port " + curr.Value);
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "listclientactivesend":
                        sendMaps = server.ListClientActiveSend();
                        if (sendMaps != null && sendMaps.Count > 0)
                        {
                            foreach (KeyValuePair<string, DateTime> curr in sendMaps)
                            {
                                Console.WriteLine("  Recipient " + curr.Key + "  Added " + curr.Value + " UTC");
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "clearclientactivesend":
                        server.ClearClientActiveSend();
                        break;

                    case "connectioncount":
                        Console.WriteLine("Active connection count: " + server.ConnectionCount());
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

                    default:
                        Console.WriteLine("Unknown command");
                        break;
                }
            }
        }

        #region Delegates

        static bool StartServer()
        {
            //
            // restart
            //
            Console.WriteLine("Attempting to start/restart server");
            if (server != null) server.Close();
            server = null;

            //
            // initialize with default configuration
            //
            server = new Server(null);

            server.Config.Debug.Enable = true;
            server.Config.Debug.ConsoleLogging = true;
            server.Config.Debug.MsgResponseTime = false;
            server.Config.Debug.ConnectionMgmt = false;
            server.Config.Debug.ChannelMgmt = false;
            server.Config.Debug.SendHeartbeat = true;
            server.Config.Heartbeat.IntervalMs = 1000;

            server.MessageReceived = MessageReceived;
            server.ServerStopped = StartServer;
            server.ClientConnected = ClientConnected;
            server.ClientLogin = ClientLogin;
            server.ClientDisconnected = ClientDisconnected;
            server.LogMessage = LogMessage;
            server.LogMessage = null;

            return true;
        }

        static bool MessageReceived(Message msg)
        {
            // Console.WriteLine(msg.ToString());
            return true;
        }

        static bool ClientConnected(Client client)
        {
            Console.WriteLine("ClientConnected received notice of connect from " + client.IpPort());
            return true;
        }

        static bool ClientLogin(Client client)
        {
            Console.WriteLine("ClientConnected received notice of connect of client GUID " + client.ClientGUID + " from " + client.IpPort());
            return true;
        }

        static bool ClientDisconnected(Client client)
        {
            Console.WriteLine("ClientDisconnected received notice of disconnect of client GUID " + client.ClientGUID + " from " + client.IpPort());
            return true;
        }

        static bool LogMessage(string msg)
        {
            Console.WriteLine("BigQServer message: " + msg);
            return true;
        }

        #endregion
    }
}
