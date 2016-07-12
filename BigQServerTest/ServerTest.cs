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
            // initialize
            StartServer();
            
            bool RunForever = true;
            while (RunForever)
            {
                // Console.WriteLine("34567890123456789012345678901234567890123456789012345678901234567890123456789");
                Console.WriteLine("---");
                Console.WriteLine("Commands: q quit cls listchannels listchannelsubscribers count");
                Console.WriteLine("          listclients listclientguidmaps listclientactivesendmaps");
                Console.WriteLine("          listusersfile  listpermissionsfile");
                Console.Write("Command: ");
                string cmd = Console.ReadLine();
                if (String.IsNullOrEmpty(cmd)) continue;

                string guid = "";
                
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

                    case "listchannels":
                        List<Channel> channels = server.ListChannels();
                        if (channels != null)
                        {
                            foreach (Channel curr in channels)
                            {
                                if (curr.Private == 1)
                                {
                                    Console.WriteLine("  " + curr.Guid + ": " + curr.ChannelName + " (owner " + curr.OwnerGuid + ") [priv]");
                                }
                                else
                                {
                                    Console.WriteLine("  " + curr.Guid + ": " + curr.ChannelName + " (owner " + curr.OwnerGuid + ") [pub]");
                                }                                
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
                        List<Client> subscribers = server.ListChannelSubscribers(guid);
                        if (subscribers != null)
                        {
                            foreach (Client curr in subscribers)
                            {
                                Console.WriteLine("  " + curr.SourceIP + ":" + curr.SourcePort + "  " + curr.Email + "  [" + curr.ClientGUID + "]");
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "listclients":
                        List<Client> clients = server.ListClients();
                        if (clients != null)
                        {
                            foreach (Client curr in clients)
                            {
                                Console.Write("  " + curr.SourceIP + ":" + curr.SourcePort + " ");

                                if (String.IsNullOrEmpty(curr.ClientGUID)) Console.Write("[login pending] ");
                                else Console.Write(curr.Email + " [" + curr.ClientGUID + "] ");

                                if (curr.IsTCP) Console.Write("[TCP] ");
                                else if (curr.IsTCPSSL) Console.Write("[TCP SSL] ");
                                else if (curr.IsWebsocket) Console.Write("[WS] ");
                                else if (curr.IsWebsocketSSL) Console.Write("[WS SSL] ");
                                Console.WriteLine("");
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "listclientguidmaps":
                        Dictionary<string, string> guidMaps = server.ListClientGuidMaps();
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

                    case "listclientactivesendmaps":
                        Dictionary<string, string> sendMaps = server.ListClientActiveSendMap();
                        if (sendMaps != null && sendMaps.Count > 0)
                        {
                            foreach (KeyValuePair<string, string> curr in sendMaps)
                            {
                                Console.WriteLine("  Recipient " + curr.Key + "  Sender " + curr.Value);
                            }
                        }
                        else
                        {
                            Console.WriteLine("(null)");
                        }
                        break;

                    case "count":
                        Console.WriteLine("Active connection count: " + server.ConnectionCount());
                        break;

                    case "listusersfile":
                        List<User> users = server.ListCurrentUsersFile();
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
                        List<Permission> perms = server.ListCurrentPermissionsFile();
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

        static bool MessageReceived(Message msg)
        {
            // Console.WriteLine(msg.ToString());
            return true;
        }

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
            server = new Server("server.json");
            server.MessageReceived = MessageReceived;
            server.ServerStopped = StartServer;
            server.ClientConnected = ClientConnected;
            server.ClientLogin = ClientLogin;
            server.ClientDisconnected = ClientDisconnected;
            server.LogMessage = LogMessage;
            server.LogMessage = null;

            return true;
        }

        static bool ClientConnected(Client client)
        {
            // client disconnected
            Console.WriteLine("ClientConnected received notice of connect from " + client.IpPort());
            return true;
        }

        static bool ClientLogin(Client client)
        {
            // client disconnected
            Console.WriteLine("ClientConnected received notice of connect of client GUID " + client.ClientGUID + " from " + client.IpPort());
            return true;
        }

        static bool ClientDisconnected(Client client)
        {
            // client disconnected
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
