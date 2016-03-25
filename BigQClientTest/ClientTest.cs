using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BigQ;

namespace BigQClientTest
{
    class ClientTest
    {
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

            BigQClient client = new BigQClient(null, null, "127.0.0.1", 8000, 10000, false);
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
                Console.WriteLine("          sendprivasync sendprivsync sendchannel listclients");
                Console.WriteLine("          whoami");
                Console.Write("Command: ");
                string cmd = Console.ReadLine();
                if (String.IsNullOrEmpty(cmd)) continue;

                string guid = "";
                string msg = "";
                int priv = 0;

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
                        client.Login();
                        break;

                    case "listclients":
                        client.ListClients();
                        break;

                    case "listchannels":
                        client.ListChannels();
                        break;

                    case "listchannelsubscribers":
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        client.ListChannelSubscribers(guid);
                        break;

                    case "joinchannel":
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        client.JoinChannel(guid);
                        break;

                    case "leavechannel":
                        Console.Write("Channel GUID: ");
                        guid = Console.ReadLine();
                        client.LeaveChannel(guid);
                        break;

                    case "createchannel":
                        Console.Write("Name: ");
                        guid = Console.ReadLine();
                        Console.Write("Private (0/1): ");
                        priv = Convert.ToInt32(Console.ReadLine());
                        client.CreateChannel(guid, priv);
                        break;

                    case "deletechannel":
                        Console.Write("GUID: ");
                        guid = Console.ReadLine();
                        client.DeleteChannel(guid);
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
            BigQClient client = new BigQClient(null, null, "127.0.0.1", 8000, 10000, false);
            return true;
        }

        static bool LogMessage(string msg)
        {
            Console.WriteLine("BigQClient message: " + msg);
            return true;
        }

        #endregion
    }
}
