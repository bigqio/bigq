using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Object containing configuration for a BigQ server instance.
    /// </summary>
    [Serializable]
    public class ServerConfiguration
    {
        #region Public-Class-Members

        /// <summary>
        /// The version number of the BigQ binary.
        /// </summary>
        public string Version;

        /// <summary>
        /// Whether or not the BigQ server should accept SSL certificates that are self-signed or unable to be verified.
        /// </summary>
        public bool AcceptInvalidSSLCerts;

        /// <summary>
        /// Optional, overrides the GUID used by the server.  By default, the server uses 00000000-0000-0000-0000-000000000000.
        /// </summary>
        public string GUID;

        /// <summary>
        /// Settings related to files referenced by BigQ.
        /// </summary>
        public FilesSettings Files;

        /// <summary>
        /// Settings related to heartbeat exchanges between client and server.
        /// </summary>
        public HeartbeatSettings Heartbeat;

        /// <summary>
        /// Settings related to notifications sent by the server.
        /// </summary>
        public NotificationSettings Notification;

        /// <summary>
        /// Settings related to debugging server behavior and log messages.
        /// </summary>
        public DebugSettings Debug;

        /// <summary>
        /// Settings related to the BigQ TCP server (no SSL).
        /// </summary>
        public TcpServerSettings TcpServer;

        /// <summary>
        /// Settings related to the BigQ TCP server (with SSL).
        /// </summary>
        public TcpSSLServerSettings TcpSSLServer;

        /// <summary>
        /// Settings related to the BigQ websocket server (no SSL).
        /// </summary>
        public WebsocketServerSettings WebsocketServer;

        /// <summary>
        /// Settings related to the BigQ websocket server (with SSL).
        /// </summary>
        public WebsocketSSLServerSettings WebsocketSSLServer;

        /// <summary>
        /// Channels to be created by the server on start or restar.
        /// </summary>
        public List<Channel> ServerChannels;

        #endregion

        #region Public-Subordinate-Classes

        /// <summary>
        /// Settings related to files referenced by BigQ.
        /// </summary>
        public class FilesSettings
        {
            /// <summary>
            /// The JSON file containing user definitions.
            /// </summary>
            public string UsersFile;

            /// <summary>
            /// The JSON file containing permission definitions.
            /// </summary>
            public string PermissionsFile;
        }

        /// <summary>
        /// Settings related to heartbeat exchanges between client and server.
        /// </summary>
        public class HeartbeatSettings
        {
            /// <summary>
            /// Enable or disable heartbeats.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// The frequency with which heartbeat messages should be sent to each client, in milliseconds.
            /// </summary>
            public int IntervalMs;

            /// <summary>
            /// The maximum number of heartbeat failures to tolerate before considering the client disconnected.
            /// </summary>
            public int MaxFailures;
        }

        /// <summary>
        /// Settings related to notifications sent by the server.
        /// </summary>
        public class NotificationSettings
        {
            /// <summary>
            /// Enable or disable sending acknowledgements for received messages.
            /// </summary>
            public bool MsgAcknowledgement;

            /// <summary>
            /// Enable or disable sending notifications when a client joins the server.
            /// </summary>
            public bool ServerJoinNotification;

            /// <summary>
            /// Enable or disable sending notifications when a client joins a channel.
            /// </summary>
            public bool ChannelJoinNotification;
        }

        /// <summary>
        /// Settings related to debugging server behavior and log messages.
        /// </summary>
        public class DebugSettings
        {
            /// <summary>
            /// Enable or disable debugging.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// Enable or disable log messages for critical methods involving thread-shared resources.
            /// </summary>
            public bool LockMethodResponseTime;

            /// <summary>
            /// Enable or disable log messages with response time information for messages sent.
            /// </summary>
            public bool MsgResponseTime;

            /// <summary>
            /// Enable or disabling logging to the console.
            /// </summary>
            public bool ConsoleLogging;
        }

        /// <summary>
        /// Settings related to the BigQ TCP server (no SSL).
        /// </summary>
        public class TcpServerSettings
        {
            /// <summary>
            /// Enable or disable the TCP server.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// IP address on which the server should listen.
            /// </summary>
            public string IP;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;
        }

        /// <summary>
        /// Settings related to the BigQ TCP server (with SSL).
        /// </summary>
        public class TcpSSLServerSettings
        {
            /// <summary>
            /// Enable or disable the TCP SSL server.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// IP address on which the server should listen.
            /// </summary>
            public string IP;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;

            /// <summary>
            /// The server certificate (PFX file) the server should use to authenticate itself in SSL connections.
            /// </summary>
            public string PFXCertFile;

            /// <summary>
            /// The password for the server certificate (PFX file).
            /// </summary>
            public string PFXCertPassword;
        }

        /// <summary>
        /// Settings related to the BigQ websocket server (no SSL).
        /// </summary>
        public class WebsocketServerSettings
        {
            /// <summary>
            /// Enable or disable the websocket server.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// The IP address on which the server should listen.
            /// </summary>
            public string IP;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;
        }

        /// <summary>
        /// Settings related to the BigQ websocket server (with SSL).
        /// </summary>
        public class WebsocketSSLServerSettings
        {
            /// <summary>
            /// Enable or disable the websocket SSL server.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// The IP address on which the server should listen.
            /// </summary>
            public string IP;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;

            /// <summary>
            /// The server certificate (PFX file) the server should use to authenticate itself in SSL connections.
            /// </summary>
            public string PFXCertFile;

            /// <summary>
            /// The password for the server certificate (PFX file).
            /// </summary>
            public string PFXCertPassword;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Save the current configuration to the specified file.
        /// </summary>
        /// <param name="file">The file you wish to write.</param>
        public void SaveConfig(string file)
        {
            string fileContents = Helper.SerializeJson(this);
            File.WriteAllBytes(file, Encoding.UTF8.GetBytes(fileContents));
            return;
        }
        
        /// <summary>
        /// Validates the current configuration object to ensure nothing required is missing and no invalid values are supplied.
        /// </summary>
        public void ValidateConfig()
        {
            if (String.IsNullOrEmpty(Version)) throw new ArgumentNullException("Version");
            if (Files == null) throw new ArgumentNullException("Files");
            if (Heartbeat == null) throw new ArgumentNullException("Heartbeat");
            if (Notification == null) throw new ArgumentNullException("Notification");
            if (Debug == null) throw new ArgumentNullException("Debug");
            if (TcpServer == null) throw new ArgumentNullException("TcpServer");
            if (TcpSSLServer == null) throw new ArgumentNullException("TcpSSLServer");
            if (WebsocketServer == null) throw new ArgumentNullException("WebsocketServer");
            if (WebsocketSSLServer == null) throw new ArgumentNullException("WebsocketSSLServer");

            if (Heartbeat.Enable)
            {
                if (Heartbeat.IntervalMs < 1000) throw new ArgumentOutOfRangeException("Heartbeat.IntervalMs");
                if (Heartbeat.MaxFailures < 1) throw new ArgumentOutOfRangeException("Heartbeat.MaxFailures");
            }

            if (TcpServer.Enable)
            {
                if (String.IsNullOrEmpty(TcpServer.IP)) throw new ArgumentNullException("TcpServer.IP");
                if (TcpServer.Port < 1) throw new ArgumentOutOfRangeException("TcpServer.Port");
            }

            if (TcpSSLServer.Enable)
            {
                if (String.IsNullOrEmpty(TcpSSLServer.IP)) throw new ArgumentNullException("TcpSSLServer.IP");
                if (TcpSSLServer.Port < 1) throw new ArgumentOutOfRangeException("TcpSSLServer.Port");
                if (String.IsNullOrEmpty(TcpSSLServer.PFXCertFile)) throw new ArgumentNullException("TcpSSLServer.PFXCertFile");
            }

            if (WebsocketServer.Enable)
            {
                if (String.IsNullOrEmpty(WebsocketServer.IP)) throw new ArgumentNullException("WebsocketServer.IP");
                if (WebsocketServer.Port < 1) throw new ArgumentOutOfRangeException("WebsocketServer.Port");
            }

            if (WebsocketSSLServer.Enable)
            {
                if (String.IsNullOrEmpty(WebsocketSSLServer.IP)) throw new ArgumentNullException("WebsocketSSLServer.IP");
                if (WebsocketSSLServer.Port < 1) throw new ArgumentOutOfRangeException("WebsocketSSLServer.Port");
                if (String.IsNullOrEmpty(TcpSSLServer.PFXCertFile)) throw new ArgumentNullException("WebsocketSSLServer.PFXCertFile");
            }

            if ((TcpServer.Enable ? 1 : 0) +
                (TcpSSLServer.Enable ? 1 : 0) +
                (WebsocketServer.Enable ? 1 : 0) +
                (WebsocketSSLServer.Enable ? 1 : 0)
                < 1)
            {
                throw new Exception("One or more servers must be enabled in the configuration file.");
            }
        }

        #endregion

        #region Public-Static-Methods

        /// <summary>
        /// Loads a configuration object from the filesystem.
        /// </summary>
        /// <param name="file">The file you wish to load.</param>
        /// <returns></returns>
        public static ServerConfiguration LoadConfig(string file)
        {
            byte[] fileBytes = File.ReadAllBytes(file);
            ServerConfiguration ret = Helper.DeserializeJson<ServerConfiguration>(fileBytes, false);
            return ret;
        }

        /// <summary>
        /// Supplies a default, valid server configuration.
        /// </summary>
        /// <returns></returns>
        public static ServerConfiguration DefaultConfig()
        {
            ServerConfiguration ret = new ServerConfiguration();
            ret.Version = System.Reflection.Assembly.GetEntryAssembly().GetName().Version.ToString();
            ret.AcceptInvalidSSLCerts = true;
            ret.GUID = "00000000-0000-0000-0000-000000000000";

            ret.Files = new FilesSettings();
            ret.Files.UsersFile = "";
            ret.Files.PermissionsFile = "";

            ret.Heartbeat = new HeartbeatSettings();
            ret.Heartbeat.Enable = true;
            ret.Heartbeat.IntervalMs = 1000;
            ret.Heartbeat.MaxFailures = 5;

            ret.Notification = new NotificationSettings();
            ret.Notification.MsgAcknowledgement = false;
            ret.Notification.ServerJoinNotification = true;
            ret.Notification.ChannelJoinNotification = true;

            ret.Debug = new DebugSettings();
            ret.Debug.Enable = false;
            ret.Debug.LockMethodResponseTime = false;
            ret.Debug.MsgResponseTime = false;
            ret.Debug.ConsoleLogging = true;

            ret.TcpServer = new TcpServerSettings();
            ret.TcpServer.Enable = true;
            ret.TcpServer.IP = "127.0.0.1";
            ret.TcpServer.Port = 8000;

            ret.TcpSSLServer = new TcpSSLServerSettings();
            ret.TcpSSLServer.Enable = false;
            ret.TcpSSLServer.IP = "127.0.0.1";
            ret.TcpSSLServer.Port = 8001;
            ret.TcpSSLServer.PFXCertFile = "server.crt";
            ret.TcpSSLServer.PFXCertPassword = "password";

            ret.WebsocketServer = new WebsocketServerSettings();
            ret.WebsocketServer.Enable = true;
            ret.WebsocketServer.IP = "127.0.0.1";
            ret.WebsocketServer.Port = 8002;

            ret.WebsocketSSLServer = new WebsocketSSLServerSettings();
            ret.WebsocketSSLServer.Enable = false;
            ret.WebsocketSSLServer.IP = "127.0.0.1";
            ret.WebsocketSSLServer.Port = 8003;
            ret.WebsocketSSLServer.PFXCertFile = "server.crt";
            ret.WebsocketSSLServer.PFXCertPassword = "password";

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            Channel serverChannel = new Channel();
            serverChannel.Broadcast = 1;
            serverChannel.Multicast = 0;
            serverChannel.Unicast = 0;
            serverChannel.ChannelName = "Default server channel";
            serverChannel.CreatedUTC = timestamp;
            serverChannel.UpdatedUTC = timestamp;
            serverChannel.Guid = Guid.NewGuid().ToString();
            serverChannel.OwnerGuid = ret.GUID;
            serverChannel.Private = 0;

            ret.ServerChannels = new List<Channel>();
            ret.ServerChannels.Add(serverChannel);

            return ret;
        }

        #endregion
    }
}
