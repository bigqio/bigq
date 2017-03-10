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
        #region Public-Members

        /// <summary>
        /// The version number of the BigQ binary.
        /// </summary>
        public string Version;
         
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
        /// Settings related to logging.
        /// </summary>
        public LoggingSettings Logging;

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
        public TcpSslServerSettings TcpSslServer;

        /// <summary>
        /// Settings related to the BigQ websocket server (no SSL).
        /// </summary>
        public WebsocketServerSettings WebsocketServer;

        /// <summary>
        /// Settings related to the BigQ websocket server (with SSL).
        /// </summary>
        public WebsocketSslServerSettings WebsocketSslServer;

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
            /// The frequency with which heartbeat messages should be sent to each client, in milliseconds.
            /// </summary>
            public int IntervalMs;

            /// <summary>
            /// The maximum number of heartbeat failures to tolerate before considering the client disconnected.
            /// </summary>
            public int MaxFailures;
        }

        /// <summary>
        /// Settings related to logging.
        /// </summary>
        public class LoggingSettings
        {
            /// <summary>
            /// Enable or disable console logging.
            /// </summary>
            public bool ConsoleLogging;

            /// <summary>
            /// Enable or disable syslog logging.
            /// </summary>
            public bool SyslogLogging;

            /// <summary>
            /// IP address of the syslog server.
            /// </summary>
            public string SyslogServerIp;

            /// <summary>
            /// UDP port of the syslog server.
            /// </summary>
            public int SyslogServerPort;

            /// <summary>
            /// Minimum severity required to send log messages.
            /// </summary>
            public int MinimumSeverity;
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
            /// Enable or disable log messages related to connection management.
            /// </summary>
            public bool ConnectionMgmt;

            /// <summary>
            /// Enable or disable log messages related to channel management.
            /// </summary>
            public bool ChannelMgmt;

            /// <summary>
            /// Enable or disable log messages related to sending heartbeat messages.
            /// </summary>
            public bool SendHeartbeat; 
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
            public string Ip;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;

            /// <summary>
            /// Enable or disable debugging.
            /// </summary>
            public bool Debug;
        }

        /// <summary>
        /// Settings related to the BigQ TCP server (with SSL).
        /// </summary>
        public class TcpSslServerSettings
        {
            /// <summary>
            /// Enable or disable the TCP SSL server.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// IP address on which the server should listen.
            /// </summary>
            public string Ip;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;

            /// <summary>
            /// The server certificate (PFX file) the server should use to authenticate itself in SSL connections.
            /// </summary>
            public string PfxCertFile;

            /// <summary>
            /// The password for the server certificate (PFX file).
            /// </summary>
            public string PfxCertPassword;

            /// <summary>
            /// Indicate whether or not invalid certificates should be accepted.
            /// </summary>
            public bool AcceptInvalidCerts;

            /// <summary>
            /// Enable or disable debugging.
            /// </summary>
            public bool Debug;
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
            public string Ip;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;

            /// <summary>
            /// Enable or disable debugging.
            /// </summary>
            public bool Debug;
        }

        /// <summary>
        /// Settings related to the BigQ websocket server (with SSL).
        /// </summary>
        public class WebsocketSslServerSettings
        {
            /// <summary>
            /// Enable or disable the websocket SSL server.
            /// </summary>
            public bool Enable;

            /// <summary>
            /// The IP address on which the server should listen.
            /// </summary>
            public string Ip;

            /// <summary>
            /// The port number on which the server should listen.
            /// </summary>
            public int Port;

            /// <summary>
            /// The server certificate (PFX file) the server should use to authenticate itself in SSL connections.
            /// </summary>
            public string PfxCertFile;

            /// <summary>
            /// The password for the server certificate (PFX file).
            /// </summary>
            public string PfxCertPassword;

            /// <summary>
            /// Indicate whether or not invalid certificates should be accepted.
            /// </summary>
            public bool AcceptInvalidCerts;

            /// <summary>
            /// Enable or disable debugging.
            /// </summary>
            public bool Debug;
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
            if (String.IsNullOrEmpty(Version)) throw new ArgumentException("Version section must not be null.");
            if (Files == null) throw new ArgumentException("Files section must not be null.");
            if (Heartbeat == null) throw new ArgumentException("Heartbeat section must not be null.");
            if (Logging == null) throw new ArgumentException("Logging section must not be null.");
            if (Notification == null) throw new ArgumentException("Notification section must not be null.");
            if (Debug == null) throw new ArgumentException("Debug section must not be null.");
            if (TcpServer == null) throw new ArgumentException("TcpServer section must not be null.");
            if (TcpSslServer == null) throw new ArgumentException("TcpSslServer section must not be null.");
            if (WebsocketServer == null) throw new ArgumentException("WebsocketServer section must not be null.");
            if (WebsocketSslServer == null) throw new ArgumentException("WebsocketSslServer section must not be null.");
            if (Heartbeat.IntervalMs < 1000) throw new ArgumentOutOfRangeException("Heartbeat.IntervalMs must be at least 1000.");
            if (Heartbeat.MaxFailures < 1) throw new ArgumentOutOfRangeException("Heartbeat.MaxFailures must be greater than or equal to one.");

            if (TcpServer.Enable)
            {
                if (String.IsNullOrEmpty(TcpServer.Ip)) throw new ArgumentException("TcpServer.IP must not be null.");
                if (TcpServer.Port < 1) throw new ArgumentOutOfRangeException("TcpServer.Port must be greater than or equal to one.");
            }

            if (TcpSslServer.Enable)
            {
                if (String.IsNullOrEmpty(TcpSslServer.Ip)) throw new ArgumentException("TcpSslServer.IP must not be null.");
                if (TcpSslServer.Port < 1) throw new ArgumentOutOfRangeException("TcpSslServer.Port must be greater than or equal to one.");
                if (String.IsNullOrEmpty(TcpSslServer.PfxCertFile)) throw new ArgumentException("TcpSslServer.PfxCertFile must not be null.");
            }

            if (WebsocketServer.Enable)
            {
                if (String.IsNullOrEmpty(WebsocketServer.Ip)) throw new ArgumentException("WebsocketServer.IP must not be null.");
                if (WebsocketServer.Port < 1) throw new ArgumentException("WebsocketServer.Port must be greater than or equal to one.");
            }

            if (WebsocketSslServer.Enable)
            {
                if (String.IsNullOrEmpty(WebsocketSslServer.Ip)) throw new ArgumentException("WebsocketSslServer.IP must not be null.");
                if (WebsocketSslServer.Port < 1) throw new ArgumentOutOfRangeException("WebsocketSslServer.Port must be greater than or equal to one.");
                if (String.IsNullOrEmpty(TcpSslServer.PfxCertFile)) throw new ArgumentException("WebsocketSslServer.PfxCertFile must not be null.");
            }

            if ((TcpServer.Enable ? 1 : 0) +
                (TcpSslServer.Enable ? 1 : 0) +
                (WebsocketServer.Enable ? 1 : 0) +
                (WebsocketSslServer.Enable ? 1 : 0)
                < 1)
            {
                throw new ArgumentException("One or more servers must be enabled in the configuration file.");
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
            ServerConfiguration ret = Helper.DeserializeJson<ServerConfiguration>(fileBytes);
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
            ret.GUID = "00000000-0000-0000-0000-000000000000";

            ret.Files = new FilesSettings();
            ret.Files.UsersFile = "";
            ret.Files.PermissionsFile = "";

            ret.Heartbeat = new HeartbeatSettings();
            ret.Heartbeat.IntervalMs = 1000;
            ret.Heartbeat.MaxFailures = 5;

            ret.Logging = new LoggingSettings();
            ret.Logging.ConsoleLogging = true;
            ret.Logging.SyslogLogging = true;
            ret.Logging.SyslogServerIp = "127.0.0.1";
            ret.Logging.SyslogServerPort = 514;
            ret.Logging.MinimumSeverity = 1;

            ret.Notification = new NotificationSettings();
            ret.Notification.MsgAcknowledgement = false;
            ret.Notification.ServerJoinNotification = true;
            ret.Notification.ChannelJoinNotification = true;

            ret.Debug = new DebugSettings();
            ret.Debug.Enable = false;
            ret.Debug.ConnectionMgmt = false;
            ret.Debug.ChannelMgmt = false;
            ret.Debug.SendHeartbeat = false;

            ret.TcpServer = new TcpServerSettings();
            ret.TcpServer.Enable = true;
            ret.TcpServer.Ip = "127.0.0.1";
            ret.TcpServer.Port = 8000;
            ret.TcpServer.Debug = false;

            ret.TcpSslServer = new TcpSslServerSettings();
            ret.TcpSslServer.Enable = false;
            ret.TcpSslServer.Ip = "127.0.0.1";
            ret.TcpSslServer.Port = 8001;
            ret.TcpSslServer.PfxCertFile = "test.pfx";
            ret.TcpSslServer.PfxCertPassword = "password";
            ret.TcpSslServer.AcceptInvalidCerts = true;
            ret.TcpSslServer.Debug = false;

            ret.WebsocketServer = new WebsocketServerSettings();
            ret.WebsocketServer.Enable = false;
            ret.WebsocketServer.Ip = "127.0.0.1";
            ret.WebsocketServer.Port = 8002;
            ret.WebsocketServer.Debug = false;

            ret.WebsocketSslServer = new WebsocketSslServerSettings();
            ret.WebsocketSslServer.Enable = false;
            ret.WebsocketSslServer.Ip = "127.0.0.1";
            ret.WebsocketSslServer.Port = 8003;
            ret.WebsocketSslServer.PfxCertFile = "test.pfx";
            ret.WebsocketSslServer.PfxCertPassword = "password";
            ret.WebsocketSslServer.AcceptInvalidCerts = true;
            ret.WebsocketSslServer.Debug = false;

            DateTime timestamp = DateTime.Now.ToUniversalTime();
            Channel serverChannel = new Channel();
            serverChannel.Broadcast = 1;
            serverChannel.Multicast = 0;
            serverChannel.Unicast = 0;
            serverChannel.ChannelName = "Default server channel";
            serverChannel.CreatedUtc = timestamp;
            serverChannel.UpdatedUtc = timestamp;
            serverChannel.ChannelGUID = Guid.NewGuid().ToString();
            serverChannel.OwnerGUID = ret.GUID;
            serverChannel.Private = 0;

            ret.ServerChannels = new List<Channel>();
            ret.ServerChannels.Add(serverChannel);

            return ret;
        }

        #endregion
    }
}
