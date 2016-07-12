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

        public string Version;
        public bool AcceptInvalidSSLCerts;
        public FilesSettings Files;
        public HeartbeatSettings Heartbeat;
        public NotificationSettings Notification;
        public DebugSettings Debug;
        public TcpServerSettings TcpServer;
        public TcpSSLServerSettings TcpSSLServer;
        public WebsocketServerSettings WebsocketServer;
        public WebsocketSSLServerSettings WebsocketSSLServer;

        #endregion

        #region Public-Subordinate-Classes

        public class FilesSettings
        {
            public string UsersFile;
            public string PermissionsFile;
        }

        public class HeartbeatSettings
        {
            public bool Enable;
            public int IntervalMs;
            public int MaxFailures;
        }

        public class NotificationSettings
        {
            public bool MsgAcknowledgement;
            public bool ServerJoinNotification;
            public bool ChannelJoinNotification;
        }

        public class DebugSettings
        {
            public bool Enable;
            public bool LockMethodResponseTime;
            public bool MsgResponseTime;
            public bool ConsoleLogging;
        }

        public class TcpServerSettings
        {
            public bool Enable;
            public string IP;
            public int Port;
        }

        public class TcpSSLServerSettings
        {
            public bool Enable;
            public string IP;
            public int Port;
            public string P12CertFile;
            public string P12CertPassword;
        }

        public class WebsocketServerSettings
        {
            public bool Enable;
            public string IP;
            public int Port;
        }

        public class WebsocketSSLServerSettings
        {
            public bool Enable;
            public string IP;
            public int Port;
            public string P12CertFile;
            public string P12CertPassword;
        }

        #endregion

        #region Public-Methods

        public void SaveConfig(string file)
        {
            string fileContents = Helper.SerializeJson(this);
            File.WriteAllBytes(file, Encoding.UTF8.GetBytes(fileContents));
            return;
        }
        
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
                if (String.IsNullOrEmpty(TcpSSLServer.P12CertFile)) throw new ArgumentNullException("TcpSSLServer.P12CertFile");
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
                if (String.IsNullOrEmpty(TcpSSLServer.P12CertFile)) throw new ArgumentNullException("WebsocketSSLServer.P12CertFile");
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

        public static ServerConfiguration LoadConfig(string file)
        {
            byte[] fileBytes = File.ReadAllBytes(file);
            ServerConfiguration ret = Helper.DeserializeJson<ServerConfiguration>(fileBytes, false);
            return ret;
        }

        public static ServerConfiguration DefaultConfig()
        {
            ServerConfiguration ret = new ServerConfiguration();
            ret.Version = System.Reflection.Assembly.GetEntryAssembly().GetName().Version.ToString();
            ret.AcceptInvalidSSLCerts = true;

            ret.Files = new FilesSettings();
            ret.Files.UsersFile = "";
            ret.Files.PermissionsFile = "";

            ret.Heartbeat = new HeartbeatSettings();
            ret.Heartbeat.Enable = false;
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
            ret.TcpSSLServer.P12CertFile = "server.crt";
            ret.TcpSSLServer.P12CertPassword = "password";

            ret.WebsocketServer = new WebsocketServerSettings();
            ret.WebsocketServer.Enable = true;
            ret.WebsocketServer.IP = "127.0.0.1";
            ret.WebsocketServer.Port = 8002;

            ret.WebsocketSSLServer = new WebsocketSSLServerSettings();
            ret.WebsocketSSLServer.Enable = false;
            ret.WebsocketSSLServer.IP = "127.0.0.1";
            ret.WebsocketSSLServer.Port = 8003;
            ret.WebsocketSSLServer.P12CertFile = "server.crt";
            ret.WebsocketSSLServer.P12CertPassword = "password";

            return ret;
        }

        #endregion
    }
}
