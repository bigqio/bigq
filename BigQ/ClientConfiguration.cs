using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Object containing configuration for a BigQ client instance.
    /// </summary>
    [Serializable]
    public class ClientConfiguration
    {
        #region Public-Class-Members

        /// <summary>
        /// The version number of the BigQ binary.
        /// </summary>
        public string Version;

        /// <summary>
        /// The version number of the BigQ binary.
        /// </summary>
        /// public string Version;
        public string GUID;

        /// <summary>
        /// The email address to use in authentication.  If null, a random one will be supplied.
        /// </summary>
        public string Email;

        /// <summary>
        /// The password to use in authentication.
        /// </summary>
        public string Password;

        /// <summary>
        /// Whether or not the BigQ client should accept SSL certificates that are self-signed or unable to be verified.
        /// </summary>
        public bool AcceptInvalidSSLCerts;

        /// <summary>
        /// The amount of time in milliseconds to wait to receive a response to a synchronous messages sent by this client.
        /// </summary>
        public int SyncTimeoutMs;

        /// <summary>
        /// Settings related to heartbeat exchanges between client and server.
        /// </summary>
        public HeartbeatSettings Heartbeat;

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

        #endregion

        #region Public-Subordinate-Classes

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
            /// The frequency with which heartbeat messages should be sent to the server, in milliseconds.
            /// </summary>
            public int IntervalMs;

            /// <summary>
            /// The maximum number of heartbeat failures to tolerate before considering the server disconnected.
            /// </summary>
            public int MaxFailures;
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
            /// IP address of the server to which this client should connect. 
            /// </summary>
            public string IP;

            /// <summary>
            /// The port number on which the specified server is listening.
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
            /// IP address of the server to which this client should connect. 
            /// </summary>
            public string IP;

            /// <summary>
            /// The port number on which the specified server is listening.
            /// </summary>
            public int Port;

            /// <summary>
            /// The client certificate (PFX file) the client should use to authenticate itself in SSL connections.
            /// </summary>
            public string PFXCertFile;

            /// <summary>
            /// The password for the client certificate (PFX file).
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
            if (SyncTimeoutMs < 1000) throw new ArgumentOutOfRangeException("SyncTimeoutMs");
            if (Heartbeat == null) throw new ArgumentNullException("Heartbeat");
            if (Debug == null) throw new ArgumentNullException("Debug");
            if (TcpServer == null) throw new ArgumentNullException("TcpServer");
            if (TcpSSLServer == null) throw new ArgumentNullException("TcpSSLServer");

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
            
            if ((TcpServer.Enable ? 1 : 0) +
                (TcpSSLServer.Enable ? 1 : 0)
                != 1)
            {
                throw new Exception("Exactly one server must be enabled in the configuration file.");
            }
        }

        #endregion

        #region Public-Static-Methods

        /// <summary>
        /// Loads a configuration object from the filesystem.
        /// </summary>
        /// <param name="file">The file you wish to load.</param>
        /// <returns></returns>
        public static ClientConfiguration LoadConfig(string file)
        {
            byte[] fileBytes = File.ReadAllBytes(file);
            ClientConfiguration ret = Helper.DeserializeJson<ClientConfiguration>(fileBytes, false);
            return ret;
        }

        /// <summary>
        /// Supplies a default, valid client configuration.
        /// </summary>
        /// <returns></returns>
        public static ClientConfiguration DefaultConfig()
        {
            ClientConfiguration ret = new ClientConfiguration();
            ret.Version = System.Reflection.Assembly.GetEntryAssembly().GetName().Version.ToString();
            ret.GUID = Guid.NewGuid().ToString();
            ret.Email = ret.GUID;
            ret.Password = ret.GUID;
            ret.SyncTimeoutMs = 10000;

            ret.Heartbeat = new HeartbeatSettings();
            ret.Heartbeat.Enable = false;
            ret.Heartbeat.IntervalMs = 1000;
            ret.Heartbeat.MaxFailures = 5;
            
            ret.Debug = new DebugSettings();
            ret.Debug.Enable = false;
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
            
            return ret;
        }

        #endregion
    }
}
