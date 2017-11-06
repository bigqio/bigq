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
        #region Public-Members

        /// <summary>
        /// The version number of the BigQ binary.
        /// </summary>
        public string Version;

        /// <summary>
        /// The GUID to use for this client.  If null, a random will will be supplied.
        /// </summary>
        public string GUID;

        /// <summary>
        /// The email address to use in authentication.  If null, a random one will be supplied.
        /// </summary>
        public string Email;

        /// <summary>
        /// The client name to appear in sent messages.  If null, a ranodm one will be supplied.
        /// </summary>
        public string Name;

        /// <summary>
        /// The password to use in authentication.
        /// </summary>
        public string Password;

        /// <summary>
        /// The amount of time in milliseconds to wait to receive a response to a synchronous messages sent by this client if not explicitly set in a sent message.
        /// </summary>
        public int SyncTimeoutMs;

        /// <summary>
        /// The GUID associated with the server (optional).
        /// </summary>
        public string ServerGUID;
         
        /// <summary>
        /// Settings related to logging.
        /// </summary>
        public LoggingSettings Logging;

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

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Subordinate-Classes
         
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
            public string Ip;

            /// <summary>
            /// The port number on which the specified server is listening.
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
            /// IP address of the server to which this client should connect. 
            /// </summary>
            public string Ip;

            /// <summary>
            /// The port number on which the specified server is listening.
            /// </summary>
            public int Port;

            /// <summary>
            /// The client certificate (PFX file) the client should use to authenticate itself in SSL connections.
            /// </summary>
            public string PfxCertFile;

            /// <summary>
            /// The password for the client certificate (PFX file).
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
            /// IP address of the server to which this client should connect. 
            /// </summary>
            public string Ip;

            /// <summary>
            /// The port number on which the specified server is listening.
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
            /// IP address of the server to which this client should connect. 
            /// </summary>
            public string Ip;

            /// <summary>
            /// The port number on which the specified server is listening.
            /// </summary>
            public int Port;

            /// <summary>
            /// The client certificate (PFX file) the client should use to authenticate itself in SSL connections.
            /// </summary>
            public string PfxCertFile;

            /// <summary>
            /// The password for the client certificate (PFX file).
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
            if (String.IsNullOrEmpty(Version)) throw new ArgumentException("Version must not be null.");
            if (SyncTimeoutMs < 1000) throw new ArgumentOutOfRangeException("DefaultSyncTimeoutMs must be greater than or equal to 1000."); 
            if (Logging == null) throw new ArgumentException("Logging must not be null.");
            if (TcpServer == null) throw new ArgumentException("TcpServer must not be null.");
            if (TcpSslServer == null) throw new ArgumentException("TcpSslServer must not be null.");
            if (WebsocketServer == null) throw new ArgumentException("WebsocketServer must not be null.");
            if (WebsocketSslServer == null) throw new ArgumentException("WebsocketSslServer must not be null."); 

            if (TcpServer.Enable)
            {
                if (String.IsNullOrEmpty(TcpServer.Ip)) throw new ArgumentException("TcpServer.IP must not be null.");
                if (TcpServer.Port < 1) throw new ArgumentOutOfRangeException("TcpServer.Port must be greater than or equal to 1.");
            }

            if (TcpSslServer.Enable)
            {
                if (String.IsNullOrEmpty(TcpSslServer.Ip)) throw new ArgumentException("TcpSslServer.IP must not be null.");
                if (TcpSslServer.Port < 1) throw new ArgumentOutOfRangeException("TcpSslServer.Port must be greater than or equal to 1.");
                if (String.IsNullOrEmpty(TcpSslServer.PfxCertFile)) throw new ArgumentException("TcpSslServer.PfxCertFile must not be null.");
            }

            if (WebsocketServer.Enable)
            {
                if (String.IsNullOrEmpty(WebsocketServer.Ip)) throw new ArgumentException("WebsocketServer.IP must not be null.");
                if (WebsocketServer.Port < 1) throw new ArgumentOutOfRangeException("WebsocketServer.Port must be greater than or equal to 1.");
            }

            if (WebsocketSslServer.Enable)
            {
                if (String.IsNullOrEmpty(WebsocketSslServer.Ip)) throw new ArgumentException("WebsocketSslServer.IP must not be null.");
                if (WebsocketSslServer.Port < 1) throw new ArgumentOutOfRangeException("WebsocketSslServer.Port must be greater than or equal to 1.");
                if (String.IsNullOrEmpty(WebsocketSslServer.PfxCertFile)) throw new ArgumentException("WebsocketSslServer.PfxCertFile must not be null.");
            }

            if ((TcpServer.Enable ? 1 : 0) +
                (TcpSslServer.Enable ? 1 : 0) +
                (WebsocketServer.Enable ? 1 : 0) +
                (WebsocketSslServer.Enable ? 1 : 0)
                != 1)
            {
                throw new ArgumentException("Exactly one server must be enabled in the configuration file.");
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
            ClientConfiguration ret = Helper.DeserializeJson<ClientConfiguration>(fileBytes);
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
            ret.Name = RandomName();
            ret.Email = Guid.NewGuid().ToString();
            ret.Password = ret.GUID;
            ret.SyncTimeoutMs = 10000;
            ret.ServerGUID = "00000000-0000-0000-0000-000000000000";
             
            ret.Logging = new LoggingSettings();
            ret.Logging.ConsoleLogging = true;
            ret.Logging.SyslogLogging = true;
            ret.Logging.SyslogServerIp = "127.0.0.1";
            ret.Logging.SyslogServerPort = 514;
            ret.Logging.MinimumSeverity = 1;

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

            return ret;
        }

        #endregion

        #region Private-Static-Methods
         
        private static string RandomName()
        {
            Random _Random = new Random((int)DateTime.Now.Ticks);

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

            int selected = _Random.Next(0, names.Length - 1);
            return names[selected];
        }

        #endregion
    }
}
