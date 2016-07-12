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

        public string Version;
        public string GUID;
        public string Email;
        public string Password;
        public bool AcceptInvalidSSLCerts;
        public int SyncTimeoutMs;
        public HeartbeatSettings Heartbeat;
        public DebugSettings Debug;
        public TcpServerSettings TcpServer;
        public TcpSSLServerSettings TcpSSLServer;

        #endregion

        #region Public-Subordinate-Classes
        
        public class HeartbeatSettings
        {
            public bool Enable;
            public int IntervalMs;
            public int MaxFailures;
        }
        
        public class DebugSettings
        {
            public bool Enable;
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
                if (String.IsNullOrEmpty(TcpSSLServer.P12CertFile)) throw new ArgumentNullException("TcpSSLServer.P12CertFile");
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

        public static ClientConfiguration LoadConfig(string file)
        {
            byte[] fileBytes = File.ReadAllBytes(file);
            ClientConfiguration ret = Helper.DeserializeJson<ClientConfiguration>(fileBytes, false);
            return ret;
        }

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
            ret.TcpSSLServer.P12CertFile = "server.crt";
            ret.TcpSSLServer.P12CertPassword = "password";
            
            return ret;
        }

        #endregion
    }
}
