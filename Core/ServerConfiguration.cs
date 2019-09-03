using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BigQ.Core
{
    /// <summary>
    /// Object containing configuration for a BigQ server instance.
    /// </summary>
    [Serializable]
    public class ServerConfiguration
    {
        #region Public-Members
         
        /// <summary>
        /// Optional, overrides the GUID used by the server.  By default, the server uses 00000000-0000-0000-0000-000000000000.
        /// </summary>
        public string GUID;

        /// <summary>
        /// Settings related to files referenced by BigQ.
        /// </summary>
        public FilesSettings Files;

        /// <summary>
        /// Settings related to message persistence.
        /// </summary>
        public PersistenceSettings Persistence;
          
        /// <summary>
        /// Settings related to notifications sent by the server.
        /// </summary>
        public NotificationSettings Notification;
         
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

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

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

            /// <summary>
            /// The Sqlite database file containing persistent messages.
            /// </summary>
            public string PersistenceDatabaseFile;
        }

        /// <summary>
        /// Settings related to message persistence.
        /// </summary>
        public class PersistenceSettings
        {
            /// <summary>
            /// Enable (true) or disable (false) persistence.
            /// </summary>
            public bool EnablePersistence;

            /// <summary>
            /// Enable (true) or disable (false) database queries and results.
            /// </summary>
            public bool DebugDatabase;

            /// <summary>
            /// The maximum number of seconds a persistent message can be stored.
            /// </summary>
            public int MaxExpirationSeconds;
             
            /// <summary>
            /// The number of milliseconds to wait between iterations removing expired messages.
            /// </summary>
            public int ExpirationIntervalMs;

            /// <summary>
            /// The number of milliseconds to wait between checks for pending persistent messages to deliver.
            /// </summary>
            public int RefreshIntervalMs;
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
            string fileContents = Common.SerializeJson(this);
            File.WriteAllBytes(file, Encoding.UTF8.GetBytes(fileContents));
            return;
        }
        
        /// <summary>
        /// Validates the current configuration object to ensure nothing required is missing and no invalid values are supplied.
        /// </summary>
        public void ValidateConfig()
        { 
            if (Files == null) throw new ArgumentException("Files section must not be null.");
            if (Persistence == null) throw new ArgumentException("Persistence section must not be null.");  
            if (Notification == null) throw new ArgumentException("Notification section must not be null."); 
            if (TcpServer == null) throw new ArgumentException("TcpServer section must not be null.");
            if (TcpSslServer == null) throw new ArgumentException("TcpSslServer section must not be null.");
            if (WebsocketServer == null) throw new ArgumentException("WebsocketServer section must not be null.");
            if (WebsocketSslServer == null) throw new ArgumentException("WebsocketSslServer section must not be null."); 

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
         
        /// <summary>
        /// Loads a configuration object from the filesystem.
        /// </summary>
        /// <param name="file">The file you wish to load.</param>
        /// <returns></returns>
        public static ServerConfiguration LoadConfig(string file)
        {
            byte[] fileBytes = File.ReadAllBytes(file);
            ServerConfiguration ret = Common.DeserializeJson<ServerConfiguration>(fileBytes);
            return ret;
        }

        /// <summary>
        /// Supplies a default, valid server configuration.
        /// </summary>
        /// <returns></returns>
        public static ServerConfiguration Default()
        {
            ServerConfiguration ret = new ServerConfiguration(); 
            ret.GUID = "00000000-0000-0000-0000-000000000000";

            ret.Files = new FilesSettings();
            ret.Files.UsersFile = "";
            ret.Files.PermissionsFile = "";
            ret.Files.PersistenceDatabaseFile = "PersistenceQueue.db";

            ret.Persistence = new PersistenceSettings();
            ret.Persistence.EnablePersistence = true;
            ret.Persistence.DebugDatabase = false;
            ret.Persistence.MaxExpirationSeconds = 30; 
            ret.Persistence.RefreshIntervalMs = 5000;
            ret.Persistence.ExpirationIntervalMs = 15000;
              
            ret.Notification = new NotificationSettings();
            ret.Notification.MsgAcknowledgement = false;
            ret.Notification.ServerJoinNotification = true;
            ret.Notification.ChannelJoinNotification = true;
             
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
            serverChannel.ChannelName = "Default server channel";
            serverChannel.CreatedUtc = timestamp; 
            serverChannel.ChannelGUID = Guid.NewGuid().ToString();
            serverChannel.OwnerGUID = ret.GUID;
            serverChannel.Type = ChannelType.Broadcast;
            serverChannel.Visibility = ChannelVisibility.Public;

            ret.ServerChannels = new List<Channel>();
            ret.ServerChannels.Add(serverChannel);

            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
