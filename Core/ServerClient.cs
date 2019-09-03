using System;
using System.Collections.Concurrent;
using System.Threading;
using Newtonsoft.Json;

namespace BigQ.Core
{
    /// <summary>
    /// Client metadata object used by the server.
    /// </summary>
    [Serializable]
    public class ServerClient : IDisposable
    {
        #region Public-Members
         
        /// <summary>
        /// The email address associated with the client.  Do not modify directly; used by the server.  
        /// </summary>
        public string Email;

        /// <summary>
        /// The password associated with the client.  Do not modify directly; used by the server.  
        /// </summary>
        public string Password;

        /// <summary>
        /// The GUID associated with the client.  Do not modify directly; used by the server.  
        /// </summary>
        public string ClientGUID;

        /// <summary>
        /// The name associated with the client.
        /// </summary>
        public string Name;

        /// <summary>
        /// The GUID associated with the server.  
        /// </summary>
        public string ServerGuid;

        /// <summary>
        /// The client's source IP address and port (i.e. 10.1.1.1:5033).  Do not modify directly; used by the server.  
        /// </summary>
        public string IpPort;

        /// <summary>
        /// The type of connection.
        /// </summary>
        public ConnectionType Connection;

        /// <summary>
        /// The UTC timestamp of when this client object was created.
        /// </summary>
        public DateTime CreatedUtc;

        /// <summary>
        /// The UTC timestamp of when this client object was last updated.
        /// </summary>
        public DateTime? UpdatedUtc;
         
        /// <summary>
        /// Indicates whether or not the client is connected to the server.  Do not modify this field.
        /// </summary>
        public bool Connected;

        /// <summary>
        /// Indicates whether or not the client is logged in to the server.  Do not modify this field.
        /// </summary>
        public bool LoggedIn;

        /// <summary>
        /// A blocking collection containing the messages that are queued for delivery to this client.
        /// </summary>
        [JsonIgnore]
        public BlockingCollection<Message> MessageQueue = new BlockingCollection<Message>();

        /// <summary>
        /// Managed by the server to destroy the thread processing the client queue when the client is shutting down.
        /// </summary>
        [JsonIgnore]
        public CancellationTokenSource RamQueueTokenSource = null;

        /// <summary>
        /// Managed by the server to destroy the thread processing the client queue when the client is shutting down.
        /// </summary>
        [JsonIgnore]
        public CancellationToken RamQueueToken;

        /// <summary>
        /// Managed by the server to destroy the thread processing the client queue when the client is shutting down.
        /// </summary>
        [JsonIgnore]
        public CancellationTokenSource DiskQueueTokenSource = null;

        /// <summary>
        /// Managed by the server to destroy the thread processing the client queue when the client is shutting down.
        /// </summary>
        [JsonIgnore]
        public CancellationToken DiskQueueToken;
         
        #endregion

        #region Private-Members
         
        #endregion
         
        #region Constructors-and-Factories

        /// <summary>
        /// This constructor is used by BigQServer.  Do not use it in client applications!
        /// </summary>
        public ServerClient()
        {
        }
          
        /// <summary>
        /// Used by the server, do not use.
        /// </summary>
        /// <param name="ipPort">The IP:port of the client.</param>
        /// <param name="connType">Type of connection.</param>
        public ServerClient(string ipPort, ConnectionType connType)
        {
            if (String.IsNullOrEmpty(ipPort)) throw new ArgumentNullException(nameof(ipPort));

            IpPort = ipPort;
            Connection = connType;

            MessageQueue = new BlockingCollection<Message>();

            DateTime ts = DateTime.Now.ToUniversalTime();
            CreatedUtc = ts;
            UpdatedUtc = ts;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down the client and dispose of background workers.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }
         
        #endregion
          
        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        { 
            if (disposing)
            { 
                if (RamQueueTokenSource != null)
                {
                    RamQueueTokenSource.Cancel(true);
                }

                if (DiskQueueTokenSource != null)
                { 
                    DiskQueueTokenSource.Cancel(true);
                }

                return;
            }
        }
          
        #endregion
    }
}
