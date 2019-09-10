using System;
using System.Collections.Concurrent;
using System.Threading;
using Newtonsoft.Json;

namespace BigQ.Server.Classes
{
    /// <summary>
    /// Client metadata object used by the server.
    /// </summary>
    [Serializable]
    public class ServerClient : IDisposable
    {
        #region Public-Members
         
        /// <summary>
        /// The email address associated with the client.
        /// </summary>
        public string Email { get; set; }

        /// <summary>
        /// The password associated with the client.
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// The GUID associated with the client.
        /// </summary>
        public string ClientGUID { get; set; }

        /// <summary>
        /// The name associated with the client.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The GUID associated with the server.  
        /// </summary>
        public string ServerGUID { get; set; }

        /// <summary>
        /// The client's source IP address and port (i.e. 10.1.1.1:5033).
        /// </summary>
        public string IpPort { get; set; }

        /// <summary>
        /// The type of connection.
        /// </summary>
        public ConnectionType Connection { get; set; }

        /// <summary>
        /// The UTC timestamp of when this client object was created.
        /// </summary>
        public DateTime CreatedUtc { get; set; }

        /// <summary>
        /// Indicates whether or not the client is connected to the server.
        /// </summary>
        public bool Connected { get; set; }

        /// <summary>
        /// Indicates whether or not the client is logged in to the server.
        /// </summary>
        public bool LoggedIn { get; set; }

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
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down the client and dispose of background workers.
        /// </summary>
        public void Dispose()
        {
            if (RamQueueTokenSource != null)
            {
                if (!RamQueueTokenSource.IsCancellationRequested) RamQueueTokenSource.Cancel();
                RamQueueTokenSource.Dispose();
                RamQueueTokenSource = null;
            }

            if (DiskQueueTokenSource != null)
            {
                if (!DiskQueueTokenSource.IsCancellationRequested) DiskQueueTokenSource.Cancel();
                DiskQueueTokenSource.Dispose();
                DiskQueueTokenSource = null;
            }

            if (MessageQueue != null)
            {
                MessageQueue.Dispose();
                MessageQueue = null;
            }
        }

        #endregion

        #region Private-Methods
         
        #endregion
    }
}
