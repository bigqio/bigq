using BigQ.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BigQ.Server.Managers
{
    /// <summary>
    /// Manages connections associated with BigQ.
    /// </summary>
    internal class ConnectionManager : IDisposable
    {
        #region Public-Members
         
        #endregion

        #region Private-Members

        private bool _Disposed = false;

        private ServerConfiguration _Config;
         
        private readonly object _ClientsLock;
        private Dictionary<string, ServerClient> _Clients;         // IpPort, Client

        private readonly object _DestroyLock;
        private Dictionary<string, DateTime> _DestroyQueue;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary> 
        /// <param name="config">ServerConfiguration instance.</param>
        public ConnectionManager(ServerConfiguration config)
        { 
            if (config == null) throw new ArgumentNullException(nameof(config));
             
            _Config = config;
             
            _ClientsLock = new object();
            _Clients = new Dictionary<string, ServerClient>();

            _DestroyLock = new object();
            _DestroyQueue = new Dictionary<string, DateTime>();

            // clean up dangling connections due to race condition
            Task.Run(() => ProcessDestroyQueue());
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down and dispose of background workers.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Retrieves a list of all ServerClient objects on the server.
        /// </summary>
        /// <returns>A list of ServerClient objects.</returns>
        public List<ServerClient> GetClients()
        {
            lock (_ClientsLock)
            {
                if (_Clients == null || _Clients.Count < 1) return null;

                List<ServerClient> ret = new List<ServerClient>();

                if (_Clients != null && _Clients.Count > 0)
                {
                    foreach (KeyValuePair<string, ServerClient> curr in _Clients) ret.Add(curr.Value);
                }

                return ret;
            }
        }
         
        /// <summary>
        /// Retrieves ServerClient object associated with supplied GUID.
        /// </summary>
        /// <param name="guid">GUID of the client.</param>
        /// <returns>A populated ServerClient object or null.</returns>
        public ServerClient GetClientByGUID(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return null;

            lock (_ClientsLock)
            {
                if (_Clients == null || _Clients.Count < 1) return null;

                ServerClient ret = _Clients.FirstOrDefault(c => c.Value.ClientGUID.ToLower().Equals(guid.ToLower())).Value;
                if (ret == null || ret == default(ServerClient)) return null;
                return ret;
            }
        }

        /// <summary>
        /// Retrieves ServerClient object associated with supplied IP:Port.
        /// </summary>
        /// <param name="ipPort">The IP:port of the client.</param>
        /// <returns>A populated ServerClient object or null.</returns>
        public ServerClient GetByIpPort(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort)) throw new ArgumentNullException(nameof(ipPort));

            lock (_ClientsLock)
            {   
                foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                {
                    ServerClient ret = _Clients.FirstOrDefault(s => s.Value.IpPort.Equals(ipPort)).Value;
                    if (ret == null || ret == default(ServerClient)) return null;
                    return ret;
                }
            }

            return null;
        }

        /// <summary>
        /// Determines whether or not a ServerClient object exists on the server by supplied GUID.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param>
        /// <returns>Boolean indicating whether or not the client exists on the server.</returns>
        public bool ClientExists(string guid)
        {
            lock (_ClientsLock)
            {
                if (_Clients == null || _Clients.Count < 1) return false;

                ServerClient ret = _Clients.FirstOrDefault(s => (!String.IsNullOrEmpty(s.Value.ClientGUID) && (s.Value.ClientGUID.ToLower().Equals(guid.ToLower())))).Value;
                if (ret == null || ret == default(ServerClient)) return false;
                return true;
            }
        }

        /// <summary>
        /// Adds a ServerClient object to the list of clients on the server.
        /// </summary>
        /// <param name="client">A populated ServerClient object.</param>
        public void AddClient(ServerClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
             
            lock (_ClientsLock)
            {
                if (_Clients.ContainsKey(client.IpPort)) _Clients.Remove(client.IpPort);
                _Clients.Add(client.IpPort, client);
            }
             
            return;
        }
         
        /// <summary>
        /// Removes a ServerClient object from the server.
        /// </summary>
        /// <param name="ipPort">The IP:port of the client.</param>
        public void RemoveClient(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort)) return;
            // Console.WriteLine("RemoveClient " + ipPort);

            bool addToQueue = false;

            Dictionary<string, ServerClient> updated = new Dictionary<string, ServerClient>();
            lock (_ClientsLock)
            {
                if (!_Clients.ContainsKey(ipPort))
                {
                    // Console.WriteLine("Not found, adding " + ipPort + " to destroy queue");
                    addToQueue = true;
                }
                else
                {
                    foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                    {
                        if (curr.Key.Equals(ipPort)) continue;
                        updated.Add(curr.Key, curr.Value);
                    }

                    _Clients = updated;
                }
            }
             
            if (addToQueue)
            {
                lock (_DestroyQueue)
                {
                    // Console.WriteLine("*** Adding " + ipPort + " to destroy queue");
                    if (!_DestroyQueue.ContainsKey(ipPort)) _DestroyQueue.Add(ipPort, DateTime.Now.AddSeconds(60));
                }
            }

            return;
        }
         
        /// <summary>
        /// Updates an existing ServerClient object on the server.
        /// </summary>
        /// <param name="client">The ServerClient object.</param>
        public void UpdateClient(ServerClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            Dictionary<string, ServerClient> updated = new Dictionary<string, ServerClient>();
            
            lock (_ClientsLock)
            {
                if (_Clients != null && _Clients.Count > 0)
                {
                    foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                    {
                        if (client.IpPort.Equals(curr.Key)) 
                        {
                            updated.Add(client.IpPort, client);
                            continue;
                        }

                        updated.Add(curr.Key, curr.Value);
                    }

                    _Clients = updated;
                }
                else
                {
                    _Clients.Add(client.IpPort, client);
                }
            }
             
            return;
        }

        #endregion

        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            {
                // do work
            }

            _Disposed = true;
        }

        private void ProcessDestroyQueue()
        {
            while (true)
            {
                Task.Delay(10000).Wait();
                List<string> removalQueue = new List<string>();

                lock (_DestroyQueue)
                {
                    foreach (KeyValuePair<string, DateTime> curr in _DestroyQueue)
                    {
                        // Console.WriteLine("*** Removal of " + curr.Key + " scheduled");
                        removalQueue.Add(curr.Key);
                        if (DateTime.Now > curr.Value) _DestroyQueue.Remove(curr.Key);
                    }
                }

                foreach (string curr in removalQueue)
                {
                    // Console.WriteLine("*** Attempting removal of " + curr);
                    RemoveClient(curr);
                }
            }
        }

        #endregion
    }
}
