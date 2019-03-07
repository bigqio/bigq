using BigQ.Core;
using SyslogLogging;
using System;
using System.Collections.Generic;

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
        private LoggingModule _Logging;

        private readonly object _ClientsLock;
        private Dictionary<string, ServerClient> _Clients;         // IpPort, Client
        
        private readonly object _ClientGUIDMapLock;
        private Dictionary<string, string> _ClientGUIDMap;   // GUID, IpPort

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="logging">LoggingModule instance.</param>
        /// <param name="config">ServerConfiguration instance.</param>
        public ConnectionManager(LoggingModule logging, ServerConfiguration config)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));

            _Logging = logging;
            _Config = config;
            _ClientsLock = new object();
            _Clients = new Dictionary<string, ServerClient>();
            _ClientGUIDMapLock = new object();
            _ClientGUIDMap = new Dictionary<string, string>();
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
                if (_Clients == null || _Clients.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "GetClients no clients found");
                    return null;
                }

                List<ServerClient> ret = new List<ServerClient>();

                if (_Clients != null && _Clients.Count > 0)
                {
                    foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                    {
                        ret.Add(curr.Value);
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetClients returning " + ret.Count + " clients");
                return ret;
            }
        }

        /// <summary>
        /// Retrieves a list of all client GUID maps on the server.
        /// </summary>
        /// <returns>Dictionary mapping GUID to IP:port strings.</returns>
        public Dictionary<string, string> GetGUIDMaps()
        {
            lock (_ClientGUIDMapLock)
            {
                if (_ClientGUIDMap == null || _ClientGUIDMap.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "GetGUIDMaps no GUID maps found");
                    return null;
                }
                
                Dictionary<string, string> ret = new Dictionary<string, string>();

                if (_ClientGUIDMap != null && _ClientGUIDMap.Count > 0)
                {
                    foreach (KeyValuePair<string, string> curr in _ClientGUIDMap)
                    {
                        ret.Add(curr.Key, curr.Value);
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetGUIDMaps returning " + ret.Count + " GUID maps");
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
            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID null GUID supplied");
                return null;
            }

            lock (_ClientsLock)
            {
                if (_Clients == null || _Clients.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID no clients found");
                    return null;
                }

                foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                {
                    if (curr.Value.ClientGUID.Equals(guid)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID returning client for GUID " + guid);
                        return curr.Value;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID unable to find client with GUID " + guid);
                return null;
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
                if (_Clients == null || _Clients.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "GetByIpPort no clients found");
                    return null;
                }
                
                foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                {
                    if (curr.Key.Equals(ipPort)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "GetByIpPort returning client for IP:port " + ipPort);
                        return curr.Value;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetByIpPort unable to find client with IP:port " + ipPort);
                return null;
            }
        }

        /// <summary>
        /// Determines whether or not a ServerClient object exists on the server by supplied GUID.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param>
        /// <returns>Boolean indicating whether or not the client exists on the server.</returns>
        public bool ClientExists(string guid)
        {
            lock (_ClientGUIDMap)
            {
                if (_ClientGUIDMap == null || _ClientGUIDMap.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "ClientExists no GUID maps exist");
                    return false;
                }

                foreach (KeyValuePair<string, string> curr in _ClientGUIDMap)
                {
                    if (curr.Key.Equals(guid)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "ClientExists client exists with GUID " + guid);
                        return true;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "ClientExists unable to find client with GUID " + guid);
                return false;
            }
        }

        /// <summary>
        /// Adds a ServerClient object to the list of clients on the server.
        /// </summary>
        /// <param name="client">A populated ServerClient object.</param>
        public void AddClient(ServerClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));

            bool found = false;
            bool replace = false;

            lock (_ClientsLock)
            {
                foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                {
                    if (curr.Key.Equals(client.IpPort)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "AddClient found existing entry for client IP:port " + client.IpPort);
                        replace = true;
                        break;
                    }
                }

                if (replace)
                {
                    if (_Clients.ContainsKey(client.IpPort)) _Clients.Remove(client.IpPort);
                    _Clients.Add(client.IpPort, client);
                }

                if (!found)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "AddClient adding client IP:port " + client.IpPort);
                    _Clients.Add(client.IpPort, client);
                }
            }

            if (!String.IsNullOrEmpty(client.ClientGUID))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "AddClient client has GUID, updating GUID map");
                RemoveGUIDMap(client.IpPort);
                AddGUIDMap(client);
            }

            return;
        }

        /// <summary>
        /// Adds a GUID map for a ServerClient object.
        /// </summary>
        /// <param name="client">The ServerClient object.</param>
        public void AddGUIDMap(ServerClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (String.IsNullOrEmpty(client.ClientGUID)) throw new ArgumentNullException(nameof(client.ClientGUID));
             
            lock (_ClientGUIDMapLock)
            {
                Dictionary<string, string> updated = new Dictionary<string, string>();

                if (_ClientGUIDMap != null && _ClientGUIDMap.Count > 0)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap starting with " + _ClientGUIDMap.Count + " entry(s)");

                    foreach (KeyValuePair<string, string> curr in _ClientGUIDMap)
                    {
                        if (curr.Key.Equals(client.ClientGUID)) 
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap map exists already for GUID " + client.ClientGUID + ", replacing");
                            continue;
                        }

                        updated.Add(curr.Key, curr.Value);
                    }
                }

                updated.Add(client.ClientGUID, client.IpPort);
                _ClientGUIDMap = updated;
            }

            _Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap exiting with " + _ClientGUIDMap.Count + " entry(s)");
        }

        /// <summary>
        /// Removes a ServerClient object from the server.
        /// </summary>
        /// <param name="ipPort">The IP:port of the client.</param>
        public void RemoveClient(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "RemoveClient null IP:port supplied");
                return;
            }

            Dictionary<string, ServerClient> updated = new Dictionary<string, ServerClient>();
            lock (_ClientsLock)
            {
                foreach (KeyValuePair<string, ServerClient> curr in _Clients)
                {
                    if (curr.Key.Equals(ipPort)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "RemoveClient map exists already for IP:port " + ipPort + ", skipping to remove");
                        continue;
                    }

                    updated.Add(curr.Key, curr.Value);
                }

                _Clients = updated;
                RemoveGUIDMap(ipPort);
                return;
            }
        }

        /// <summary>
        /// Removes a client GUID map from the server.
        /// </summary>
        /// <param name="ipPort">The IP:Port of the client.</param>
        public void RemoveGUIDMap(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort)) throw new ArgumentNullException(nameof(ipPort));

            Dictionary<string, string> updated = new Dictionary<string, string>();

            lock (_ClientGUIDMapLock)
            {
                foreach (KeyValuePair<string, string> curr in _ClientGUIDMap)
                {
                    if (curr.Value.Equals(ipPort)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "RemoveGUIDMap found map for IP:port " + ipPort + ", skipping to remove");
                        continue;
                    }

                    updated.Add(curr.Key, curr.Value);
                }

                _ClientGUIDMap = updated;
                return;
            }
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
                            _Logging.Log(LoggingModule.Severity.Debug, "UpdateClient found client to update on IP:port " + client.IpPort);
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

                if (!String.IsNullOrEmpty(client.ClientGUID))
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "UpdateClient found GUID in client, updating GUID map for IP:port " + client.IpPort);
                    RemoveGUIDMap(client.IpPort);
                    AddGUIDMap(client);
                }
                else
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "UpdateClient no GUID in client IP:port " + client.IpPort);
                }

                return;
            }
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

        #endregion
    }
}
