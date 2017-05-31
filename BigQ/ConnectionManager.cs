using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyslogLogging;

namespace BigQ
{
    /// <summary>
    /// Manages connections associated with BigQ.
    /// </summary>
    public class ConnectionManager
    {
        #region Public-Members

        public bool Debug;

        #endregion

        #region Private-Members

        private ServerConfiguration Config;
        private LoggingModule Logging;

        private readonly object ClientsLock;
        private Dictionary<string, Client> Clients;         // IpPort, Client
        
        private readonly object ClientGUIDMapLock;
        private Dictionary<string, string> ClientGUIDMap;   // GUID, IpPort

        #endregion

        #region Constructors

        public ConnectionManager(LoggingModule logging, ServerConfiguration config)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));

            Logging = logging;
            Config = config;
            ClientsLock = new object();
            Clients = new Dictionary<string, Client>();
            ClientGUIDMapLock = new object();
            ClientGUIDMap = new Dictionary<string, string>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Retrieves a list of all clients on the server.
        /// </summary>
        /// <returns>A list of Client objects.</returns>
        public List<Client> GetClients()
        {
            lock (ClientsLock)
            {
                if (Clients == null || Clients.Count < 1)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "GetClients no clients found");
                    return null;
                }

                List<Client> ret = new List<Client>();

                if (Clients != null && Clients.Count > 0)
                {
                    foreach (KeyValuePair<string, Client> curr in Clients)
                    {
                        ret.Add(curr.Value);
                    }
                }

                Logging.Log(LoggingModule.Severity.Debug, "GetClients returning " + ret.Count + " clients");
                return ret;
            }
        }

        /// <summary>
        /// Retrieves a list of all client GUID maps on the server.
        /// </summary>
        /// <returns>A list of Client objects.</returns>
        public Dictionary<string, string> GetGUIDMaps()
        {
            lock (ClientGUIDMapLock)
            {
                if (ClientGUIDMap == null || ClientGUIDMap.Count < 1)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "GetGUIDMaps no GUID maps found");
                    return null;
                }
                
                Dictionary<string, string> ret = new Dictionary<string, string>();

                if (ClientGUIDMap != null && ClientGUIDMap.Count > 0)
                {
                    foreach (KeyValuePair<string, string> curr in ClientGUIDMap)
                    {
                        ret.Add(curr.Key, curr.Value);
                    }
                }

                Logging.Log(LoggingModule.Severity.Debug, "GetGUIDMaps returning " + ret.Count + " GUID maps");
                return ret;
            }
        }

        /// <summary>
        /// Retrieves Client object associated with supplied GUID.
        /// </summary>
        /// <param name="guid">GUID of the client.</param>
        /// <returns>A populated Client object or null.</returns>
        public Client GetClientByGUID(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID null GUID supplied");
                return null;
            }

            lock (ClientsLock)
            {
                if (Clients == null || Clients.Count < 1)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID no clients found");
                    return null;
                }

                foreach (KeyValuePair<string, Client> curr in Clients)
                {
                    if (String.Compare(curr.Value.ClientGUID, guid) == 0)
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID returning client for GUID " + guid);
                        return curr.Value;
                    }
                }

                Logging.Log(LoggingModule.Severity.Debug, "GetClientByGUID unable to find client with GUID " + guid);
                return null;
            }
        }

        /// <summary>
        /// Retrieves Client object associated with supplied IP:Port.
        /// </summary>
        /// <param name="ipPort">The IP:port of the client.</param>
        /// <returns>A populated Client object or null.</returns>
        public Client GetByIpPort(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort))
            {
                Logging.Log(LoggingModule.Severity.Debug, "GetByIpPort unable to find client with IP:port " + ipPort);
                return null;
            }

            lock (ClientsLock)
            {
                if (Clients == null || Clients.Count < 1)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "GetByIpPort no clients found");
                    return null;
                }
                
                foreach (KeyValuePair<string, Client> curr in Clients)
                {
                    if (String.Compare(curr.Key, ipPort) == 0)
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "GetByIpPort returning client for IP:port " + ipPort);
                        return curr.Value;
                    }
                }

                Logging.Log(LoggingModule.Severity.Debug, "GetByIpPort unable to find client with IP:port " + ipPort);
                return null;
            }
        }

        /// <summary>
        /// Determines whether or not a client exists on the server by supplied GUID.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param>
        /// <returns>Boolean indicating whether or not the client exists on the server.</returns>
        public bool ClientExists(string guid)
        {
            lock (ClientGUIDMap)
            {
                if (ClientGUIDMap == null || ClientGUIDMap.Count < 1)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "ClientExists no GUID maps exist");
                    return false;
                }

                foreach (KeyValuePair<string, string> curr in ClientGUIDMap)
                {
                    if (String.Compare(curr.Key, guid) == 0)
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "ClientExists client exists with GUID " + guid);
                        return true;
                    }
                }

                Logging.Log(LoggingModule.Severity.Debug, "ClientExists unable to find client with GUID " + guid);
                return false;
            }
        }
        
        /// <summary>
        /// Adds a Client object to the list of clients on the server.
        /// </summary>
        /// <param name="currClient">A populated Client object or null.</param>
        public void AddClient(Client currClient)
        {
            if (currClient == null)
            {
                Logging.Log(LoggingModule.Severity.Debug, "AddClient null client supplied");
                return;
            }

            bool found = false;
            bool replace = false;

            lock (ClientsLock)
            {
                foreach (KeyValuePair<string, Client> curr in Clients)
                {
                    if (String.Compare(curr.Key, currClient.IpPort) == 0)
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "AddClient found existing entry for client IP:port " + currClient.IpPort);
                        replace = true;
                        break;
                    }
                }

                if (replace)
                {
                    if (Clients.ContainsKey(currClient.IpPort)) Clients.Remove(currClient.IpPort);
                    Clients.Add(currClient.IpPort, currClient);
                }

                if (!found)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "AddClient adding client IP:port " + currClient.IpPort);
                    Clients.Add(currClient.IpPort, currClient);
                }
            }

            if (!String.IsNullOrEmpty(currClient.ClientGUID))
            {
                Logging.Log(LoggingModule.Severity.Debug, "AddClient client has GUID, updating GUID map");
                RemoveGUIDMap(currClient.IpPort);
                AddGUIDMap(currClient);
            }

            return;
        }

        /// <summary>
        /// Adds a GUID map for a client.
        /// </summary>
        /// <param name="currClient">The Client object.</param>
        public void AddGUIDMap(Client currClient)
        {
            if (currClient == null)
            {
                Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap null client supplied");
                return;
            }

            if (String.IsNullOrEmpty(currClient.ClientGUID))
            {
                Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap client has a null GUID");
                return;
            }
             
            lock (ClientGUIDMapLock)
            {
                Dictionary<string, string> updated = new Dictionary<string, string>();

                if (ClientGUIDMap != null && ClientGUIDMap.Count > 0)
                {
                    Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap starting with " + ClientGUIDMap.Count + " entry(s)");

                    foreach (KeyValuePair<string, string> curr in ClientGUIDMap)
                    {
                        if (String.Compare(curr.Key, currClient.ClientGUID) == 0)
                        {
                            Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap map exists already for GUID " + currClient.ClientGUID + ", replacing");
                            continue;
                        }

                        updated.Add(curr.Key, curr.Value);
                    }
                }

                updated.Add(currClient.ClientGUID, currClient.IpPort);
                ClientGUIDMap = updated;
            }

            Logging.Log(LoggingModule.Severity.Debug, "AddGUIDMap exiting with " + ClientGUIDMap.Count + " entry(s)");
        }

        /// <summary>
        /// Removes a Client object from the server.
        /// </summary>
        /// <param name="ipPort">The IP:port of the client.</param>
        public void RemoveClient(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort))
            {
                Logging.Log(LoggingModule.Severity.Debug, "RemoveClient null IP:port supplied");
                return;
            }

            Dictionary<string, Client> updated = new Dictionary<string, Client>();
            lock (ClientsLock)
            {
                foreach (KeyValuePair<string, Client> curr in Clients)
                {
                    if (String.Compare(curr.Key, ipPort) == 0)
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "RemoveClient map exists already for IP:port " + ipPort + ", skipping to remove");
                        continue;
                    }

                    updated.Add(curr.Key, curr.Value);
                }

                Clients = updated;
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
            if (String.IsNullOrEmpty(ipPort))
            {
                Logging.Log(LoggingModule.Severity.Debug, "RemoveGUIDMap null IP:port supplied");
                return;
            }

            Dictionary<string, string> updated = new Dictionary<string, string>();

            lock (ClientGUIDMapLock)
            {
                foreach (KeyValuePair<string, string> curr in ClientGUIDMap)
                {
                    if (String.Compare(curr.Value, ipPort) == 0)
                    {
                        Logging.Log(LoggingModule.Severity.Debug, "RemoveGUIDMap found map for IP:port " + ipPort + ", skipping to remove");
                        continue;
                    }

                    updated.Add(curr.Key, curr.Value);
                }

                ClientGUIDMap = updated;
                return;
            }
        }

        /// <summary>
        /// Updates an existing Client object on the server.
        /// </summary>
        /// <param name="currClient">The Client object.</param>
        public void UpdateClient(Client currClient)
        {
            if (currClient == null)
            {
                Logging.Log(LoggingModule.Severity.Debug, "UpdateClient null client supplied");
                return;
            }

            Dictionary<string, Client> updated = new Dictionary<string, Client>();
            
            lock (ClientsLock)
            {
                if (Clients != null && Clients.Count > 0)
                {
                    foreach (KeyValuePair<string, Client> curr in Clients)
                    {
                        if (String.Compare(currClient.IpPort, curr.Key) == 0)
                        {
                            Logging.Log(LoggingModule.Severity.Debug, "UpdateClient found client to update on IP:port " + currClient.IpPort);
                            updated.Add(currClient.IpPort, currClient);
                            continue;
                        }

                        updated.Add(curr.Key, curr.Value);
                    }

                    Clients = updated;
                }
                else
                {
                    Clients.Add(currClient.IpPort, currClient);
                }

                if (!String.IsNullOrEmpty(currClient.ClientGUID))
                {
                    Logging.Log(LoggingModule.Severity.Debug, "UpdateClient found GUID in client, updating GUID map for IP:port " + currClient.IpPort);
                    RemoveGUIDMap(currClient.IpPort);
                    AddGUIDMap(currClient);
                }
                else
                {
                    Logging.Log(LoggingModule.Severity.Debug, "UpdateClient no GUID in client IP:port " + currClient.IpPort);
                }

                return;
            }
        }
        
        #endregion

        #region Private-Methods

        #endregion
    }
}
