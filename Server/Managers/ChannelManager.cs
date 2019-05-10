using BigQ.Core;
using SyslogLogging;
using System;
using System.Collections.Generic;

namespace BigQ.Server.Managers
{
    /// <summary>
    /// Manages channels associated with BigQ.
    /// </summary>
    internal class ChannelManager : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private bool _Disposed = false;

        private ServerConfiguration _Config;
        private LoggingModule _Logging;

        private readonly object _ChannelsLock;
        private Dictionary<string, Channel> _Channels;         // GUID, Channel
        
        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="logging">LoggingModule instance.</param>
        /// <param name="config">ServerConfiguration instance.</param>
        public ChannelManager(LoggingModule logging, ServerConfiguration config)
        {
            if (logging == null) throw new ArgumentNullException(nameof(logging));
            if (config == null) throw new ArgumentNullException(nameof(config));

            _Logging = logging;
            _Config = config;
            _ChannelsLock = new object();
            _Channels = new Dictionary<string, Channel>();
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
        /// Retrieves a list of all channels on the server.
        /// </summary>
        /// <returns>A list of Channel objects.</returns>
        public List<Channel> GetChannels()
        {
            lock (_ChannelsLock)
            {
                if (_Channels == null || _Channels.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "GetChannels no channels found");
                    return null;
                }
                
                List<Channel> ret = new List<Channel>();
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    ret.Add(curr.Value);
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetChannels returning " + ret.Count + " channel(s)");
                return ret;
            }
        }

        /// <summary>
        /// Retrieves Channel object associated with supplied GUID.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>A populated Channel object or null.</returns>
        public Channel GetChannelByGUID(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByGUID null GUID supplied");
                return null;
            }

            lock (_ChannelsLock)
            {
                if (_Channels == null || _Channels.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByGUID no channels found");
                    return null;
                }

                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Value.ChannelGUID.Equals(guid)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByGUID returning channel with GUID " + guid);
                        return curr.Value;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByGUID no channel found with GUID " + guid);
                return null;
            }
        }

        /// <summary>
        /// Retrieves Channel object associated with supplied name.
        /// </summary>
        /// <param name="name">Name of the channel.</param>
        /// <returns>A populated Channel object or null.</returns>
        public Channel GetChannelByName(string name)
        {
            if (String.IsNullOrEmpty(name))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByName null name supplied");
                return null;
            }

            lock (_ChannelsLock)
            {
                if (_Channels == null || _Channels.Count < 1)
                {
                    _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByName no channels found");
                    return null;
                }

                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Value.ChannelName.Equals(name)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByName returning channel with name " + name);
                        return curr.Value;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetChannelByName no channel found with name " + name);
                return null;
            }
        }

        /// <summary>
        /// Retrieves Client objects that are members of a Channel with supplied GUID.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>A list of ServerClient objects or null.</returns>
        public List<ServerClient> GetChannelMembers(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));
            List<ServerClient> ret = new List<ServerClient>();

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(guid)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "GetChannelMembers found channel GUID " + guid);

                        if (curr.Value.Members != null && curr.Value.Members.Count > 0)
                        {
                            foreach (ServerClient currClient in curr.Value.Members)
                            {
                                ret.Add(currClient);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "GetChannelMembers returning " + ret.Count + " member(s) for channel GUID " + guid);
                            return ret;
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "GetChannelMembers no members found for channel GUID " + guid);
                        return null;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetChannelMembers unable to find channel GUID " + guid);
                return null;
            }
        }

        /// <summary>
        /// Retrieves Client objects that are subscribers of a Channel with supplied GUID.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>A list of ServerClient objects or null.</returns>
        public List<ServerClient> GetChannelSubscribers(string guid)
        {
            if (String.IsNullOrEmpty(guid)) throw new ArgumentNullException(nameof(guid));

            List<ServerClient> ret = new List<ServerClient>();

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(guid)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "GetChannelSubscribers found channel GUID " + guid);

                        if (curr.Value.Subscribers != null && curr.Value.Subscribers.Count > 0)
                        {
                            foreach (ServerClient currClient in curr.Value.Subscribers)
                            {
                                ret.Add(currClient);
                            }

                            _Logging.Log(LoggingModule.Severity.Debug, "GetChannelSubscribers returning " + ret.Count + " subscriber(s) for channel GUID " + guid);
                            return ret;
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "GetChannelSubscribers no subscribers found for channel GUID " + guid);
                        return null;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "GetChannelSubscribers unable to find channel GUID " + guid);
                return null;
            }
        }

        /// <summary>
        /// Determines if a ServerClient object is a member of the specified Channel.
        /// </summary>
        /// <param name="client">The ServerClient.</param>
        /// <param name="channel">The Channel.</param>
        /// <returns>Boolean indicating if the ServerClient object is a member of the Channel.</returns>
        public bool IsChannelMember(ServerClient client, Channel channel)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (channel == null) throw new ArgumentNullException(nameof(channel));
            
            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(channel.ChannelGUID)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "IsChannelMember found channel GUID " + channel.ChannelGUID);

                        if (curr.Value.Members != null && curr.Value.Members.Count > 0)
                        {
                            foreach (ServerClient currClient in curr.Value.Members)
                            {
                                if (client.ClientGUID.Equals(currClient.ClientGUID)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "IsChannelMember found channel GUID " + channel.ChannelGUID + " member GUID " + currClient.ClientGUID);
                                    return true;
                                }
                            }
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "IsChannelMember client GUID " + client.ClientGUID + " is not a member of channel GUID " + channel.ChannelGUID);
                        return false;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "IsChannelMember unable to find channel GUID " + channel.ChannelGUID);
                return false;
            }
        }

        /// <summary>
        /// Determines if a ServerClient object is a subscriber of the specified Channel.
        /// </summary>
        /// <param name="client">The ServerClient.</param>
        /// <param name="channel">The Channel.</param>
        /// <returns>Boolean indicating if the ServerClient object is a subscriber of the Channel.</returns>
        public bool IsChannelSubscriber(ServerClient client, Channel channel)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (channel == null) throw new ArgumentNullException(nameof(channel));

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(channel.ChannelGUID)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "IsChannelSubscriber found channel GUID " + channel.ChannelGUID);

                        if (curr.Value.Subscribers != null && curr.Value.Subscribers.Count > 0)
                        {
                            foreach (ServerClient currClient in curr.Value.Subscribers)
                            {
                                if (client.ClientGUID.Equals(currClient.ClientGUID)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "IsChannelSubscriber found channel GUID " + channel.ChannelGUID + " subscriber GUID " + currClient.ClientGUID);
                                    return true;
                                }
                            }
                        }

                        _Logging.Log(LoggingModule.Severity.Debug, "IsChannelSubscriber client GUID " + client.ClientGUID + " is not a subscriber of channel GUID " + channel.ChannelGUID);
                        return false;
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "IsChannelSubscriber unable to find channel GUID " + channel.ChannelGUID);
                return false;
            }
        }

        /// <summary>
        /// Determines whether or not a channel exists on the server by supplied GUID.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <returns>Boolean indicating whether or not the channel exists on the server.</returns>
        public bool ChannelExists(string guid)
        {
            if (_Channels == null || _Channels.Count < 1)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "ChannelExists no channels found");
                return false;
            }
            
            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Value != null)
                    {
                        if (!String.IsNullOrEmpty(curr.Value.ChannelGUID))
                        {
                            if (curr.Value.ChannelGUID.Equals(guid)) 
                            {
                                _Logging.Log(LoggingModule.Severity.Debug, "ChannelExists found channel GUID " + guid);
                                return true;
                            }
                        }
                    }
                }

                _Logging.Log(LoggingModule.Severity.Debug, "ChannelExists unable to find channel GUID " + guid);
                return false;
            }
        }

        /// <summary>
        /// Adds a Channel object to the list of channels on the server.
        /// </summary>
        /// <param name="channel">A populated Channel object or null.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool AddChannel(Channel channel)
        {
            if (channel == null)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "AddChannel null channel supplied");
                return false;
            }

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(channel.ChannelGUID)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "AddChannel channel GUID " + channel.ChannelGUID + " already exists");
                        return false;
                    }
                }

                _Channels.Add(channel.ChannelGUID, channel);
                _Logging.Log(LoggingModule.Severity.Debug, "AddChannel added channel " + channel.ChannelGUID);
                return true;
            }
        }

        /// <summary>
        /// Adds a ServerClient object to a Channel as a member.
        /// </summary>
        /// <param name="channel">The Channel.</param>
        /// <param name="client">The ServerClient.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool AddChannelMember(Channel channel, ServerClient client)
        {
            if (channel == null) throw new ArgumentNullException(nameof(channel));
            if (client == null) throw new ArgumentNullException(nameof(client));

            bool matchFound = false;

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(channel.ChannelGUID)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "AddChannelMember successfully found channel " + channel.ChannelGUID);

                        if (curr.Value.Members != null || curr.Value.Members.Count > 0)
                        {
                            foreach (ServerClient c in curr.Value.Members)
                            {
                                if (c.ClientGUID.Equals(client.ClientGUID)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "AddChannelMember member GUID " + c.ClientGUID + " already exists in channel GUID " + channel.ChannelGUID);
                                    matchFound = true;
                                }
                            }
                        }
                        else
                        {
                            curr.Value.Members = new List<ServerClient>();
                        }

                        if (!matchFound)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "AddChannelMember adding member GUID " + client.ClientGUID + " to channel GUID " + channel.ChannelGUID);
                            curr.Value.Members.Add(client);
                            return true;
                        }
                        else
                        {
                            return true;
                        }
                    }
                }
            }

            _Logging.Log(LoggingModule.Severity.Debug, "AddChannelMember unable to find channel GUID " + channel.ChannelGUID);
            return false;
        }

        /// <summary>
        /// Adds a ServerClient object to a Channel as a subscriber.
        /// </summary>
        /// <param name="channel">The Channel.</param>
        /// <param name="client">The ServerClient.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool AddChannelSubscriber(Channel channel, ServerClient client)
        {
            if (channel == null) throw new ArgumentNullException(nameof(channel));
            if (client == null) throw new ArgumentNullException(nameof(client));

            bool matchFound = false;

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(channel.ChannelGUID)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "AddChannelSubscriber successfully found channel " + channel.ChannelGUID);

                        if (curr.Value.Subscribers != null || curr.Value.Subscribers.Count > 0)
                        {
                            foreach (ServerClient c in curr.Value.Subscribers)
                            {
                                if (c.ClientGUID.Equals(client.ClientGUID)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "AddChannelSubscriber subscriber GUID " + c.ClientGUID + " already exists in channel GUID " + channel.ChannelGUID);
                                    matchFound = true;
                                }
                            }
                        }
                        else
                        {
                            curr.Value.Members = new List<ServerClient>();
                        }

                        if (!matchFound)
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "AddChannelSubscriber adding subscriber GUID " + client.ClientGUID + " to channel GUID " + channel.ChannelGUID);
                            curr.Value.Subscribers.Add(client);
                            return true;
                        }
                        else
                        {
                            return true;
                        }
                    }
                }
            }

            _Logging.Log(LoggingModule.Severity.Debug, "AddChannelSubscriber unable to find channel GUID " + channel.ChannelGUID);
            return false;
        }

        /// <summary>
        /// Removes a Channel object from the server.
        /// </summary>
        /// <param name="guid">The GUID of the channel.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool RemoveChannel(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel null GUID supplied");
                return false;
            }

            bool found = false;
            Dictionary<string, Channel> updated = new Dictionary<string, Channel>();

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(guid)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannel found channel GUID " + guid + ", skipping to remove");
                        found = true;
                        continue;
                    }

                    updated.Add(curr.Key, curr.Value);
                }

                _Channels = updated;
                return found;
            }
        }

        /// <summary>
        /// Remove channels associated with the GUID of a ServerClient object.
        /// </summary>
        /// <param name="ownerGuid">GUID of the ServerClient object.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool RemoveClientChannels(string ownerGuid, out List<Channel> affectedChannels)
        {
            affectedChannels = new List<Channel>();

            if (String.IsNullOrEmpty(ownerGuid))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "RemoveClientChannels null GUID supplied");
                return false;
            }

            bool found = false;
            Dictionary<string, Channel> updated = new Dictionary<string, Channel>();

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Value.OwnerGUID.Equals(ownerGuid)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "RemoveClientChannels found channel GUID " + curr.Value.ChannelGUID + " owned by GUID " + ownerGuid + ", skipping to remove");
                        affectedChannels.Add(curr.Value);
                        found = true;
                        continue;
                    }

                    updated.Add(curr.Key, curr.Value);
                }

                _Channels = updated;
                return found;
            }
        }

        /// <summary>
        /// Remove a ServerClient object from a Channel's member list.
        /// </summary>
        /// <param name="channel">The Channel from which the Client should be removed.</param>
        /// <param name="client">The ServerClient object that should be removed from the channel.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool RemoveChannelMember(Channel channel, ServerClient client)
        {
            if (channel == null) throw new ArgumentNullException(nameof(channel));
            if (client == null) throw new ArgumentNullException(nameof(client));

            bool matchFound = false;

            lock (_ChannelsLock)
            {
                Channel updatedChannel = null;

                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(channel.ChannelGUID)) 
                    {
                        #region Channel-Found

                        _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelMember found channel GUID " + channel.ChannelGUID + " (" + curr.Value.Members.Count + ") members");
                        updatedChannel = channel;
                        List<ServerClient> updatedMembers = new List<ServerClient>();

                        if (curr.Value.Members != null && curr.Value.Members.Count > 0)
                        {
                            foreach (ServerClient c in curr.Value.Members)
                            {
                                if (c.ClientGUID.Equals(client.ClientGUID)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelMember found member GUID " + c.ClientGUID + " in channel GUID " + channel.ChannelGUID + ", skipping to remove");
                                    matchFound = true;
                                }
                                else
                                {
                                    updatedMembers.Add(c);
                                }
                            }

                            updatedChannel.Members = updatedMembers;
                        }
                        else
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelMember no channel members found");
                        }

                        #endregion
                    }
                }

                if (updatedChannel != null)
                {
                    Dictionary<string, Channel> updatedChannels = new Dictionary<string, Channel>();

                    foreach (KeyValuePair<string, Channel> currKvp in _Channels)
                    {
                        if (String.Compare(currKvp.Key, updatedChannel.ChannelGUID) != 0)
                        {
                            updatedChannels.Add(currKvp.Key, currKvp.Value);
                        }
                    }

                    updatedChannels.Add(updatedChannel.ChannelGUID, updatedChannel);
                    _Channels = updatedChannels;
                }
            }

            return matchFound;
        }

        /// <summary>
        /// Remove a ServerClient object from a Channel's subscriber list.
        /// </summary>
        /// <param name="channel">The Channel from which the Client should be removed.</param>
        /// <param name="client">The ServerClient that should be removed from the channel.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool RemoveChannelSubscriber(Channel channel, ServerClient client)
        {
            if (channel == null) throw new ArgumentNullException(nameof(channel));
            if (client == null) throw new ArgumentNullException(nameof(client));

            bool matchFound = false;

            lock (_ChannelsLock)
            {
                Channel updatedChannel = null;

                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (curr.Key.Equals(channel.ChannelGUID)) 
                    {
                        #region Channel-Found

                        _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelSubscriber found channel GUID " + channel.ChannelGUID + " (" + curr.Value.Subscribers.Count + ") subscribers");

                        updatedChannel = channel;
                        List<ServerClient> updatedSubscribers = new List<ServerClient>();

                        if (curr.Value.Subscribers != null && curr.Value.Subscribers.Count > 0)
                        {
                            foreach (ServerClient c in curr.Value.Subscribers)
                            {
                                if (c.ClientGUID.Equals(client.ClientGUID)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelSubscriber found subscriber GUID " + c.ClientGUID + " in channel GUID " + channel.ChannelGUID + ", skipping to remove");
                                    matchFound = true;
                                }
                                else
                                {
                                    updatedSubscribers.Add(c);
                                }
                            }

                            updatedChannel.Subscribers = updatedSubscribers;
                        }
                        else
                        {
                            _Logging.Log(LoggingModule.Severity.Debug, "RemoveChannelSubscriber no channel subscribers found");
                        }

                        #endregion
                    }
                }

                if (updatedChannel != null)
                {
                    Dictionary<string, Channel> updatedChannels = new Dictionary<string, Channel>();

                    foreach (KeyValuePair<string, Channel> currKvp in _Channels)
                    {
                        if (String.Compare(currKvp.Key, updatedChannel.ChannelGUID) != 0)
                        {
                            updatedChannels.Add(currKvp.Key, currKvp.Value);
                        }
                    }

                    updatedChannels.Add(updatedChannel.ChannelGUID, updatedChannel);
                    _Channels = updatedChannels;
                }
            }

            return matchFound;
        }

        /// <summary>
        /// Updates an existing Channel object on the server.
        /// </summary>
        /// <param name="channel">The Channel object.</param>
        public void UpdateChannel(Channel channel)
        {
            if (channel == null)
            {
                _Logging.Log(LoggingModule.Severity.Debug, "UpdateChannel null channel supplied");
                return;
            }

            Dictionary<string, Channel> updated = new Dictionary<string, Channel>();

            lock (_ChannelsLock)
            {
                foreach (KeyValuePair<string, Channel> curr in _Channels)
                {
                    if (channel.ChannelGUID.Equals(curr.Key)) 
                    {
                        _Logging.Log(LoggingModule.Severity.Debug, "UpdateClient found channel GUID " + channel.ChannelGUID + ", updating");
                        updated.Add(channel.ChannelGUID, channel);
                        continue;
                    }
                    updated.Add(curr.Key, curr.Value);
                }

                return;
            }
        }

        /// <summary>
        /// Removes a ServerClient object from all Channel member and subscriber lists.
        /// </summary>
        /// <param name="ipPort">The IP:port of the Client.</param>
        public void RemoveClient(string ipPort)
        {
            if (String.IsNullOrEmpty(ipPort))
            {
                _Logging.Log(LoggingModule.Severity.Debug, "RemoveClient null IP:port supplied");
                return;
            }

            lock (_ChannelsLock)
            {
                if (_Channels != null && _Channels.Count > 0)
                {
                    Dictionary<string, Channel> updated = new Dictionary<string, Channel>();    // GUID, Channel

                    foreach (KeyValuePair<string, Channel> curr in _Channels)
                    {
                        List<ServerClient> updatedMembers = new List<ServerClient>();
                        List<ServerClient> updatedSubscribers = new List<ServerClient>();

                        if (curr.Value.Members != null && curr.Value.Members.Count > 0)
                        {
                            foreach (ServerClient currMember in curr.Value.Members)
                            {
                                if (currMember.IpPort.Equals(ipPort)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "RemoveClient removing member GUID " + currMember.ClientGUID + " from channel GUID " + curr.Value.ChannelGUID);
                                    continue;
                                }
                                else
                                {
                                    updatedMembers.Add(currMember);
                                }
                            }
                        }

                        if (curr.Value.Subscribers != null && curr.Value.Subscribers.Count > 0)
                        {
                            foreach (ServerClient currSubscriber in curr.Value.Subscribers)
                            {
                                if (currSubscriber.IpPort.Equals(ipPort)) 
                                {
                                    _Logging.Log(LoggingModule.Severity.Debug, "RemoveClient removing subscriber GUID " + currSubscriber.ClientGUID + " from channel GUID " + curr.Value.ChannelGUID);
                                    continue;
                                }
                                else
                                {
                                    updatedSubscribers.Add(currSubscriber);
                                }
                            }
                        }

                        curr.Value.Members = updatedMembers;
                        curr.Value.Subscribers = updatedSubscribers;
                        updated.Add(curr.Key, curr.Value);
                    }

                    _Channels = updated;
                }
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
