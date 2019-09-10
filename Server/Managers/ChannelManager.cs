using System;
using System.Collections.Generic;
using System.Linq;

using BigQ.Server.Classes;

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
         
        private ServerConfiguration _Config; 

        private readonly object _ChannelsLock;
        private Dictionary<string, Channel> _Channels;         // GUID, Channel
        
        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary> 
        /// <param name="config">ServerConfiguration instance.</param>
        public ChannelManager(ServerConfiguration config)
        { 
            if (config == null) throw new ArgumentNullException(nameof(config));
             
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
            _Channels = null;
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
                    return null;
                }
                List<Channel> ret = _Channels.Values.ToList(); 
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
                return null;
            }

            lock (_ChannelsLock)
            {
                if (_Channels == null || _Channels.Count < 1)
                {
                    return null;
                }
                Channel ret = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(guid.ToLower())).Value;
                if (ret == default(Channel))
                {
                    return null;
                }
                return ret;
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
                return null;
            }

            lock (_ChannelsLock)
            {
                if (_Channels == null || _Channels.Count < 1)
                {
                    return null;
                }
                Channel ret = _Channels.FirstOrDefault(c => c.Value.ChannelName.ToLower().Equals(name.ToLower())).Value;
                if (ret == default(Channel))
                {
                    return null;
                }
                return ret;
            }
        }

        /// <summary>
        /// Retrieves Client objects that are members of a Channel with supplied GUID.
        /// </summary>
        /// <param name="guid">GUID of the channel.</param>
        /// <returns>A list of ServerClient objects or null.</returns>
        public List<ServerClient> GetChannelMembers(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                return null;
            }
            List<ServerClient> ret = new List<ServerClient>();

            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(guid.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return null;
                }
                if (curr.Members != null && curr.Members.Count > 0)
                {
                    return new List<ServerClient>(curr.Members);
                }
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
            if (String.IsNullOrEmpty(guid))
            {
                return null;
            }

            List<ServerClient> ret = new List<ServerClient>();

            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(guid.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return null;
                }
                if (curr.Subscribers != null && curr.Subscribers.Count > 0)
                {
                    return new List<ServerClient>(curr.Subscribers);
                }
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
            if (client == null)
            {
                return false;
            }
            if (channel == null)
            {
                return false;
            }
            
            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return false;
                }
                if (curr.Members != null && curr.Members.Count > 0)
                {
                    ServerClient sc = curr.Members.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower()));
                    if (sc == null || sc == default(ServerClient))
                    {
                        return false;
                    }
                    return true;
                } 
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
            if (client == null)
            {
                return false;
            }
            if (channel == null)
            {
                return false;
            }

            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return false;
                }
                if (curr.Subscribers != null && curr.Subscribers.Count > 0)
                {
                    ServerClient sc = curr.Subscribers.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower()));
                    if (sc == null || sc == default(ServerClient))
                    {
                        return false;
                    }
                    return true;
                }
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
                return false;
            }
            
            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(guid.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return false;
                }
                return true; 
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
                return false;
            }

            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr != null && curr != default(Channel))
                {
                    return false;
                }
                _Channels.Add(channel.ChannelGUID, channel);
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
            if (channel == null)
            { 
                return false;
            }
            if (client == null)
            { 
                return false;
            }
             
            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                { 
                    return false;
                }

                ServerClient sc = curr.Members.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower()));
                if (sc == null || sc == default(ServerClient)) 
                { 
                    curr.Members.Add(client);
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr); 
                }
                else
                { 
                    curr.Members.Remove(sc);
                    curr.Members.Add(client); 
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr);
                }
            }
             
            return true;
        }

        /// <summary>
        /// Adds a ServerClient object to a Channel as a subscriber.
        /// </summary>
        /// <param name="channel">The Channel.</param>
        /// <param name="client">The ServerClient.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool AddChannelSubscriber(Channel channel, ServerClient client)
        {
            if (channel == null)
            {
                return false;
            }
            if (client == null)
            {
                return false;
            }
             
            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                { 
                    return false;
                }

                ServerClient sc = curr.Subscribers.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower()));
                if (sc == null || sc == default(ServerClient))
                { 
                    curr.Subscribers.Add(client);
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr);
                }
                else
                { 
                    curr.Subscribers.Remove(sc);
                    curr.Subscribers.Add(client);
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr);
                } 
            }
             
            return true;
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
                return false;
            }

            lock (_ChannelsLock)
            {
                List<Channel> remove = _Channels.Values.Where(c => c.ChannelGUID.ToLower().Equals(guid.ToLower())).ToList();
                if (remove != null && remove.Count > 0)
                {
                    foreach (Channel curr in remove)
                    {
                        _Channels.Remove(curr.ChannelGUID);
                    }
                } 
            }

            return true;
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
                return false;
            }

            Dictionary<string, Channel> updated = new Dictionary<string, Channel>();

            lock (_ChannelsLock)
            {
                affectedChannels = _Channels.Values.Where(c => c.OwnerGUID.ToLower().Equals(ownerGuid.ToLower())).ToList();
                if (affectedChannels != null && affectedChannels.Count > 0)
                {
                    foreach (Channel curr in affectedChannels)
                    {
                        _Channels.Remove(curr.ChannelGUID);
                    }

                    return true;
                }

                return false;
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
            if (channel == null)
            {
                return false;
            }
            if (client == null)
            {
                return false;
            }

            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return false;
                }

                ServerClient sc = curr.Members.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower()));
                if (sc == null || sc == default(ServerClient))
                {
                    return false;
                }

                if (curr.Members != null && curr.Members.Count > 0)
                {
                    List<ServerClient> updated = curr.Members.Where(m => !m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower())).ToList();
                    curr.Members = updated;
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr);
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// Remove a ServerClient object from a Channel's member list.
        /// </summary>
        /// <param name="channel">The Channel from which the Client should be removed.</param>
        /// <param name="clientGuid">The GUID of the ServerClient object that should be removed from the channel.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool RemoveChannelMember(Channel channel, string clientGuid)
        {
            if (channel == null)
            {
                return false;
            }
            if (String.IsNullOrEmpty(clientGuid))
            {
                return false;
            }

            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return false;
                }

                ServerClient sc = curr.Members.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(clientGuid.ToLower()));
                if (sc == null || sc == default(ServerClient))
                {
                    return false;
                }

                if (curr.Members != null && curr.Members.Count > 0)
                {
                    List<ServerClient> updated = curr.Members.Where(m => !m.ClientGUID.ToLower().Equals(clientGuid.ToLower())).ToList();
                    curr.Members = updated;
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr);
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// Remove a ServerClient object from a Channel's subscriber list.
        /// </summary>
        /// <param name="channel">The Channel from which the Client should be removed.</param>
        /// <param name="client">The ServerClient that should be removed from the channel.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool RemoveChannelSubscriber(Channel channel, ServerClient client)
        {
            if (channel == null)
            { 
                return false;
            }
            if (client == null)
            { 
                return false;
            }
             
            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                { 
                    return false;
                }

                ServerClient sc = curr.Subscribers.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower()));
                if (sc == null || sc == default(ServerClient))
                { 
                    return false;
                }

                if (curr.Subscribers != null && curr.Subscribers.Count > 0)
                {
                    List<ServerClient> updated = curr.Subscribers.Where(m => !m.ClientGUID.ToLower().Equals(client.ClientGUID.ToLower())).ToList();
                    curr.Subscribers = updated;
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr); 
                    return true;
                }
                 
                return false;
            }
        }

        /// <summary>
        /// Remove a ServerClient object from a Channel's subscriber list.
        /// </summary>
        /// <param name="channel">The Channel from which the Client should be removed.</param>
        /// <param name="clientGuid">The GUID of the ServerClient that should be removed from the channel.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool RemoveChannelSubscriber(Channel channel, string clientGuid)
        {
            if (channel == null)
            {
                return false;
            }
            if (String.IsNullOrEmpty(clientGuid))
            {
                return false;
            }

            lock (_ChannelsLock)
            {
                Channel curr = _Channels.FirstOrDefault(c => c.Value.ChannelGUID.ToLower().Equals(channel.ChannelGUID.ToLower())).Value;
                if (curr == null || curr == default(Channel))
                {
                    return false;
                }

                ServerClient sc = curr.Subscribers.FirstOrDefault(m => m.ClientGUID.ToLower().Equals(clientGuid.ToLower()));
                if (sc == null || sc == default(ServerClient))
                {
                    return false;
                }

                if (curr.Subscribers != null && curr.Subscribers.Count > 0)
                {
                    List<ServerClient> updated = curr.Subscribers.Where(m => !m.ClientGUID.ToLower().Equals(clientGuid.ToLower())).ToList();
                    curr.Subscribers = updated;
                    _Channels.Remove(curr.ChannelGUID);
                    _Channels.Add(curr.ChannelGUID, curr);
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// Removes a ServerClient object from all Channel member and subscriber lists.
        /// </summary>
        /// <param name="guid">The GUID of the Client.</param>
        public void RemoveClient(string guid)
        {
            if (String.IsNullOrEmpty(guid))
            {
                return;
            }

            List<Channel> owned = new List<Channel>();
            List<Channel> member = new List<Channel>();
            List<Channel> subscriber = new List<Channel>();

            lock (_ChannelsLock)
            {
                if (_Channels != null && _Channels.Count > 0)
                {
                    Dictionary<string, Channel> updated = new Dictionary<string, Channel>();    // GUID, Channel

                    owned = _Channels.Values.Where(c => c.OwnerGUID.ToLower().Equals(guid.ToLower())).ToList();

                    member = _Channels.Values.Where(c =>
                        c.Members != null
                        && c.Members.Exists(d => d.ClientGUID.ToLower().Equals(guid.ToLower()))).ToList();

                    subscriber = _Channels.Values.Where(c =>
                        c.Subscribers != null
                        && c.Subscribers.Exists(d => d.ClientGUID.ToLower().Equals(guid.ToLower()))).ToList();
                }
            }

            if (member != null && member.Count > 0)
            {
                foreach (Channel curr in member)
                {
                    RemoveChannelMember(curr, guid);
                }
            }

            if (subscriber != null && subscriber.Count > 0)
            {
                foreach (Channel curr in subscriber)
                {
                    RemoveChannelSubscriber(curr, guid);
                }
            }

            if (owned != null && owned.Count > 0)
            {
                foreach (Channel curr in owned)
                {
                    RemoveChannel(curr.ChannelGUID);
                }
            } 
        }

        #endregion

        #region Private-Methods
         
        #endregion
    }
}
