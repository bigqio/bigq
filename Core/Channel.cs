using System;
using System.Collections.Generic;

namespace BigQ.Core
{
    /// <summary>
    /// Object containing metadata about a channel on BigQ.
    /// </summary>
    [Serializable]
    public class Channel
    {
        #region Public-Members

        /// <summary>
        /// The GUID of the channel.
        /// </summary>
        public string ChannelGUID;

        /// <summary>
        /// The name of the channel.
        /// </summary>
        public string ChannelName;

        /// <summary>
        /// The GUID of the client (or server) that created the channel.
        /// </summary>
        public string OwnerGUID;

        /// <summary>
        /// The creation time.
        /// </summary>
        public DateTime? CreatedUtc;

        /// <summary>
        /// The time of last update.
        /// </summary>
        public DateTime? UpdatedUtc;

        /// <summary>
        /// Indicates whether or not the channel is private, i.e. hidden from list channel responses.
        /// </summary>
        public int? Private;

        /// <summary>
        /// Indicates if the channel is a broadcast channel.
        /// </summary>
        public int? Broadcast;

        /// <summary>
        /// Indicates if the channel is a multicast channel.
        /// </summary>
        public int? Multicast;

        /// <summary>
        /// Indicates if the channel is a unicast channel.
        /// </summary>
        public int? Unicast;

        /// <summary>
        /// Clients that are members of the channel.
        /// </summary>
        public List<ServerClient> Members;

        /// <summary>
        /// Clients that are subscribers of the channel.
        /// </summary>
        public List<ServerClient> Subscribers;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public Channel()
        {
            DateTime ts = DateTime.Now.ToUniversalTime();
            CreatedUtc = ts;
            UpdatedUtc = ts;
            Members = new List<ServerClient>();
            Subscribers = new List<ServerClient>();
        }

        #endregion

        #region Public-Methods

        public static Channel FromMessage(ServerClient currentClient, Message currentMessage)
        {
            if (currentClient == null) throw new ArgumentNullException(nameof(currentClient));
            if (currentMessage == null) throw new ArgumentNullException(nameof(currentMessage));
            if (currentMessage.Data == null) throw new ArgumentException("Message data cannot be null.");

            Channel ret = null;
            try
            {
                ret = Common.DeserializeJson<Channel>(currentMessage.Data);
            }
            catch (Exception)
            { 
                ret = null;
            }

            if (ret == null)
            {
                return null;
            }

            // assume ret.Private is set in the request
            if (ret.Private == default(int)) ret.Private = 0;

            if (String.IsNullOrEmpty(ret.ChannelGUID)) ret.ChannelGUID = Guid.NewGuid().ToString();
            if (String.IsNullOrEmpty(ret.ChannelName)) ret.ChannelName = ret.ChannelGUID;
            ret.CreatedUtc = DateTime.Now.ToUniversalTime();
            ret.UpdatedUtc = ret.CreatedUtc;
            ret.OwnerGUID = currentClient.ClientGUID;
            ret.Members = new List<ServerClient>();
            ret.Members.Add(currentClient);
            ret.Subscribers = new List<ServerClient>();
            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
