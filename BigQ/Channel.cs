using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Object containing metadata about a channel on BigQ.
    /// </summary>
    [Serializable]
    public class Channel
    {
        #region Class-Members

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
        public List<Client> Members;

        /// <summary>
        /// Clients that are subscribers of the channel.
        /// </summary>
        public List<Client> Subscribers;

        #endregion

        #region Constructor

        public Channel()
        {
            DateTime ts = DateTime.Now.ToUniversalTime();
            CreatedUtc = ts;
            UpdatedUtc = ts;
            Members = new List<Client>();
            Subscribers = new List<Client>();
        }

        #endregion
    }
}
