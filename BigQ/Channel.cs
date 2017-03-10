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

        public string ChannelGUID;
        public string ChannelName;
        public string OwnerGUID;
        public DateTime? CreatedUtc;
        public DateTime? UpdatedUtc;
        public int? Private;
        public int? Broadcast;
        public int? Multicast;
        public int? Unicast;

        public List<Client> Members;
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
