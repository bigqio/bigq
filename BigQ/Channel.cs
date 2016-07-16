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

        public string Guid;
        public string ChannelName;
        public string OwnerGuid;
        public DateTime? CreatedUTC;
        public DateTime? UpdatedUTC;
        public int? Private;
        public int? Broadcast;
        public int? Multicast;

        public List<Client> Members;
        public List<Client> Subscribers;

        #endregion

        #region Delegates

        #endregion
    }
}
