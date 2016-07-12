using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Structure for the BigQ configuration file.
    /// </summary>
    [Serializable]
    public class ServerConfig
    {
        #region Class-Members

        public string Guid;
        public string ChannelName;
        public string OwnerGuid;
        public DateTime? CreatedUTC;
        public DateTime? UpdatedUTC;
        public int? Private;

        public List<BigQClient> Subscribers;

        #endregion

        #region Delegates

        #endregion
    }
}
