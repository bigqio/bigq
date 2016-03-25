using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    [Serializable]
    public class BigQChannel
    {
        #region Class-Members

        public string Guid;
        public string ChannelName;
        public string OwnerGuid;
        public DateTime Created;
        public DateTime Updated;
        public int Private;

        public List<BigQClient> Subscribers;

        #endregion

        #region Delegates

        #endregion
    }
}
