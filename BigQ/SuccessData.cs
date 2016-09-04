using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Object containing details about a successful request.
    /// </summary>
    [Serializable]
    public class SuccessData
    {
        #region Class-Members
        
        /// <summary>
        /// Indicates whether or not the request succeeded or failed.
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Success response data.
        /// </summary>
        public object Data { get; set; }

        #endregion

        #region Factory

        /// <summary>
        /// Create a byte array containing a success response object.
        /// </summary>
        /// <param name="detail">Details related to the response.</param>
        /// <param name="data">Response data.</param>
        /// <returns></returns>
        public static byte[] ToBytes(string detail, object data)
        {
            Dictionary<string, object> dict = new Dictionary<string, object>();
            dict.Add("Success", true);
            if (!String.IsNullOrEmpty(detail)) dict.Add("Detail", detail);
            if (data != null) dict.Add("Data", data);

            return Encoding.UTF8.GetBytes(Helper.SerializeJson(dict));
        }

        #endregion
    }
}
