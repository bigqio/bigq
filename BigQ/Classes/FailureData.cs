using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Types of errors included in error responses.
    /// </summary>
    public enum ErrorTypes
    {
        Unknown,
        ServerError,
        RecipientNotFound,
        ChannelNotFound,
        BadRequest,
        AuthenticationFailed,
        AuthorizationFailed,
        LoginRequired,
        UnknownCommand,
        NotAChannelMember,
        UnableToQueue,
        NoChannelMembers,
        NoChannelSubscribers,
        ChannelAlreadyExists,
        UnableToCreateChannel,
        UnableToDeleteChannel,
        UnableToJoinChannel,
        UnableToLeaveChannel,
        UnableToSubscribeChannel,
        UnableToUnsubscribeChannel,
        DataError
    }

    /// <summary>
    /// Object containing details about an encountered error.
    /// </summary>
    [Serializable]
    public class FailureData
    {
        #region Public-Members

        /// <summary>
        /// Indicates whether or not the response succeeded or failed.
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// The type of error referenced in the message.
        /// </summary>
        public ErrorTypes ErrorType { get; set; }

        /// <summary>
        /// Additional detail or context about the error.
        /// </summary>
        public string ErrorDetail { get; set; }

        /// <summary>
        /// Failure response data.
        /// </summary>
        public object Data { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public FailureData()
        {

        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Create a byte array containng an error object.
        /// </summary>
        /// <param name="error">The type of error.</param>
        /// <param name="detail">Additional detail related to the error.</param>
        /// <param name="data">Additional data related to the error.</param>
        /// <returns></returns>
        public static byte[] ToBytes(ErrorTypes error, string detail, object data)
        {
            Dictionary<string, object> outer = new Dictionary<string, object>();
            outer.Add("Success", false);

            Dictionary<string, object> inner = new Dictionary<string, object>();
            inner.Add("ErrorType", error.ToString());
            if (!String.IsNullOrEmpty(detail)) inner.Add("ErrorDetail", detail);
            if (data != null) inner.Add("Data", data);

            outer.Add("Error", inner);
            return Encoding.UTF8.GetBytes(Helper.SerializeJson(outer));
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
