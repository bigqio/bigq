using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;

namespace BigQ.Client.Classes
{
    /// <summary>
    /// Types of errors included in error responses.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ErrorTypes
    {
        /// <summary>
        /// Unknown error.
        /// </summary>
        [EnumMember(Value = "Unknown")]
        Unknown,
        /// <summary>
        /// An error has occurred on the server.
        /// </summary>
        [EnumMember(Value = "ServerError")]
        ServerError,
        /// <summary>
        /// The recipient was not found.
        /// </summary>
        [EnumMember(Value = "RecipientNotFound")]
        RecipientNotFound,
        /// <summary>
        /// The channel was not found.
        /// </summary>
        [EnumMember(Value = "ChannelNotFound")]
        ChannelNotFound,
        /// <summary>
        /// Bad request.
        /// </summary>
        [EnumMember(Value = "BadRequest")]
        BadRequest,
        /// <summary>
        /// Authentication failed.
        /// </summary>
        [EnumMember(Value = "AuthenticationFailed")]
        AuthenticationFailed,
        /// <summary>
        /// Authorization failed.
        /// </summary>
        [EnumMember(Value = "AuthorizationFailed")]
        AuthorizationFailed,
        /// <summary>
        /// Login required.
        /// </summary>
        [EnumMember(Value = "LoginRequired")]
        LoginRequired,
        /// <summary>
        /// Unknown command.
        /// </summary>
        [EnumMember(Value = "UnknownCommand")]
        UnknownCommand,
        /// <summary>
        /// The recipient is not a channel member.
        /// </summary>
        [EnumMember(Value = "NotAChannelMember")]
        NotAChannelMember,
        /// <summary>
        /// Unable to queue the message.
        /// </summary>
        [EnumMember(Value = "UnableToQueue")]
        UnableToQueue,
        /// <summary>
        /// The channel has no members.
        /// </summary>
        [EnumMember(Value = "NoChannelMembers")]
        NoChannelMembers,
        /// <summary>
        /// The channel has no subscribers.
        /// </summary>
        [EnumMember(Value = "NoChannelSubscribers")]
        NoChannelSubscribers,
        /// <summary>
        /// The channel already exists.
        /// </summary>
        [EnumMember(Value = "ChannelAlreadyExists")]
        ChannelAlreadyExists,
        /// <summary>
        /// Unable to create the channel.
        /// </summary>
        [EnumMember(Value = "UnableToCreateChannel")]
        UnableToCreateChannel,
        /// <summary>
        /// Unable to delete the channel.
        /// </summary>
        [EnumMember(Value = "UnableToDeleteChannel")]
        UnableToDeleteChannel,
        /// <summary>
        /// Unable to join the channel.
        /// </summary>
        [EnumMember(Value = "UnableToJoinChannel")]
        UnableToJoinChannel,
        /// <summary>
        /// Unable to leave the channel.
        /// </summary>
        [EnumMember(Value = "UnableToLeaveChannel")]
        UnableToLeaveChannel,
        /// <summary>
        /// Unable to subscribe to the channel.
        /// </summary>
        [EnumMember(Value = "UnableToSubscribeChannel")]
        UnableToSubscribeChannel,
        /// <summary>
        /// Unable to unsubscribe from the channel.
        /// </summary>
        [EnumMember(Value = "UnableToUnsubscribeChannel")]
        UnableToUnsubscribeChannel,
        /// <summary>
        /// There is an error in the supplied data.
        /// </summary>
        [EnumMember(Value = "DataError")]
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

        /// <summary>
        /// Instantiates the object.
        /// </summary>
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
            return Encoding.UTF8.GetBytes(Common.SerializeJson(outer));
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
