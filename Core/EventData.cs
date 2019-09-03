using System;
using System.Runtime.Serialization;
using System.Text;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;

namespace BigQ.Core
{
    /// <summary>
    /// The available event types.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EventTypes
    {
        [EnumMember(Value = "ClientJoinedServer")]
        ClientJoinedServer,
        [EnumMember(Value = "ClientLeftServer")]
        ClientLeftServer,
        [EnumMember(Value = "ClientJoinedChannel")]
        ClientJoinedChannel,
        [EnumMember(Value = "ClientLeftChannel")]
        ClientLeftChannel,
        [EnumMember(Value = "SubscriberJoinedChannel")]
        SubscriberJoinedChannel,
        [EnumMember(Value = "SubscriberLeftChannel")]
        SubscriberLeftChannel,
        [EnumMember(Value = "ChannelCreated")]
        ChannelCreated,
        [EnumMember(Value = "ChannelDestroyed")]
        ChannelDestroyed
    }

    /// <summary>
    /// Object containing metadata about an event that occurred on BigQ.
    /// </summary>
    [Serializable]
    public class EventData
    {
        //
        //
        // Intended to be payload within BigQMessage.Data
        //
        //

        #region Public-Members

        /// <summary>
        /// The type of event.
        /// </summary>
        public EventTypes EventType { get; set; }

        /// <summary>
        /// Event-related data.
        /// </summary>
        public object Data { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Do not use.  Used internally by BigQ libraries.
        /// </summary>
        public EventData()
        {

        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a string formatted with event details.
        /// </summary>
        /// <returns>Returns a string formatted with event details.</returns>
        public override string ToString()
        {
            string ret = "";
            ret += Environment.NewLine;
            ret += " Event: EventType " + EventType.ToString() + " ";

            if (Data != null)
            {
                string DataString = Data.ToString();
                ret += " Data (" + DataString.Length + " bytes): " + Environment.NewLine;
                ret += DataString + Environment.NewLine;
            }
            else
            {
                ret += " Data: (null)" + Environment.NewLine;
            }

            return ret;
        }

        /// <summary>
        /// Create a byte array containing an EventData object.
        /// </summary>
        /// <param name="eventType">The type of event.</param>
        /// <param name="data">The data associated with the event.</param>
        /// <returns></returns>
        public static byte[] ToBytes(EventTypes eventType, object data)
        {
            EventData e = new EventData();
            e.EventType = eventType;
            if (data != null) e.Data = data;
            return Encoding.UTF8.GetBytes(Common.SerializeJson(e));
        }

        #endregion

        #region Private-Methods

        #endregion 
    }
}