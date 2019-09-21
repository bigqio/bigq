using System;
using System.Runtime.Serialization;
using System.Text;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;

namespace BigQ.Server.Classes
{
    /// <summary>
    /// The available event types.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EventTypes
    {
        /// <summary>
        /// Client has joined the server.
        /// </summary>
        [EnumMember(Value = "ClientJoinedServer")]
        ClientJoinedServer,
        /// <summary>
        /// Client has left the server.
        /// </summary>
        [EnumMember(Value = "ClientLeftServer")]
        ClientLeftServer,
        /// <summary>
        /// Client has joined the channel.
        /// </summary>
        [EnumMember(Value = "ClientJoinedChannel")]
        ClientJoinedChannel,
        /// <summary>
        /// Client has left the channel.
        /// </summary>
        [EnumMember(Value = "ClientLeftChannel")]
        ClientLeftChannel,
        /// <summary>
        /// Subscriber has joined the channel.
        /// </summary>
        [EnumMember(Value = "SubscriberJoinedChannel")]
        SubscriberJoinedChannel,
        /// <summary>
        /// Subscriber has left the channel.
        /// </summary>
        [EnumMember(Value = "SubscriberLeftChannel")]
        SubscriberLeftChannel,
        /// <summary>
        /// A channel has been created.
        /// </summary>
        [EnumMember(Value = "ChannelCreated")]
        ChannelCreated,
        /// <summary>
        /// A channel has been destroyed.
        /// </summary>
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