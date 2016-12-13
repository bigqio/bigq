using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// The available event types.
    /// </summary>
    public enum EventTypes
    {
        ClientJoinedServer,
        ClientLeftServer,
        ClientJoinedChannel,
        ClientLeftChannel,
        SubscriberJoinedChannel,
        SubscriberLeftChannel,
        ChannelCreated,
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

        #region Class-Variables

        /// <summary>
        /// The type of event.
        /// </summary>
        public EventTypes EventType { get; set; }

        /// <summary>
        /// Event-related data.
        /// </summary>
        public object Data { get; set; }

        #endregion

        #region Constructor

        /// <summary>
        /// Do not use.  Used internally by BigQ libraries.
        /// </summary>
        public EventData()
        {

        }

        #endregion

        #region Instance-Methods

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

        #endregion

        #region Factory

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
            return Encoding.UTF8.GetBytes(Helper.SerializeJson(e));
        }

        #endregion
    }
}
