using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    /// <summary>
    /// Object containing metadata about an event that occurred on BigQ.
    /// </summary>
    [Serializable]
    public class Event
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
        public string EventType;

        /// <summary>
        /// Event-related data.
        /// </summary>
        public object Data;

        #endregion

        #region Constructor

        /// <summary>
        /// Do not use.  Used internally by BigQ libraries.
        /// </summary>
        public Event()
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
            ret += " Event: EventType " + EventType + " ";
            
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
    }
}
