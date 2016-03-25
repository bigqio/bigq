using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    [Serializable]
    public class BigQEvent
    {
        //
        //
        // Intended to be payload within BigQMessage.Data
        //
        //

        #region Class-Variables

        public string EventType;
        public object Data;

        #endregion

        #region Constructor

        public BigQEvent()
        {

        }

        #endregion

        #region Instance-Methods
        
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
