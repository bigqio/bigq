using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    [Serializable]
    public class BigQMessage
    {
        #region Class-Variables

        public string Email;
        public string Password;
        public string Command;
        public DateTime Created;
        
        // flags set by receiver when responding
        public bool? Success;

        // flags set by sender/receiver
        public bool? SyncRequest;
        public bool? SyncResponse;

        // used for sending/receiving messages
        // GUIDs can be an individual or a channel
        public string MessageId;
        public string SenderGuid;
        public string RecipientGuid;
        public string ChannelGuid;

        public object Data;

        #endregion

        #region Constructor

        public BigQMessage()
        {

        }

        #endregion

        #region Instance-Methods

        public bool IsValid()
        {
            List<string> errors = new List<string>();
            if (String.IsNullOrEmpty(MessageId)) errors.Add("MessageId is missing");
            if (String.IsNullOrEmpty(SenderGuid)) errors.Add("SenderGuid is missing");
            
            if (errors.Count > 0)
            {
                Console.WriteLine("Message failed validation with the following errors:");
                foreach (string curr in errors) Console.WriteLine("  " + curr);
                return false;
            }
            else
            {
                return true;
            }
        }

        public override string ToString()
        {
            string ret = "";
            ret += Environment.NewLine;
            ret += " | " + Created.ToString("MM/dd/yyyy hh:mm:ss") + " " + SenderGuid + " -> " + RecipientGuid + Environment.NewLine;

            ret += " | ";
            if (!String.IsNullOrEmpty(Email)) ret += "Email " + Email + " ";
            if (!String.IsNullOrEmpty(ChannelGuid)) ret += "Channel " + ChannelGuid + " ";
            if (!String.IsNullOrEmpty(Command)) ret += "Command " + Command + " ";
            if (Success != null) ret += "Success " + Success + " ";
            if (SyncRequest != null) ret += "SyncRequest " + SyncRequest + " ";
            if (SyncResponse != null) ret += "SyncResponse " + SyncResponse + " ";
            ret += Environment.NewLine;
            
            if (Data != null)
            {
                string DataString = Data.ToString();
                ret += " | Data (" + DataString.Length + " bytes)";
                ret += Environment.NewLine + Environment.NewLine + DataString + Environment.NewLine;
            }
            else
            {
                ret += " | Data: (null)" + Environment.NewLine;
            }

            return ret;
        }

        #endregion
    }
}
