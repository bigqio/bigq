using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQ
{
    [Serializable]
    public class BigQMessage
    {
        #region Class-Variables

        //
        //
        // Standard Headers
        //
        //
        public string Email;
        public string Password;
        public string Command;                          // used for server messagin
        public DateTime? CreatedUTC;                     // message timestamp in UTC time
        public bool? Success;                           // set by receiver when responding
        public bool? SyncRequest;                       // set by receiver when sending
        public bool? SyncResponse;                      // set by receiver when responding
        public string MessageId;                        // GUID
        public string ConversationId;                   // can be used for grouping messages into conversation
        public long? MessageSequenceNumber;              // can be used to indicate message ordering
        public string SenderGuid;                       // sender's GUID
        public string RecipientGuid;                    // recipient's GUID
        public string ChannelGuid;                      // channel's GUID or null
        public Dictionary<string, string> UserHeaders;  // anything starting with x-
        public string ContentType;
        public long? ContentLength;
        public byte[] Data;

        #endregion

        #region Constructors

        public BigQMessage()
        {

        }

        public BigQMessage(byte[] bytes)
        {
            //
            //
            // used by MessageRead to populate metadata fields
            //
            //

            #region Check-for-Null-Values

            if (bytes == null || bytes.Length < 1) throw new ArgumentNullException("bytes");

            #endregion

            #region Parse-to-String-Array

            string headerString = Encoding.UTF8.GetString(bytes);
            string[] headers = headerString.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.None);

            if (headers == null || headers.Length < 1) throw new ArgumentException("Unable to derive headers from supplied data");

            #endregion

            #region Initialize-Values
            
            Email = null;
            Password = null;
            Command = null;
            CreatedUTC = DateTime.Now.ToUniversalTime();
            Success = null;
            SyncRequest = null;
            SyncResponse = null;
            MessageId = null;
            ConversationId = null;
            MessageSequenceNumber = null;
            SenderGuid = null;
            RecipientGuid = null;
            ChannelGuid = null;
            UserHeaders = new Dictionary<string, string>();
            ContentType = null;
            ContentLength = null;
            Data = null;

            #endregion

            #region Process

            foreach (string header in headers)
            {
                #region Check-for-Null-Values

                if (String.IsNullOrEmpty(header)) continue;

                #endregion

                #region Variables

                int headerLength;
                int colonPosition; 
                string key = "";
                string val = "";
                
                #endregion

                #region Parse-into-Key-Value-Pair
                
                if (header.Contains(":"))
                {
                    headerLength = header.Length;
                    colonPosition = header.IndexOf(":");
                    key = header.Substring(0, colonPosition).Trim();
                    val = header.Substring((colonPosition + 1), (headerLength - colonPosition - 1)).Trim();
                }
                else
                {
                    key = header.Trim();
                    val = null;
                }

                #endregion

                #region Populate-Header-Fields

                if (key.StartsWith("x-"))
                {
                    #region User-Headers

                    if (!UserHeaders.ContainsKey(key)) UserHeaders.Add(key, val);

                    #endregion
                }
                switch (key.ToLower())
                {
                    #region Standard-Headers

                    case "email":
                        Email = val;
                        break;

                    case "password":
                        Password = val;
                        break;

                    case "command":
                        Command = val;
                        break;

                    case "createdutc":
                        try
                        {
                            CreatedUTC = DateTime.ParseExact(val, "MM/dd/yyyy HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("CreatedUTC must be in the format MM/dd/yyyy HH:mm:ss.fffffff");
                        }
                        break;

                    case "success":
                        try
                        {
                            Success = Convert.ToBoolean(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("Success must be of form convertible to boolean");
                        }
                        break;

                    case "syncrequest":
                        try
                        {
                            SyncRequest = Convert.ToBoolean(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("SyncRequest must be of form convertible to boolean");
                        }
                        break;

                    case "syncresponse":
                        try
                        {
                            SyncResponse = Convert.ToBoolean(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("SyncResponse must be of form convertible to boolean");
                        }
                        break;

                    case "messageid":
                        MessageId = val;
                        break;

                    case "conversationid":
                        ConversationId = val;
                        break;

                    case "messagesequencenumber":
                        try
                        {
                            MessageSequenceNumber = Convert.ToInt64(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("MessageSequenceNumber must be of form convertible to long");
                        }
                        break;

                    case "senderguid":
                        SenderGuid = val;
                        break;

                    case "recipientguid":
                        RecipientGuid = val;
                        break;

                    case "channelguid":
                        ChannelGuid = val;
                        break;

                    case "contenttype":
                        ContentType = val;
                        break;

                    case "contentlength":
                        try
                        {
                            ContentLength = Convert.ToInt64(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("ContentLength must be of form convertible to long");
                        }
                        break;

                    default:
                        if (!UserHeaders.ContainsKey(key)) UserHeaders.Add(key, val);
                        break;

                    #endregion
                }

                #endregion
            }

            #endregion
        }
        
        #endregion

        #region Public-Instance-Methods

        public bool IsValid()
        {
            List<string> errors = new List<string>();
            if (String.IsNullOrEmpty(MessageId)) errors.Add("MessageId is missing");
            if (String.IsNullOrEmpty(SenderGuid)) errors.Add("SenderGuid is missing");
            if (CreatedUTC == null) errors.Add("CreatedUTC is missing");

            if (Data != null)
            {
                if (ContentLength == null)
                {
                    errors.Add("ContentLength is missing");
                }
                else
                {
                    if (Data.Length != ContentLength) errors.Add("ContentLength does not match data length");
                }
            }

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

            if (!String.IsNullOrEmpty(MessageId))
            {
                ret += " | Message ID " + MessageId + " Created " + Convert.ToDateTime(CreatedUTC).ToString("MM/dd/yyyy HH:mm:ss.fffffff") + Environment.NewLine;
            }

            if (!String.IsNullOrEmpty(ChannelGuid))
            {
                ret += " | " + SenderGuid + " -> Channel " + ChannelGuid + Environment.NewLine;
            }
            else
            {
                ret += " | " + SenderGuid + " -> " + RecipientGuid + Environment.NewLine;
            }
            
            if (!String.IsNullOrEmpty(Email)
                || !String.IsNullOrEmpty(Command)
                || !String.IsNullOrEmpty(ContentType)
                || Success != null
                || SyncRequest != null
                || SyncResponse != null
                )
            {
                ret += " | ";
                if (!String.IsNullOrEmpty(Email)) ret += "Email " + Email + " ";
                if (!String.IsNullOrEmpty(Command)) ret += "Command " + Command + " ";
                if (!String.IsNullOrEmpty(ContentType)) ret += "Content Type " + ContentType + " ";
                if (Success != null) ret += "Success " + Success + " ";
                if (SyncRequest != null) ret += "SyncRequest " + SyncRequest + " ";
                if (SyncResponse != null) ret += "SyncResponse " + SyncResponse + " ";
                ret += Environment.NewLine;
            }
            
            if (UserHeaders != null)
            {
                if (UserHeaders.Count > 0)
                {
                    ret += " | User Headers: " + Environment.NewLine;
                    foreach (KeyValuePair<string, string> currUserHeader in UserHeaders)
                    {
                        ret += "   | " + currUserHeader.Key + ": " + currUserHeader.Value + Environment.NewLine;
                    }
                }
            }

            if (Data != null)
            {
                if (ContentLength != null)
                {
                    string DataString = Encoding.UTF8.GetString(Data);
                    ret += " | Data (" + ContentLength + " bytes)";
                    ret += Environment.NewLine + Environment.NewLine + DataString + Environment.NewLine;
                }
                else
                {
                    string DataString = Encoding.UTF8.GetString(Data);
                    ret += " | Data (no content length specified)";
                    ret += Environment.NewLine + Environment.NewLine + DataString + Environment.NewLine;
                }
            }
            else
            {
                ret += " | Data: (null)" + Environment.NewLine;
            }

            return ret;
        }

        public byte[] ToBytes()
        {
            #region Variables

            StringBuilder headerSb = new StringBuilder();
            string headerString = "";
            byte[] headerBytes;
            byte[] messageAsBytes;

            #endregion

            #region Build-Header

            if (!String.IsNullOrEmpty(Email))
            {
                string sanitizedEmail;
                if (SanitizeString(Email, out sanitizedEmail))
                {
                    headerSb.Append("Email: " + sanitizedEmail);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(Password))
            {
                string sanitizedPassword;
                if (SanitizeString(Password, out sanitizedPassword))
                {
                    headerSb.Append("Password: " + sanitizedPassword);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(Command))
            {
                string sanitizedCommand;
                if (SanitizeString(Command, out sanitizedCommand))
                {
                    headerSb.Append("Command: " + sanitizedCommand);
                    headerSb.Append("\r\n");
                }
            }

            if (CreatedUTC != null)
            {
                string sanitizedCreatedUTC = Convert.ToDateTime(CreatedUTC).ToUniversalTime().ToString("MM/dd/yyyy HH:mm:ss.fffffff");
                headerSb.Append("CreatedUTC: " + sanitizedCreatedUTC);
                headerSb.Append("\r\n");
            }

            if (Success != null)
            {
                headerSb.Append("Success: " + BigQHelper.IsTrue(Success));
                headerSb.Append("\r\n");
            }

            if (SyncRequest != null)
            {
                headerSb.Append("SyncRequest: " + BigQHelper.IsTrue(SyncRequest));
                headerSb.Append("\r\n");
            }

            if (SyncResponse != null)
            {
                headerSb.Append("SyncResponse: " + BigQHelper.IsTrue(SyncResponse));
                headerSb.Append("\r\n");
            }

            if (!String.IsNullOrEmpty(Command))
            {
                string sanitizedCommand;
                if (SanitizeString(Command, out sanitizedCommand))
                {
                    headerSb.Append("Command: " + sanitizedCommand);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(Command))
            {
                string sanitizedCommand;
                if (SanitizeString(Command, out sanitizedCommand))
                {
                    headerSb.Append("Command: " + sanitizedCommand);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(MessageId))
            {
                string sanitizedMessageId;
                if (SanitizeString(MessageId, out sanitizedMessageId))
                {
                    headerSb.Append("MessageId: " + sanitizedMessageId);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(ConversationId))
            {
                string sanitizedConversationId;
                if (SanitizeString(ConversationId, out sanitizedConversationId))
                {
                    headerSb.Append("ConversationId: " + sanitizedConversationId);
                    headerSb.Append("\r\n");
                }
            }

            if (MessageSequenceNumber != null)
            {
                headerSb.Append("MessageSequenceNumber: " + MessageSequenceNumber);
                headerSb.Append("\r\n");
            }

            if (!String.IsNullOrEmpty(SenderGuid))
            {
                string sanitizedSenderGuid;
                if (SanitizeString(SenderGuid, out sanitizedSenderGuid))
                {
                    headerSb.Append("SenderGuid: " + sanitizedSenderGuid);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(RecipientGuid))
            {
                string sanitizedRecipientGuid;
                if (SanitizeString(RecipientGuid, out sanitizedRecipientGuid))
                {
                    headerSb.Append("RecipientGuid: " + sanitizedRecipientGuid);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(ChannelGuid))
            {
                string sanitizedChannelGuid;
                if (SanitizeString(ChannelGuid, out sanitizedChannelGuid))
                {
                    headerSb.Append("ChannelGuid: " + sanitizedChannelGuid);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(ContentType))
            {
                string sanitizedContentType;
                if (SanitizeString(ContentType, out sanitizedContentType))
                {
                    headerSb.Append("ContentType: " + sanitizedContentType);
                    headerSb.Append("\r\n");
                }
            }

            if (ContentLength != null)
            {
                headerSb.Append("ContentLength: " + ContentLength);
                headerSb.Append("\r\n");
            }

            if (UserHeaders != null)
            {
                if (UserHeaders.Count > 0)
                {
                    foreach (KeyValuePair<string, string> curr in UserHeaders)
                    {
                        if (String.IsNullOrEmpty(curr.Key)) continue;
                        if (!curr.Key.ToLower().StartsWith("x-")) continue;

                        string sanitizedKey = "";
                        if (SanitizeString(curr.Key, out sanitizedKey))
                        {
                            string sanitizedValue = "";
                            if (SanitizeString(curr.Value, out sanitizedValue))
                            {
                                headerSb.Append(sanitizedKey + ": " + sanitizedValue);
                                headerSb.Append("\r\n");
                            }
                        }
                    }
                }
            }

            headerSb.Append("\r\n");    // end of headers
            headerString = headerSb.ToString();
            headerBytes = Encoding.UTF8.GetBytes(headerString);

            #endregion

            #region Build-Data

            if (Data == null)
            {
                messageAsBytes = new byte[(headerBytes.Length)];
                Buffer.BlockCopy(headerBytes, 0, messageAsBytes, 0, headerBytes.Length);
            }
            else
            {
                messageAsBytes = new byte[(headerBytes.Length + Data.Length)];
                Buffer.BlockCopy(headerBytes, 0, messageAsBytes, 0, headerBytes.Length);
                Buffer.BlockCopy(Data, 0, messageAsBytes, (headerBytes.Length), Data.Length);
            }
            
            #endregion

            return messageAsBytes;
        }

        #endregion

        #region Private-Utility-Methods

        private bool SanitizeString(string dirty, out string clean)
        {
            clean = null;
            if (String.IsNullOrEmpty(dirty)) return true;

            // null, below ASCII range, above ASCII range
            for (int i = 0; i < dirty.Length; i++)
            {
                if (((int)(dirty[i]) == 0) ||    // null
                    ((int)(dirty[i]) < 32) ||    // below ASCII range
                    ((int)(dirty[i]) > 126)      // above ASCII range
                    )
                {
                    continue;
                }
                else
                {
                    clean += dirty[i];
                }
            }

            return true;
        }

        #endregion
    }
}
