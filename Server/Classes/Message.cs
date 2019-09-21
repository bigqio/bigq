using System;
using System.Collections.Generic;
using System.Text;

namespace BigQ.Server.Classes
{
    /// <summary>
    /// Object containing metadata and data from a message sent from one client to another on BigQ.
    /// </summary>
    [Serializable]
    public class Message
    {
        #region Public-Members
         
        /// <summary>
        /// Email address of the client.  Primarily used in authentication (future).
        /// </summary>
        public string Email;

        /// <summary>
        /// Password of the client.  Primarily used in authentication (future).
        /// </summary>
        public string Password;
        
        /// <summary>
        /// Command issued by the sender.  Primarily used in messages directed toward the server or events.
        /// </summary>
        public MessageCommand Command;

        /// <summary>
        /// Timestamp indicating when the message was created.
        /// </summary>
        public DateTime CreatedUtc;

        /// <summary>
        /// Contained in a response message to indicate if the request message was successful.
        /// </summary>
        public bool Success;

        /// <summary>
        /// Set by the sender to indicate if the message should be handled synchronously by the receiver (i.e. the sender is blocking while waiting for a response).
        /// </summary>
        public bool SyncRequest;

        /// <summary>
        /// Set by the recipient to indicate that the message is a response to a synchronous request message.
        /// </summary>
        public bool SyncResponse;
        
        /// <summary>
        /// The amount of time in milliseconds to wait to receive a response to this message, if synchronous.  This parameter will override the value in the client configuration. 
        /// </summary>
        public int SyncTimeoutMs;

        /// <summary>
        /// Indicates whether or not the message should be stored persistently if unable to immediately deliver.
        /// </summary>
        public bool Persist;

        /// <summary>
        /// Timestamp indicating when the persistent message should expire.
        /// </summary>
        public DateTime? ExpirationUtc;

        /// <summary>
        /// Unique identifier for the message.
        /// </summary>
        public string MessageID;

        /// <summary>
        /// Reserved for future use.
        /// </summary>
        public string ConversationID;

        /// <summary>
        /// Reserved for future use.
        /// </summary>
        public long MessageSeqnum;

        /// <summary>
        /// Unique identifier for the sender.
        /// </summary>
        public string SenderGUID;

        /// <summary>
        /// The name of the sender.
        /// </summary>
        public string SenderName;

        /// <summary>
        /// Unique identifier for the recipient.
        /// </summary>
        public string RecipientGUID;

        /// <summary>
        /// Unique identifier for the channel.
        /// </summary>
        public string ChannelGUID;

        /// <summary>
        /// The name of the channel.
        /// </summary>
        public string ChannelName;

        /// <summary>
        /// Dictionary containing key/value pairs for user-supplied headers (anything starting with x-).
        /// </summary>
        public Dictionary<string, string> UserHeaders = new Dictionary<string, string>();

        /// <summary>
        /// Contains the content-type of the message data; specified by the sender.
        /// </summary>
        public string ContentType;

        /// <summary>
        /// Contains the number of bytes in the data payload.
        /// </summary>
        public long ContentLength;

        /// <summary>
        /// The data payload.
        /// </summary>
        public byte[] Data;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public Message()
        {

        }

        /// <summary>
        /// Converts a byte array to a populated BigQMessage object.
        /// </summary>
        /// <param name="bytes">The byte array containing the message data.</param>
        public Message(byte[] bytes)
        {
            #region Check-for-Null-Values

            if (bytes == null || bytes.Length < 1) throw new ArgumentNullException(nameof(bytes));

            #endregion
             
            #region Parse-to-String-Array

            string headerString = Encoding.UTF8.GetString(bytes);  
            string[] lines = headerString.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.None); 

            if (lines == null || lines.Length < 1) throw new ArgumentException("Unable to derive headers from supplied data");
            int numLines = lines.Length;

            #endregion

            #region Initialize-Values

            Email = null;
            Password = null;
            Command = MessageCommand.Unknown;
            CreatedUtc = DateTime.Now.ToUniversalTime();
            UserHeaders = new Dictionary<string, string>();
            Data = null;

            #endregion

            #region Process-Headers

            for (int i = 0; i < numLines; i++)
            {
                #region Initialize

                string currLine = lines[i]; 
                if (String.IsNullOrEmpty(currLine)) break; 

                #endregion

                #region Variables

                int headerLength;
                int colonPosition; 
                string key = "";
                string val = "";
                
                #endregion

                #region Parse-into-Key-Value-Pair
                
                if (currLine.Contains(":"))
                {
                    headerLength = currLine.Length;
                    colonPosition = currLine.IndexOf(":");
                    key = currLine.Substring(0, colonPosition).Trim();
                    val = currLine.Substring((colonPosition + 1), (headerLength - colonPosition - 1)).Trim();
                }
                else
                {
                    key = currLine.Trim();
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
                        Command = (MessageCommand)Enum.Parse(typeof(MessageCommand), val);
                        break;

                    case "createdutc":
                        try
                        {
                            CreatedUtc = DateTime.Parse(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("CreatedUtc was unable to be parsed; use MM/dd/yyyy HH:mm:ss.fffffff");
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

                    case "synctimeoutms":
                        try
                        {
                            SyncTimeoutMs = Convert.ToInt32(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("SyncTimeoutMs must be of form convertible to integer.");
                        }
                        break;

                    case "persist":
                        try
                        {
                            Persist = Convert.ToBoolean(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("Persist must be of form convertible to boolean");
                        }
                        break;

                    case "expirationutc":
                        try
                        {
                            ExpirationUtc = DateTime.Parse(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("ExpirationUtc was unable to be parsed; use MM/dd/yyyy HH:mm:ss.fffffff");
                        }
                        break;

                    case "messageid":
                        MessageID = val;
                        break;

                    case "conversationid":
                        ConversationID = val;
                        break;

                    case "messagesequencenumber":
                        try
                        {
                            MessageSeqnum = Convert.ToInt64(val);
                        }
                        catch (Exception)
                        {
                            throw new ArgumentException("MessageSequenceNumber must be of form convertible to long");
                        }
                        break;

                    case "senderguid":
                        SenderGUID = val;
                        break;

                    case "sendername":
                        SenderName = val;
                        break;

                    case "recipientguid":
                        RecipientGUID = val;
                        break;

                    case "channelguid":
                        ChannelGUID = val;
                        break;

                    case "channelname":
                        ChannelName = val;
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

            #region Get-Data

            if (ContentLength > 0 && ContentLength < bytes.Length)
            {
                Data = new byte[ContentLength];
                Buffer.BlockCopy(bytes, (int)(bytes.Length - ContentLength), Data, 0, (int)ContentLength);
            }
             
            #endregion
        }
        
        #endregion

        #region Public-Methods
         
        /// <summary>
        /// Creates a formatted string containing information about the message.
        /// </summary>
        /// <returns>A formatted string containing information about the message.</returns>
        public override string ToString()
        {
            string ret = ""; 
            ret += " | Created " + (CreatedUtc != null ? Convert.ToDateTime(CreatedUtc).ToString("MM/dd/yyyy HH:mm:ss") : "null") + 
                " Expiration " + (ExpirationUtc != null ? Convert.ToDateTime(ExpirationUtc).ToString("MM/dd/yyyy HH:mm:ss") : "null") + Environment.NewLine;

            ret += " | Message ID " + (!String.IsNullOrEmpty(MessageID) ? MessageID : "null") +
                " Conversation ID " + (!String.IsNullOrEmpty(ConversationID) ? ConversationID : "null") + Environment.NewLine;

            ret += " | Command " + Command.ToString() + 
                " Success " + Success + Environment.NewLine;

            ret += " | Sync Request " + SyncRequest + 
                " Sync Timeout " + SyncTimeoutMs + 
                " Sync Response " + SyncResponse + 
                " Persist " + Persist + Environment.NewLine;

            ret += " | Sender Name " + SenderName + " Sender GUID " + SenderGUID + Environment.NewLine;

            ret += " | Recipient GUID " + RecipientGUID + Environment.NewLine;

            ret += " | Channel GUID " + (!String.IsNullOrEmpty(ChannelGUID) ? ChannelGUID : "null") + 
                " Channel Name " + (!String.IsNullOrEmpty(ChannelName) ? ChannelName : "null") + Environment.NewLine;

            ret += " | Content Type " + (!String.IsNullOrEmpty(ContentType) ? ContentType : "null") + " Content Length " + ContentLength + Environment.NewLine;
            
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
                if (ContentLength > 0)
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

        /// <summary>
        /// Creates a byte array that can be transmitted to a stream (such as a socket) from a populated BigQMessage object.
        /// </summary>
        /// <returns>A byte array that can be transmitted to a stream.</returns>
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
            
            string sanitizedCreatedUtc = Convert.ToDateTime(CreatedUtc).ToUniversalTime().ToString("MM/dd/yyyy HH:mm:ss.fffffff");
            headerSb.Append("CreatedUtc: " + sanitizedCreatedUtc);
            headerSb.Append("\r\n");

            headerSb.Append("Success: " + Success);
            headerSb.Append("\r\n");

            headerSb.Append("SyncRequest: " + SyncRequest);
            headerSb.Append("\r\n");

            headerSb.Append("SyncResponse: " + SyncResponse);
            headerSb.Append("\r\n");

            headerSb.Append("SyncTimeoutMs: " + SyncTimeoutMs);
            headerSb.Append("\r\n");

            headerSb.Append("Persist: " + Persist);
            headerSb.Append("\r\n");

            headerSb.Append("Command: " + Command.ToString());
            headerSb.Append("\r\n");

            if (ExpirationUtc != null)
            {
                headerSb.Append("ExpirationUtc: " + Convert.ToDateTime(ExpirationUtc).ToUniversalTime().ToString("MM/dd/yyyy HH:mm:ss.ffffff"));
                headerSb.Append("\r\n");
            }
            
            if (!String.IsNullOrEmpty(MessageID))
            {
                string sanitizedMessageId;
                if (SanitizeString(MessageID, out sanitizedMessageId))
                {
                    headerSb.Append("MessageId: " + sanitizedMessageId);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(ConversationID))
            {
                string sanitizedConversationId;
                if (SanitizeString(ConversationID, out sanitizedConversationId))
                {
                    headerSb.Append("ConversationId: " + sanitizedConversationId);
                    headerSb.Append("\r\n");
                }
            } 

            headerSb.Append("MessageSequenceNumber: " + MessageSeqnum);
            headerSb.Append("\r\n"); 

            if (!String.IsNullOrEmpty(SenderGUID))
            {
                string sanitizedSenderGuid;
                if (SanitizeString(SenderGUID, out sanitizedSenderGuid))
                {
                    headerSb.Append("SenderGUID: " + sanitizedSenderGuid);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(SenderName))
            {
                string sanitizedSenderName;
                if (SanitizeString(SenderName, out sanitizedSenderName))
                {
                    headerSb.Append("SenderName: " + sanitizedSenderName);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(RecipientGUID))
            {
                string sanitizedRecipientGuid;
                if (SanitizeString(RecipientGUID, out sanitizedRecipientGuid))
                {
                    headerSb.Append("RecipientGUID: " + sanitizedRecipientGuid);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(ChannelGUID))
            {
                string sanitizedChannelGuid;
                if (SanitizeString(ChannelGUID, out sanitizedChannelGuid))
                {
                    headerSb.Append("ChannelGUID: " + sanitizedChannelGuid);
                    headerSb.Append("\r\n");
                }
            }

            if (!String.IsNullOrEmpty(ChannelName))
            {
                string sanitizedChannelName;
                if (SanitizeString(ChannelName, out sanitizedChannelName))
                {
                    headerSb.Append("ChannelName: " + sanitizedChannelName);
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
            
            if (Data != null && Data.Length > 0)
            {
                headerSb.Append("ContentLength: " + Data.Length);
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
        
        /// <summary>
        /// Redacts credentials from a message.
        /// </summary> 
        public Message Redact()
        {
            Email = null;
            Password = null;
            return this;
        }

        #endregion

        #region Private-Methods

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
