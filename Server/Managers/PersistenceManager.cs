using BigQ.Core;
using SqliteWrapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace BigQ.Server.Managers
{
    /// <summary>
    /// Manages retention of messages marked as persistent using Sqlite.
    /// </summary>
    internal class PersistenceManager : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private bool _Disposed = false;
         
        private ServerConfiguration _Config; 
        private DatabaseClient _Database;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the persistence manager.
        /// </summary> 
        /// <param name="config">ServerConfiguration instance.</param>
        public PersistenceManager(ServerConfiguration config)
        { 
            if (config == null) throw new ArgumentNullException(nameof(config)); 
             
            _Config = config;

            if (config.Persistence == null || !config.Persistence.EnablePersistence) return;
            if (String.IsNullOrEmpty(config.Files.PersistenceDatabaseFile)) return;
            if (config.Persistence.ExpirationIntervalMs < 1000) return;

            InitializeDatabase();
            Task.Run(() => ExpirationTask());
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down and dispose of background workers.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Store a message persistently.
        /// </summary>
        /// <param name="msg">The message to persist.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool PersistMessage(Message msg)
        {
            if (msg == null) return false;

            DateTime ts = DateTime.Now.ToUniversalTime();
            double createdTs = DateTimeToEpoch(ts);
            double expTs = DateTimeToEpoch(ts.AddSeconds(_Config.Persistence.MaxExpirationSeconds));

            if (msg.ExpirationUtc != null)
            {
                DateTime msgExpTs = Convert.ToDateTime(msg.ExpirationUtc);
                double msgExpEpoch = DateTimeToEpoch(msgExpTs);
                if (msgExpEpoch < expTs) expTs = msgExpEpoch;
            }

            string query = 
                "BEGIN TRANSACTION; " +
                "INSERT INTO Messages (" +
                "  CreatedUtc, " +
                "  ExpirationUtc, " +
                "  SenderGuid, " +
                "  SenderName, " +
                "  RecipientGuid, " +
                "  ChannelGuid, " +
                "  ChannelName, " +
                "  MessageId, " +
                "  ConversationId, " +
                "  MessageSeqnum, " +
                "  ContentType, " +
                "  ContentLength ";

            if (msg.UserHeaders != null && msg.UserHeaders.Count > 0)
                query += ",UserHeaders ";

            if (msg.Data != null && msg.Data.Length > 0)
                query += ",Data ";
            
            query += 
                ") " +
                "VALUES (" +
                "  '" + createdTs + "', " +
                "  '" + expTs + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.SenderGUID) + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.SenderName) + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.RecipientGUID) + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.ChannelGUID) + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.ChannelName) + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.MessageID) + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.ConversationID) + "', " +
                "  '" + msg.MessageSeqnum + "', " +
                "  '" + DatabaseClient.SanitizeString(msg.ContentType) + "', " +
                "  '" + msg.ContentLength + "' ";

            if (msg.UserHeaders != null && msg.UserHeaders.Count > 0)
                query += ",'" + EncodeHeaders(msg.UserHeaders) + "' ";

            if (msg.Data != null && msg.Data.Length > 0)
                query += ",X'" + EncodeData(msg.Data) + "' ";

            query +=
                "); " +
                "SELECT last_insert_rowid() AS Id;" +
                "COMMIT; ";

            DataTable result = _Database.Query(query);
            if (Helper.DataTableIsNullOrEmpty(result)) return false;

            return true;
        }

        /// <summary>
        /// Expire a message by its ID.
        /// </summary>
        /// <param name="id">The ID of the message.</param>
        public void ExpireMessage(int id)
        {
            if (id < 1) return;
            string query = "DELETE FROM Messages WHERE Id = " + id + "; ";
            DataTable result = _Database.Query(query);
            return;
        }

        /// <summary>
        /// Retrieve messages stored persistently for a given client GUID.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param>
        /// <param name="msgs">A dictionary containing key value pairs with the message ID as the key and the message as the value.</param>
        public void GetMessagesForRecipient(string guid, out Dictionary<int, Message> msgs)
        {
            msgs = new Dictionary<int, Message>();
            if (String.IsNullOrEmpty(guid)) return;

            string query =
                "SELECT * FROM Messages WHERE RecipientGuid = '" + guid + "' ORDER BY CreatedUtc ASC;";

            DataTable result = _Database.Query(query);
            if (Helper.DataTableIsNullOrEmpty(result)) return;
              
            foreach (DataRow curr in result.Rows)
            {
                Message m = null;
                int id = -1;
                if (!DataRowToMessage(curr, out m, out id)) continue; 
                msgs.Add(id, m);
            }
        }

        /// <summary>
        /// Retrieve the number of persistent messages stored.
        /// </summary>
        /// <returns>Integer representing the number of persistent messages stored.</returns>
        public int QueueDepth()
        {
            string query =
                "SELECT COUNT(*) AS NumMessages FROM Messages;";

            DataTable result = _Database.Query(query);
            if (!Helper.DataTableIsNullOrEmpty(result))
            {
                foreach (DataRow curr in result.Rows)
                {
                    return Convert.ToInt32(curr["NumMessages"]);
                }
            }

            return 0;
        }

        /// <summary>
        /// Retrieve the number of persistent messages stored for a client by GUID.  If no GUID is supplied, the queue depth for all clients is returned.
        /// </summary>
        /// <param name="guid">The GUID of the client.</param>
        /// <returns>Integer representing the number of persistent message stored.</returns>
        public int QueueDepth(string guid)
        {
            if (String.IsNullOrEmpty(guid)) return QueueDepth();

            string query =
                "SELECT COUNT(*) AS NumMessages FROM Messages WHERE RecipientGuid = '" + guid + "';";

            DataTable result = _Database.Query(query);
            if (!Helper.DataTableIsNullOrEmpty(result))
            {
                foreach (DataRow curr in result.Rows)
                {
                    return Convert.ToInt32(curr["NumMessages"]);
                }
            }

            return 0;
        }

        #endregion

        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            {
                // do work
            }

            _Disposed = true;
        }

        private void InitializeDatabase()
        {
            _Database = new DatabaseClient(_Config.Files.PersistenceDatabaseFile, _Config.Persistence.DebugDatabase);

            string createTableQuery =
                "CREATE TABLE IF NOT EXISTS Messages " +
                "(" +
                "  Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "  CreatedUtc INTEGER, " +
                "  ExpirationUtc INTEGER, " +
                "  SenderGuid VARCHAR(64), " +
                "  SenderName VARCHAR(128), " +
                "  RecipientGuid VARCHAR(64), " +
                "  ChannelGuid VARCHAR(64), " +
                "  ChannelName VARCHAR(128), " +
                "  MessageId VARCHAR(64), " +
                "  ConversationId VARCHAR(64), " +
                "  MessageSeqnum INTEGER, " +
                "  ContentType VARCHAR(64), " +
                "  ContentLength INTEGER, " +
                "  UserHeaders BLOB, " +
                "  Data BLOB" +
                ")";

            DataTable result = _Database.Query(createTableQuery);
        }

        private void ExpirationTask()
        {
            while (true)
            {
                Task.Delay(_Config.Persistence.ExpirationIntervalMs).Wait();
                double epoch = DateTimeToEpoch(DateTime.Now.ToUniversalTime());
                string cleanupQuery = "DELETE FROM Messages WHERE ExpirationUtc < '" + epoch + "'; ";
                DataTable result = _Database.Query(cleanupQuery);
            }
        }

        private string EncodeHeaders(Dictionary<string, string> headers)
        {
            if (headers == null || headers.Count < 1) return null;
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(Common.SerializeJson(headers)));
        }

        private Dictionary<string, string> DecodeHeaders(string headers)
        {
            Console.WriteLine("Headers: " + headers);
            if (String.IsNullOrEmpty(headers)) return null;
            return Common.DeserializeJson<Dictionary<string, string>>(Convert.FromBase64String(headers));
        }

        private string EncodeData(byte[] data)
        {
            if (data == null || data.Length < 1) return null;
            return Common.BytesToHex(data); 
        }
         
        private double DateTimeToEpoch(DateTime dt)
        {
            DateTime unixStart = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            long unixTimeStampInTicks = (dt.ToUniversalTime() - unixStart).Ticks;
            return (double)unixTimeStampInTicks / TimeSpan.TicksPerSecond;
        }

        private DateTime EpochToDateTime(double epoch)
        {
            DateTime unixStart = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            long unixTimeStampInTicks = (long)(epoch * TimeSpan.TicksPerSecond);
            return new DateTime(unixStart.Ticks + unixTimeStampInTicks, System.DateTimeKind.Utc);
        }

        private bool DataRowToMessage(DataRow row, out Message m, out int id)
        {
            m = new Message();
            id = -1;
            if (row == null) return false;

            id = Convert.ToInt32(row["Id"]);

            if (row["CreatedUtc"] != DBNull.Value)
                m.CreatedUtc = EpochToDateTime(Convert.ToDouble(row["CreatedUtc"]));

            if (row["ExpirationUtc"] != DBNull.Value)
                m.ExpirationUtc = EpochToDateTime(Convert.ToDouble(row["ExpirationUtc"]));

            if (row["SenderGuid"] != DBNull.Value)
                m.SenderGUID = row["SenderGuid"].ToString();

            if (row["SenderName"] != DBNull.Value)
                m.SenderName = row["SenderName"].ToString();

            if (row["RecipientGuid"] != DBNull.Value)
                m.RecipientGUID = row["RecipientGuid"].ToString();

            if (row["ChannelGuid"] != DBNull.Value)
                m.ChannelGUID = row["ChannelGuid"].ToString();

            if (row["ChannelName"] != DBNull.Value)
                m.ChannelName = row["ChannelName"].ToString();

            if (row["MessageId"] != DBNull.Value)
                m.MessageID = row["MessageId"].ToString();

            if (row["ConversationId"] != DBNull.Value)
                m.ConversationID = row["ConversationId"].ToString();

            if (row["MessageSeqnum"] != DBNull.Value)
                m.MessageSeqnum = Convert.ToInt64(row["MessageSeqnum"]);

            if (row["ContentType"] != DBNull.Value)
                m.ContentType = row["ContentType"].ToString();

            if (row["ContentLength"] != DBNull.Value)
                m.ContentLength = Convert.ToInt64(row["ContentLength"]);

            if (row["UserHeaders"] != DBNull.Value)
                m.UserHeaders = DecodeHeaders(row["UserHeaders"].ToString());

            if (row["Data"] != DBNull.Value)
                m.Data = (byte[])(row["Data"]);

            m.Persist = true;

            return true;
        }

        #endregion
    }
}
