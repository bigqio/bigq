using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mono.Data.Sqlite;

namespace BigQ
{
    public class BigQSqlite
    {
        #region Administrative

        public static bool CreateFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException("filename");
            
            using (SqliteConnection conn = new SqliteConnection("Data Source=" + filename + ";Version=3;"))
            {
                conn.Open();
                if (!File.Exists(filename))
                {
                    SqliteConnection.CreateFile(filename);
                    return true;
                }
                else
                {
                    return true;
                }
            }
        }

        public static bool SendQuery(string filename, string query, out DataTable result)
        {
            result = new DataTable();

            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException("filename");
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException("query");

            string connstr = "Data Source=" + filename + ";Version=3;";

            using (SqliteConnection conn = new SqliteConnection(connstr))
            {
                conn.Open();

                using (SqliteCommand cmd = new SqliteCommand(query, conn))
                {
                    using (SqliteDataReader rdr = cmd.ExecuteReader())
                    {
                        result.Load(rdr);
                        return true;
                    }
                }
            }
        }

        #endregion

        #region BigQ-Specific

        public static bool CreateDatabase()
        {
            if (!CreateFile("bigq.db"))
            {
                Console.WriteLine("*** Unable to create bigq.db");
                return false;
            }

            string ChannelsTableQuery =
                "CREATE TABLE IF NOT EXISTS channel " +
                "(" +
                " channel_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                " guid VARCHAR(64), " +
                " owner VARCHAR(64), " +
                " private INTEGER " +
                ")";

            DataTable result = null;
            if (!SendQuery("bigq.db", ChannelsTableQuery, out result))
            {
                Console.WriteLine("*** Unable to create channel table in bigq.db");
                return false;
            }

            string UsersTableQuery =
                "CREATE TABLE IF NOT EXISTS user " +
                "(" +
                " user_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                " guid VARCHAR(64), " +
                " email VARCHAR(64), " +
                " password VARCHAR(64) " +
                ")";

            if (!SendQuery("bigq.db", UsersTableQuery, out result))
            {
                Console.WriteLine("*** Unable to create user table in bigq.db");
                return false;
            }

            string JournalQuery =
                "PRAGMA journal_mode = TRUNCATE";

            if (!SendQuery("bigq.db", JournalQuery, out result))
            {
                Console.WriteLine("*** Unable to execute journal query in bigq.db");
                return false;
            }

            return true;
        }
        
        public static BigQClient GetUser(string email)
        {
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException("email");
            string query = "SELECT * FROM user WHERE email = '" + email + "'";
            DataTable result;
            if (!SendQuery("bigq.db", query, out result))
            {
                Console.WriteLine("*** Unable to execute query to retrieve user with email " + email);
                return null;
            }

            foreach (DataRow curr in result.Rows)
            {
                BigQClient client = new BigQClient();
                client.Email = email;
                client.Password = curr["password"].ToString();
                client.ClientGuid = curr["guid"].ToString();
                return client;
            }

            return null;
        }
        
        public static bool AuthenticateUser(string email, string password)
        {
            if (String.IsNullOrEmpty(email)) throw new ArgumentNullException("email");
            if (String.IsNullOrEmpty(password)) throw new ArgumentNullException("password");

            BigQClient client = GetUser(email);
            if (client == null)
            {
                Console.WriteLine("*** Unable to find user with email " + email);
                return false;
            }

            if (String.Compare(password, client.Password) == 0)
            {
                return true;
            }

            return false;
        }

        #endregion
    }
}