using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using Newtonsoft.Json;

namespace BigQ
{
    public class BigQHelper
    {
        public static bool SocketWrite(TcpClient Client, byte[] Data)
        {
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** SocketWrite null client supplied");
                    return false;
                }

                if (Data == null || Data.Length < 1)
                {
                    Log("*** SocketWrite null data supplied");
                    return false;
                }

                #endregion

                #region Process

                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;

                Client.GetStream().Write(Data, 0, Data.Length);
                Client.GetStream().Flush();
                return true;

                #endregion
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** SocketWrite " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** SocketWrite " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** SocketWrite " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** SocketWrite " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** SocketWrite " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("SocketWrite " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
        }

        public static bool SocketRead(TcpClient Client, out byte[] Data)
        {
            Data = null;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** SocketRead null client supplied");
                    return false;
                }

                #endregion

                #region Variables

                int BytesRead = 0;
                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;
                NetworkStream ClientStream = Client.GetStream();

                #endregion

                #region Read-from-Stream

                if (!IsPeerConnected(Client))
                {
                    // Log(SourceIp + ":" + SourcePort + " disconnected");
                    return false;
                }

                byte[] buffer = new byte[2048];
                using (MemoryStream ms = new MemoryStream())
                {
                    int read;

                    while ((read = ClientStream.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        // Log("Read " + read + " bytes from stream from peer " + SourceIp + ":" + SourcePort);
                        ms.Write(buffer, 0, read);
                        BytesRead += read;

                        // if (read < buffer.Length) break;
                        if (!ClientStream.DataAvailable) break;
                    }

                    Data = ms.ToArray();
                }

                if (Data == null || Data.Length < 1)
                {
                    // Log("No data read from " + SourceIp + ":" + SourcePort);
                    return false;
                }

                #endregion

                // Log("Returning " + Data.Length + " bytes from " + SourceIp + ":" + SourcePort);
                return true;
            }
            catch (ObjectDisposedException ObjDispInner)
            {
                Log("*** SocketRead " + SourceIp + ":" + SourcePort + " disconnected (obj disposed exception): " + ObjDispInner.Message);
                return false;
            }
            catch (SocketException SockInner)
            {
                Log("*** SocketRead " + SourceIp + ":" + SourcePort + " disconnected (socket exception): " + SockInner.Message);
                return false;
            }
            catch (InvalidOperationException InvOpInner)
            {
                Log("*** SocketRead " + SourceIp + ":" + SourcePort + " disconnected (invalid operation exception): " + InvOpInner.Message);
                return false;
            }
            catch (IOException IOInner)
            {
                Log("*** SocketRead " + SourceIp + ":" + SourcePort + " disconnected (IO exception): " + IOInner.Message);
                return false;
            }
            catch (Exception EInner)
            {
                Log("*** SocketRead " + SourceIp + ":" + SourcePort + " disconnected (general exception): " + EInner.Message);
                LogException("SocketRead " + SourceIp + ":" + SourcePort, EInner);
                return false;
            }
        }

        public static object BytesToObject(byte[] Data)
        {
            using (var ms = new MemoryStream())
            {
                var bf = new BinaryFormatter();
                ms.Write(Data, 0, Data.Length);
                ms.Seek(0, SeekOrigin.Begin);
                var obj = bf.Deserialize(ms);
                return obj;
            }
        }

        public static byte[] ObjectToBytes(object obj)
        {
            if (obj == null) return null;
            if (obj is byte[]) return (byte[])obj;

            BinaryFormatter bf = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }

        public static bool IsPeerConnected(TcpClient Client)
        {
            // see http://stackoverflow.com/questions/6993295/how-to-determine-if-the-tcp-is-connected-or-not

            bool success = false;
            string SourceIp = "";
            int SourcePort = 0;

            try
            {
                #region Check-for-Null-Values

                if (Client == null)
                {
                    Log("*** IsPeerConnected null client supplied");
                    return false;
                }

                #endregion

                #region Check-if-Client-Connected

                success = false;
                SourceIp = ((IPEndPoint)Client.Client.RemoteEndPoint).Address.ToString();
                SourcePort = ((IPEndPoint)Client.Client.RemoteEndPoint).Port;

                if (Client != null
                    && Client.Client != null
                    && Client.Client.Connected)
                {
                    if (Client.Client.Poll(0, SelectMode.SelectRead))
                    {
                        byte[] buff = new byte[1];
                        if (Client.Client.Receive(buff, SocketFlags.Peek) == 0) success = false;
                        else success = true;
                    }

                    success = true;
                }
                else
                {
                    success = false;
                }

                return success;

                #endregion
            }
            catch
            {
                return false;
            }
            finally
            {
                if (Client != null)
                {
                    // Log("Client " + SourceIp + ":" + SourcePort + " connected: " + success);
                }
                else
                {
                    // Log("Client null");
                }
            }
        }

        public static T DeserializeJson<T>(string json, bool debug)
        {
            if (debug)
            {
                Log("");
                Log("DeserializeJson input:");
                Log(json);
                Log("");
            }

            Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings();
            settings.NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore;
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);
        }

        public static T DeserializeJson<T>(byte[] bytes, bool debug)
        {
            if (debug)
            {
                Log("");
                Log("DeserializeJson input:");
                Log(Encoding.UTF8.GetString(bytes));
                Log("");
            }

            Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings();
            string json = Encoding.UTF8.GetString(bytes);
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);
        }

        public static T JObjectToObject<T>(object obj)
        {
            Newtonsoft.Json.Linq.JObject jobject = (Newtonsoft.Json.Linq.JObject)obj;
            T ret = jobject.ToObject<T>();
            return ret;
        }

        public static T JArrayToList<T>(object obj)
        {
            // 
            // Call with List<T> as the type, i.e.
            // foo = JArrayToList<List<foo>>(data);
            //

            if (obj == null) throw new ArgumentNullException("obj");
            Newtonsoft.Json.Linq.JArray jarray = (Newtonsoft.Json.Linq.JArray)obj;
            return jarray.ToObject<T>();
        }

        public static string SerializeJson<T>(T obj)
        {
            string json = JsonConvert.SerializeObject(obj, Newtonsoft.Json.Formatting.Indented, new JsonSerializerSettings { });
            return json;
        }

        public static T CopyObject<T>(T source)
        {
            string json = SerializeJson<T>(source);
            T ret = DeserializeJson<T>(json, false);
            return ret;
        }

        public static void Log(string message)
        {
            Console.WriteLine(message);
        }

        public static void LogException(string method, Exception e)
        {
            Log("================================================================================");
            Log(" = Method: " + method);
            Log(" = Exception Type: " + e.GetType().ToString());
            Log(" = Exception Data: " + e.Data);
            Log(" = Inner Exception: " + e.InnerException);
            Log(" = Exception Message: " + e.Message);
            Log(" = Exception Source: " + e.Source);
            Log(" = Exception StackTrace: " + e.StackTrace);
            Log("================================================================================");
        }

        public static bool IsTrue(int? val)
        {
            if (val == null) return false;
            if (Convert.ToInt32(val) == 1) return true;
            return false;
        }

        public static bool IsTrue(int val)
        {
            if (val == 1) return true;
            return false;
        }

        public static bool IsTrue(bool val)
        {
            return val;
        }

        public static bool IsTrue(bool? val)
        {
            if (val == null) return false;
            return Convert.ToBoolean(val);
        }

        public static bool IsTrue(string val)
        {
            if (String.IsNullOrEmpty(val)) return false;
            val = val.ToLower().Trim();
            int val_int = 0;
            if (Int32.TryParse(val, out val_int)) if (val_int == 1) return true;
            if (String.Compare(val, "true") == 0) return true;
            return false;
        }
    }
}
