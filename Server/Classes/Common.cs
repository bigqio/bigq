using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQ.Server.Classes
{ 
    internal static class Common
    {
        internal static T DeserializeJson<T>(string json)
        {
            // Newtonsoft
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore;
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);

            // System.Web.Script.Serialization
            // JavaScriptSerializer ser = new JavaScriptSerializer();
            // ser.MaxJsonLength = Int32.MaxValue;
            // return (T)ser.Deserialize<T>(json);
        }

        internal static T DeserializeJson<T>(byte[] bytes)
        {
            // Newtonsoft
            JsonSerializerSettings settings = new JsonSerializerSettings();
            string json = Encoding.UTF8.GetString(bytes);
            return (T)Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, settings);

            // System.Web.Script.Serialization
            // JavaScriptSerializer ser = new JavaScriptSerializer();
            // ser.MaxJsonLength = Int32.MaxValue;
            // return (T)ser.Deserialize<T>(Encoding.UTF8.GetString(bytes));
        }

        internal static string SerializeJson(object obj)
        {
            // Newtonsoft
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.NullValueHandling = NullValueHandling.Ignore;
            string json = JsonConvert.SerializeObject(obj, Newtonsoft.Json.Formatting.Indented, settings);
            return json;

            // System.Web.Script.Serialization
            // JavaScriptSerializer ser = new JavaScriptSerializer();
            // ser.MaxJsonLength = Int32.MaxValue;
            // string json = ser.Serialize(obj);
            // return json;
        }

        internal static string BytesToHex(byte[] ba)
        {
            if (ba == null || ba.Length < 1) return null;
            string hex = BitConverter.ToString(ba);
            return hex.Replace("-", "");
        }

        internal static byte[] HexToBytes(string hex)
        {
            if (String.IsNullOrEmpty(hex)) return null;
            int numChars = hex.Length;
            byte[] bytes = new byte[numChars / 2];
            for (int i = 0; i < numChars; i += 2)
                bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
            return bytes;
        }
    }
}
