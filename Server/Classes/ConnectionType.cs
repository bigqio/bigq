using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace BigQ.Server.Classes
{
    /// <summary>
    /// The type of connection.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ConnectionType
    {
        /// <summary>
        /// Connected via TCP without SSL.
        /// </summary>
        [EnumMember(Value = "Tcp")]
        Tcp,
        /// <summary>
        /// Connected via TCP with SSL.
        /// </summary>
        [EnumMember(Value = "TcpSsl")]
        TcpSsl,
        /// <summary>
        /// Connected via Websocket without SSL.
        /// </summary>
        [EnumMember(Value = "Websocket")]
        Websocket,
        /// <summary>
        /// Connected via Websocket with SSL.
        /// </summary>
        [EnumMember(Value = "WebsocketSsl")]
        WebsocketSsl
    }
}
