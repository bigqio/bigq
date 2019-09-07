using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace BigQ.Client.Classes
{
    /// <summary>
    /// The type of connection.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ConnectionType
    {
        [EnumMember(Value = "Tcp")]
        Tcp,
        [EnumMember(Value = "TcpSsl")]
        TcpSsl,
        [EnumMember(Value = "Websocket")]
        Websocket,
        [EnumMember(Value = "WebsocketSsl")]
        WebsocketSsl
    }
}
