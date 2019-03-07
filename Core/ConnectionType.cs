using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace BigQ.Core
{
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
