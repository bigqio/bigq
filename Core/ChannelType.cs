using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization; 

namespace BigQ.Core
{
    /// <summary>
    /// The message distribution type of the channel.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ChannelType
    {
        [EnumMember(Value = "Broadcast")]
        Broadcast,
        [EnumMember(Value = "Unicast")]
        Unicast,
        [EnumMember(Value = "Multicast")]
        Multicast
    }
}
