using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization; 

namespace BigQ.Server.Classes
{
    /// <summary>
    /// The message distribution type of the channel.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ChannelType
    {
        /// <summary>
        /// Broadcast channel.  Messages are sent to everyone.
        /// </summary>
        [EnumMember(Value = "Broadcast")]
        Broadcast,
        /// <summary>
        /// Unicast channel.  Messages are sent to a single subscriber.
        /// </summary>
        [EnumMember(Value = "Unicast")]
        Unicast,
        /// <summary>
        /// Multicast channel.  Messages are sent to all subscribers.
        /// </summary>
        [EnumMember(Value = "Multicast")]
        Multicast
    }
}
