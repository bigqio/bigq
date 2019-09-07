using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization; 

namespace BigQ.Server.Classes
{
    /// <summary>
    /// The visibility of the channel.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ChannelVisibility
    {
        [EnumMember(Value = "Public")]
        Public,
        [EnumMember(Value = "Private")]
        Private
    }
}
