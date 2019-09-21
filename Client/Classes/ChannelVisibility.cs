using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization; 

namespace BigQ.Client.Classes
{
    /// <summary>
    /// The visibility of the channel.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ChannelVisibility
    {
        /// <summary>
        /// Public channel.  Channel will appear in list channels responses.
        /// </summary>
        [EnumMember(Value = "Public")]
        Public,
        /// <summary>
        /// Private channel.  Channel will not appear in list channels responses, except for the owner.
        /// </summary>
        [EnumMember(Value = "Private")]
        Private
    }
}
