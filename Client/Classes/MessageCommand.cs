using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace BigQ.Client.Classes
{ 
    [JsonConverter(typeof(StringEnumConverter))]
    public enum MessageCommand
    {
        [EnumMember(Value = "Echo")]
        Echo,
        [EnumMember(Value = "Login")]
        Login,
        [EnumMember(Value = "Event")]
        Event,
        [EnumMember(Value = "HeartbeatRequest")]
        HeartbeatRequest,
        [EnumMember(Value = "JoinChannel")]
        JoinChannel,
        [EnumMember(Value = "LeaveChannel")]
        LeaveChannel,
        [EnumMember(Value = "SubscribeChannel")]
        SubscribeChannel,
        [EnumMember(Value = "UnsubscribeChannel")]
        UnsubscribeChannel,
        [EnumMember(Value = "CreateChannel")]
        CreateChannel,
        [EnumMember(Value = "DeleteChannel")]
        DeleteChannel,
        [EnumMember(Value = "ListClients")]
        ListClients,
        [EnumMember(Value = "ListChannels")]
        ListChannels,
        [EnumMember(Value = "ListChannelMembers")]
        ListChannelMembers,
        [EnumMember(Value = "ListChannelSubscribers")]
        ListChannelSubscribers,
        [EnumMember(Value = "IsClientConnected")]
        IsClientConnected,
        [EnumMember(Value = "MessagePrivate")]
        MessagePrivate,
        [EnumMember(Value = "MessageChannel")]
        MessageChannel,
        [EnumMember(Value = "Unknown")]
        Unknown
    }
}
