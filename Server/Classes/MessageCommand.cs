using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace BigQ.Server.Classes
{ 
    /// <summary>
    /// Message commands.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum MessageCommand
    {
        /// <summary>
        /// Echo the request back to the sender.
        /// </summary>
        [EnumMember(Value = "Echo")]
        Echo,
        /// <summary>
        /// Login message.
        /// </summary>
        [EnumMember(Value = "Login")]
        Login,
        /// <summary>
        /// An event has fired.
        /// </summary>
        [EnumMember(Value = "Event")]
        Event,
        /// <summary>
        /// Heartbeat request.
        /// </summary>
        [EnumMember(Value = "HeartbeatRequest")]
        HeartbeatRequest,
        /// <summary>
        /// Join the specified channel.
        /// </summary>
        [EnumMember(Value = "JoinChannel")]
        JoinChannel,
        /// <summary>
        /// Leave the specified channel.
        /// </summary>
        [EnumMember(Value = "LeaveChannel")]
        LeaveChannel,
        /// <summary>
        /// Subscribe to the specified channel.
        /// </summary>
        [EnumMember(Value = "SubscribeChannel")]
        SubscribeChannel,
        /// <summary>
        /// Unsubscribe from the specified channel.
        /// </summary>
        [EnumMember(Value = "UnsubscribeChannel")]
        UnsubscribeChannel,
        /// <summary>
        /// Create a channel.
        /// </summary>
        [EnumMember(Value = "CreateChannel")]
        CreateChannel,
        /// <summary>
        /// Delete a channel.
        /// </summary>
        [EnumMember(Value = "DeleteChannel")]
        DeleteChannel,
        /// <summary>
        /// List clients connected to the server.
        /// </summary>
        [EnumMember(Value = "ListClients")]
        ListClients,
        /// <summary>
        /// List channels visible to me.
        /// </summary>
        [EnumMember(Value = "ListChannels")]
        ListChannels,
        /// <summary>
        /// List members of a channel.
        /// </summary>
        [EnumMember(Value = "ListChannelMembers")]
        ListChannelMembers,
        /// <summary>
        /// List subscribers of a channel.
        /// </summary>
        [EnumMember(Value = "ListChannelSubscribers")]
        ListChannelSubscribers,
        /// <summary>
        /// Determine if a client is connected.
        /// </summary>
        [EnumMember(Value = "IsClientConnected")]
        IsClientConnected,
        /// <summary>
        /// Send a private message.
        /// </summary>
        [EnumMember(Value = "MessagePrivate")]
        MessagePrivate,
        /// <summary>
        /// Send a message to a channel.
        /// </summary>
        [EnumMember(Value = "MessageChannel")]
        MessageChannel,
        /// <summary>
        /// Unknown.
        /// </summary>
        [EnumMember(Value = "Unknown")]
        Unknown
    }
}
