using System;

namespace BigQ.Core
{
    /// <summary>
    /// Message builder.
    /// </summary>
    public class MessageBuilder
    {
        #region Public-Members

        public string ServerGUID { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        public MessageBuilder(string serverGuid)
        {
            if (String.IsNullOrEmpty(serverGuid)) throw new ArgumentNullException(nameof(serverGuid));

            ServerGUID = serverGuid;
        }

        #endregion

        #region Public-Methods

        #region Administrative-and-Authorization

        public Message LoginRequired()
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = false;
            ResponseMessage.SyncRequest = false;
            ResponseMessage.SyncResponse = false;
            ResponseMessage.Data = FailureData.ToBytes(ErrorTypes.LoginRequired, "Login required", null);
            return ResponseMessage;
        }

        public Message AuthorizationFailed(Message currentMessage)
        {
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncRequest = false;
            currentMessage.SyncResponse = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.AuthorizationFailed, "Authorization failed", null);
            return currentMessage;
        }

        public Message HeartbeatRequest(ServerClient currentClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.MessageID = Guid.NewGuid().ToString();
            ResponseMessage.RecipientGUID = currentClient.ClientGUID;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.Command = MessageCommand.HeartbeatRequest;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Data = null;
            return ResponseMessage;
        }

        public Message ServerJoinEvent(ServerClient newClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = false;
            ResponseMessage.SyncResponse = false;
            ResponseMessage.Command = MessageCommand.Event;
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientJoinedServer, newClient.ClientGUID);
            return ResponseMessage;
        }

        public Message ServerLeaveEvent(ServerClient leavingClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = false;
            ResponseMessage.SyncResponse = false;
            ResponseMessage.Command = MessageCommand.Event;
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientLeftServer, leavingClient.ClientGUID);
            return ResponseMessage;
        }

        #endregion

        #region Errors

        public Message UnknownCommand(ServerClient currentClient, Message currentMessage)
        {
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnknownCommand, "Unknown command", currentMessage.Command);
            return currentMessage;
        }

        public Message RecipientNotFound(ServerClient currentClient, Message currentMessage)
        {
            string originalRecipientGUID = currentMessage.RecipientGUID;
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;

            if (!String.IsNullOrEmpty(originalRecipientGUID))
            {
                currentMessage.Data = FailureData.ToBytes(ErrorTypes.RecipientNotFound, "Unknown recipient", originalRecipientGUID);
            }
            else if (!String.IsNullOrEmpty(currentMessage.ChannelGUID))
            {
                currentMessage.Data = FailureData.ToBytes(ErrorTypes.ChannelNotFound, "Unknown channel", currentMessage.ChannelGUID);
            }
            else
            {
                currentMessage.Data = FailureData.ToBytes(ErrorTypes.BadRequest, "No recipient or channel supplied", null);
            }
            return currentMessage;
        }

        public Message NotChannelMember(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.NotAChannelMember, "You are not a member of this channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message DataError(ServerClient currentClient, Message currentMessage, string message)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.DataError, "Data error encountered", message);
            return currentMessage;
        }

        #endregion

        #region Queue-Acknowledgements

        public Message MessageQueueSuccess(ServerClient currentClient, Message currentMessage)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;

            if (!String.IsNullOrEmpty(currentMessage.RecipientGUID))
            {
                #region Individual-Recipient

                currentMessage.Data = SuccessData.ToBytes("Message queued to recipient", currentMessage.RecipientGUID);
                return currentMessage;

                #endregion
            }
            else if (!String.IsNullOrEmpty(currentMessage.ChannelGUID))
            {
                #region Channel-Recipient

                currentMessage.Data = SuccessData.ToBytes("Message queued to channel", currentMessage.ChannelGUID);
                return currentMessage;

                #endregion
            }
            else
            {
                #region Unknown-Recipient

                return RecipientNotFound(currentClient, currentMessage);

                #endregion
            }
        }

        public Message MessageQueueFailure(ServerClient currentClient, Message currentMessage)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToQueue, "Unable to queue message", null);
            return currentMessage;
        }

        #endregion

        #region Channel-Messages

        public Message ChannelNotFound(ServerClient currentClient, Message currentMessage)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.ChannelNotFound, "Channel not found", currentMessage.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelNoMembers(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.NoChannelMembers, "No members in channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelNoSubscribers(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.NoChannelSubscribers, "No subscribers in channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelAlreadyExists(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.ChannelAlreadyExists, "Channel already exists", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelCreateSuccess(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = SuccessData.ToBytes("Channel created successfully", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelCreateFailure(ServerClient currentClient, Message currentMessage)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToCreateChannel, "Unable to create channel", null);
            return currentMessage;
        }

        public Message ChannelJoinSuccess(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = SuccessData.ToBytes("Successfully joined channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelLeaveSuccess(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = SuccessData.ToBytes("Successfully left channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelSubscribeSuccess(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = SuccessData.ToBytes("Successfully subscribed to channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelUnsubscribeSuccess(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = SuccessData.ToBytes("Successfully unsubscribed from channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelJoinFailure(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToJoinChannel, "Unable to join channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelLeaveFailure(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToLeaveChannel, "Unable to leave channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelSubscribeFailure(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToSubscribeChannel, "Unable to subscribe to channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelUnsubscribeFailure(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToUnsubscribeChannel, "Unable to unsubscribe from channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelDeletedByOwner(ServerClient currentClient, Channel currentChannel)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = currentClient.ClientGUID;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = currentChannel.ChannelGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncResponse = ResponseMessage.SyncRequest;
            ResponseMessage.SyncRequest = false;
            ResponseMessage.Data = SuccessData.ToBytes("Channel deleted by owner", currentChannel.ChannelGUID);
            return ResponseMessage;
        }

        public Message ChannelDeleteSuccess(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = true;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = SuccessData.ToBytes("Successfully deleted channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelDeleteFailure(ServerClient currentClient, Message currentMessage, Channel currentChannel)
        {
            currentMessage = currentMessage.Redact();
            currentMessage.RecipientGUID = currentMessage.SenderGUID;
            currentMessage.SenderGUID = ServerGUID;
            currentMessage.ChannelGUID = currentChannel.ChannelGUID;
            currentMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            currentMessage.Success = false;
            currentMessage.SyncResponse = currentMessage.SyncRequest;
            currentMessage.SyncRequest = false;
            currentMessage.Data = FailureData.ToBytes(ErrorTypes.UnableToDeleteChannel, "Unable to delete channel", currentChannel.ChannelGUID);
            return currentMessage;
        }

        public Message ChannelJoinEvent(Channel currentChannel, ServerClient newClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = currentChannel.ChannelGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = false;
            ResponseMessage.SyncResponse = false;
            ResponseMessage.Command = MessageCommand.Event;
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientJoinedChannel, newClient.ClientGUID);
            return ResponseMessage;
        }

        public Message ChannelLeaveEvent(Channel currentChannel, ServerClient leavingClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = currentChannel.ChannelGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.Command = MessageCommand.Event;
            ResponseMessage.Data = EventData.ToBytes(EventTypes.ClientLeftChannel, leavingClient.ClientGUID);
            return ResponseMessage;
        }

        public Message ChannelSubscriberJoinEvent(Channel currentChannel, ServerClient newClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = currentChannel.ChannelGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.SyncRequest = false;
            ResponseMessage.SyncResponse = false;
            ResponseMessage.Command = MessageCommand.Event;
            ResponseMessage.Data = EventData.ToBytes(EventTypes.SubscriberJoinedChannel, newClient.ClientGUID);
            return ResponseMessage;
        }

        public Message ChannelSubscriberLeaveEvent(Channel currentChannel, ServerClient leavingClient)
        {
            Message ResponseMessage = new Message();
            ResponseMessage.RecipientGUID = null;
            ResponseMessage.SenderGUID = ServerGUID;
            ResponseMessage.ChannelGUID = currentChannel.ChannelGUID;
            ResponseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            ResponseMessage.Success = true;
            ResponseMessage.Command = MessageCommand.Event;
            ResponseMessage.Data = EventData.ToBytes(EventTypes.SubscriberLeftChannel, leavingClient.ClientGUID);
            return ResponseMessage;
        }

        public Message ChannelCreateEvent(ServerClient currentClient, Channel currentChannel)
        {
            Message responseMessage = new Message();
            responseMessage.RecipientGUID = currentClient.ClientGUID;
            responseMessage.SenderGUID = ServerGUID;
            responseMessage.ChannelGUID = currentChannel.ChannelGUID;
            responseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            responseMessage.Success = true;
            responseMessage.SyncResponse = false;
            responseMessage.SyncRequest = false;
            responseMessage.Command = MessageCommand.Event;
            responseMessage.Data = EventData.ToBytes(EventTypes.ChannelCreated, currentChannel.ChannelGUID);
            return responseMessage;
        }

        public Message ChannelDestroyEvent(ServerClient currentClient, Channel currentChannel)
        {
            Message responseMessage = new Message();
            responseMessage.RecipientGUID = currentClient.ClientGUID;
            responseMessage.SenderGUID = ServerGUID;
            responseMessage.ChannelGUID = currentChannel.ChannelGUID;
            responseMessage.CreatedUtc = DateTime.Now.ToUniversalTime();
            responseMessage.Success = true;
            responseMessage.SyncResponse = false;
            responseMessage.SyncRequest = false;
            responseMessage.Command = MessageCommand.Event;
            responseMessage.Data = EventData.ToBytes(EventTypes.ChannelDestroyed, currentChannel.ChannelGUID);
            return responseMessage;
        }

        #endregion

        #endregion

        #region Private-Methods

        #endregion
    }
}
