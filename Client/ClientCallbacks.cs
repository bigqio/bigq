using System;  
using System.Threading.Tasks; 

using BigQ.Core;

namespace BigQ.Client
{
    /// <summary>
    /// BigQ client callbacks object.
    /// </summary>
    [Serializable]
    public class ClientCallbacks
    {
        #region Public-Members

        /// <summary>
        /// Delegate method called when an asynchronous message is received.
        /// The message is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<Message, Task> AsyncMessageReceived = null;

        /// <summary>
        /// Delegate method called when a synchronous message is received.
        /// The message is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<Message, Task<byte[]>> SyncMessageReceived = null;

        /// <summary>
        /// Delegate method called when the server connection is severed.
        /// A response of true is expected.
        /// </summary>
        public Func<Task> ServerDisconnected = null;

        /// <summary>
        /// Delegate method called when the server connection is restored.
        /// A response of true is expected.
        /// </summary>
        public Func<Task> ServerConnected = null;

        /// <summary>
        /// Delegate method called when a client joins the server.
        /// The client GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, Task> ClientJoinedServer = null;

        /// <summary>
        /// Delegate method called when a client leaves the server.
        /// The client GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, Task> ClientLeftServer = null;

        /// <summary>
        /// Delegate method called when a client joins a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, Task> ClientJoinedChannel = null;

        /// <summary>
        /// Delegate method called when a client leaves a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, Task> ClientLeftChannel = null;

        /// <summary>
        /// Delegate method called when a subscriber joins a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, Task> SubscriberJoinedChannel = null;

        /// <summary>
        /// Delegate method called when a subscriber leaves a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, Task> SubscriberLeftChannel = null;

        /// <summary>
        /// Delegate method called when a public channel is created.
        /// The channel GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, Task> ChannelCreated = null;

        /// <summary>
        /// Delegate method called when a public channel is destroyed.
        /// The channel GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, Task> ChannelDestroyed = null;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public ClientCallbacks()
        { 
        }

        #endregion

        #region Public-Methods
        
        #endregion

        #region Private-Methods
         
        #endregion
    }
}
