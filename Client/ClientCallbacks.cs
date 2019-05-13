using BigQ.Core;
using Newtonsoft.Json.Linq; 
using System; 
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WatsonTcp;
using WatsonWebsocket;

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
        public Func<Message, bool> AsyncMessageReceived;

        /// <summary>
        /// Delegate method called when a synchronous message is received.
        /// The message is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<Message, byte[]> SyncMessageReceived;

        /// <summary>
        /// Delegate method called when the server connection is severed.
        /// A response of true is expected.
        /// </summary>
        public Func<bool> ServerDisconnected;

        /// <summary>
        /// Delegate method called when the server connection is restored.
        /// A response of true is expected.
        /// </summary>
        public Func<bool> ServerConnected;

        /// <summary>
        /// Delegate method called when a client joins the server.
        /// The client GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, bool> ClientJoinedServer;

        /// <summary>
        /// Delegate method called when a client leaves the server.
        /// The client GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, bool> ClientLeftServer;

        /// <summary>
        /// Delegate method called when a client joins a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, bool> ClientJoinedChannel;

        /// <summary>
        /// Delegate method called when a client leaves a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, bool> ClientLeftChannel;

        /// <summary>
        /// Delegate method called when a subscriber joins a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, bool> SubscriberJoinedChannel;

        /// <summary>
        /// Delegate method called when a subscriber leaves a channel.
        /// The client GUID is the first parameter.
        /// The channel GUID is the second parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, string, bool> SubscriberLeftChannel;

        /// <summary>
        /// Delegate method called when a public channel is created.
        /// The channel GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, bool> ChannelCreated;

        /// <summary>
        /// Delegate method called when a public channel is destroyed.
        /// The channel GUID is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<string, bool> ChannelDestroyed;
         
        #endregion

        #region Private-Members

        #endregion
         
        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public ClientCallbacks()
        {
            AsyncMessageReceived = null;
            SyncMessageReceived = null;
            ServerDisconnected = null;
            ServerConnected = null;
            ClientJoinedServer = null;
            ClientLeftServer = null;
            ClientJoinedChannel = null;
            ClientLeftChannel = null;
            SubscriberJoinedChannel = null;
            SubscriberLeftChannel = null;
            ChannelCreated = null;
            ChannelDestroyed = null; 
        }

        #endregion

        #region Public-Methods
        
        #endregion

        #region Private-Methods
         
        #endregion
    }
}
