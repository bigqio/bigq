using System; 
using System.Threading.Tasks;

using BigQ.Core;

namespace BigQ.Server
{
    /// <summary>
    /// BigQ server callbacks object.
    /// </summary>
    public class ServerCallbacks
    {
        #region Public-Members

        /// <summary>
        /// Delegate method called when the server receives a message from a connected client.
        /// The message received is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<Message, Task> MessageReceived = null;

        /// <summary>
        /// Delegate method called when the server stops.
        /// A response of true is expected.
        /// </summary>
        public Func<Task> ServerStopped = null;

        /// <summary>
        /// Delegate method called when a client connects to the server.
        /// The client object is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<ServerClient, Task> ClientConnected = null;

        /// <summary>
        /// Delegate method called when a client issues the login command.
        /// The client object is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<ServerClient, Task> ClientLogin = null;

        /// <summary>
        /// Delegate method called when the connection to the server is severed.
        /// The client object is the first parameter.
        /// A response of true is expected.
        /// </summary>
        public Func<ServerClient, Task> ClientDisconnected = null;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ServerCallbacks()
        { 
        }
         
        #endregion

        #region Public-Methods
         
        #endregion
         
        #region Private-Methods
         
        #endregion
    }
}
