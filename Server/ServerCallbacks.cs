using BigQ.Core;
using BigQ.Server.Managers;
using SyslogLogging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WatsonTcp;
using WatsonWebsocket;

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
        /// </summary>
        public Func<Message, bool> MessageReceived;

        /// <summary>
        /// Delegate method called when the server stops.
        /// </summary>
        public Func<bool> ServerStopped;

        /// <summary>
        /// Delegate method called when a client connects to the server.
        /// </summary>
        public Func<ServerClient, bool> ClientConnected;

        /// <summary>
        /// Delegate method called when a client issues the login command.
        /// </summary>
        public Func<ServerClient, bool> ClientLogin;

        /// <summary>
        /// Delegate method called when the connection to the server is severed.
        /// </summary>
        public Func<ServerClient, bool> ClientDisconnected;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ServerCallbacks()
        {
            MessageReceived = null;
            ServerStopped = null;
            ClientConnected = null;
            ClientLogin = null;
            ClientDisconnected = null;
        }
         
        #endregion

        #region Public-Methods
         
        #endregion
         
        #region Private-Methods
         
        #endregion
    }
}
