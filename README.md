# BigQ

[![][nuget-img]][nuget]

[nuget]:     https://www.nuget.org/packages/BigQ.dll
[nuget-img]: https://badge.fury.io/nu/Object.svg

## Messaging Platform in C#

For a sample app exercising bigq, please see: https://github.com/bigqio/chat

## Help or Feedback

First things first - do you need help or have feedback?  Contact me at joel dot christner at gmail dot com or file an issue here!

## New in v2.0.0

- Major refactor and simplification for better code manageability
- Separate logic for client and server functions, removed client dependencies within the server
- Enums where appropriate including client connection type and message commands
- Removed unnecessary configuration parameters and constructors
- Creation of a common Core library and separate client and server libraries
- Several minor bugfixes

## Description

BigQ is a messaging platform using TCP sockets and websockets (intentionally not using AMQP by design) featuring sync, async, channel, and private communications. BigQ is written in C# and made available under the MIT license.  BigQ is tested and compatible with Mono.

Core use cases for BigQ:
- Simple sockets wrapper with integrated framing - we make sockets programming easier
- Standard communication layer connecting apps through diverse transports including:
  - TCP
  - TCP with SSL
  - Websockets
  - Websockets with SSL
- Real-time messaging like chat applications
- Synchronous and asynchronous messaging
- Flexible message distribution options
  - Unicast node to node (private)
  - Multicast channels for publisher to multiple subscribers
  - Unicast channels for publisher to single subscriber
  - Broadcast channels for publisher to all members
- Cluster management
- Near real-time notifications and events

## Performance

Performance in BigQ is good, however, connection and channel management both have high overhead.  If you have a use case with lots of client joins/exits, BigQ may not be suitable for your environment.  We'd love your help in making BigQ more efficient!

## Persistence

Persistence is supported as an attribute of a directed asynchronous message.  If you use persistence, make sure you include ```sqlite3.dll``` in your program execution directory.  This is very important.

## Components

Two main components to BigQ: client and server.  The server can be run independently or instantiated within your own application.  Clients initiate connections to the server and maintain them to avoid issues with intermediary firewalls.  

## Starting the Server

Refer to the BigQServerTest project for a thorough example.
```
using BigQ.Core;
using BigQ.Server;
...
// start the server
Server server = new Server(null);		// with a default configuration
Server server = new Server("server.json");	// with a configuration file
Server server = new Server(serverConfig);	// with a configuration object

// set callbacks
server.Callbacks.MessageReceived = MessageReceived;		
server.Callbacks.ServerStopped = ServerStopped;				
server.Callbacks.ClientConnected = ClientConnected;
server.Callbacks.ClientLogin = ClientLogin;
server.Callbacks.ClientDisconnected = ClientDisconnected;
server.Callbacks.LogMessage = LogMessage;

// callback implementation, these methods should return true
static bool MessageReceived(Message msg) { ... }
static bool ClientConnected(Client client) { ... }
static bool ClientLogin(Client client) { ... }
static bool ClientDisconnected(Client client) { ... }
static bool LogMessage(string msg) { ... }
```

## Starting the Client

Refer to the BigQClientTest project for a thorough example.
```
using BigQ.Client;
using BigQ.Core;
...
// start the client and connect to the server
Client client = new Client(null);		// with a default configuration
Client client = new Client("client.json");	// with a configuration file
Client client = new Client(clientConfig);	// with a configuration object

// set callbacks
client.Callbacks.AsyncMessageReceived = AsyncMessageReceived;
client.Callbacks.SyncMessageReceived = SyncMessageReceived;
client.Callbacks.ServerConnected = ServerConnected;
client.Callbacks.ServerDisconnected = ServerDisconnected;
client.Callbacks.ClientJoinedServer = ClientJoinedServer;
client.Callbacks.ClientLeftServer = ClientLeftServer;
client.Callbacks.ClientJoinedChannel = ClientJoinedChannel;
client.Callbacks.ClientLeftChannel = ClientLeftChannel;
client.Callbacks.SubscriberJoinedChannel = SubscriberJoinedChannel;
client.Callbacks.SubscriberLeftChannel = SubscriberLeftChannel;
client.Callbacks.LogMessage = LogMessage;

// implement callbacks, these methods should return true
// sync message callback should return the data to be returned to requestor
static bool AsyncMessageReceived(Message msg) { ... }
static byte[] SyncMessageReceived(Message msg) { return Encoding.UTF8.GetBytes("Hello!"); }
static bool ServerConnected() { ... }
static bool ServerDisconnected() { ... }
static bool ClientJoinedServer(string clientGuid) { ... }
static bool ClientLeftServer(string clientGuid) { ... }
static bool ClientJoinedChannel(string clientGuid, string channelGuid) { ... }
static bool ClientLeftChannel(string clientGuid, string channelGuid) { ... }
static bool SubscriberJoinedChannel(string clientGuid, string channelGuid) { ... }
static bool SubscriberLeftChannel(string clientGuid, string channelGuid) { ... }
static bool LogMessage(string msg) { ... }

// login from the client
Message response;
if (!client.Login(out response)) { // handle failures }
```

## Unicast Messaging

Unicast messages are sent directly between clients without a channel
```
Message response;
List<ServerClient> clients;

// find a client to message
if (!client.ListClients(out response, out clients)) { // handle errors }

// private async message
// received by 'AsyncMessageReceived' on recipient
if (!client.SendPrivateMessageAsync(guid, msg)) { // handle errors }

// private async message with persistence
// received by 'AsyncMessageReceived' on recipient
if (!client.SendPrivateMessageAsync(guid, null, msg, true)) { // handle errors }

// private sync message
// received by 'SyncMessageReceived' on recipient client
// which should return response data
if (!client.SendPrivateMessageSync(guid, "Hello!", out response)) { // handle errors }
```

## Channel Messaging

Channel messages are sent to one or more channel members based on the type of channel

- Messages sent to a unicast channel are sent to a single random subscriber
- Messages sent to a multicast channel are sent to all members that are subscribers
- Messages sent to a broadcast channel are sent to all members whether they are subscribers or not
```
Message response;
List<Channel> channels;
List<ServerClient> clients;

// list and join or create a channel
if (!client.ListChannels(out response, out channels)) { // handle errors }
if (!client.JoinChannel(guid, out response)) { // handle errors }
if (!client.CreateUnicastChannel(name, 0, out response)) { // handle errors }
if (!client.CreateMulticastChannel(name, 0, out response)) { // handle errors }
if (!client.CreateBroadcastChannel(name, 0, out response)) { // handle errors }

// subscribe to a channel
if (!client.SubscribeChannel(guid, out response)) { // handle errors }

// send message to a channel
if (!client.SendChannelMessageSync(guid, "Hello!", out response)) { // handle errors }
if (!client.SendChannelMessageAsync(guid, "Hello!")) { // handle errors }

// leave a channel, unsubscribe, or delete it if yours
if (!client.LeaveChannel(guid, out response)) { // handle errors }
if (!client.UnsubscribeChannel(guid, out response)) { // handle errors }
if (!client.DeleteChannel(guid, out response)) { // handle errors }

// list channel members or subscribers
if (!client.ListChannelMembers(guid, out response, out clients)) { // handle errors }
if (!client.ListChannelSubscribers(guid, out response, out clients)) { // handle errors }
```

## Connecting using Websockets

Please refer to the sample test client or Javascript chat application on Github.

## Connecting using SSL

When connecting using SSL, if you are using self-signed certificates, be sure to set 'AcceptInvalidSSLCerts' to true in the config file on both the server and client.  Use PFX files for certificates.  Note that for Websockets and SSL, the certificate must be bound to the port in the operating system.

## Authorization

BigQ uses two filesystem files (defined in the server configuration file) to determine if messages should be authorized.  Please refer to the sample files in the project for their structure.  It is important to note that using this feature can and will affect performance.

## BigQ Framing

BigQ uses a simple framing mechanism that closely follows HTTP.  A set of headers start each message, with each header ending in a carriage return and newline ```\r\n```.  The headers contain a variety of metadata, and most importantly, ContentLength, which indicates how many bytes are to be read after the header delimiter.  The header delimiter is an additional carriage return and newline ```\r\n``` which follows the carriage return and newline of the final header.  The body is internally treated as a byte array so the connected clients will need to manage encoding.
```
Email: foo@bar.com
ContentType: application/json  
ContentLength: 22

{ first_name: 'joel' }
```

## Running under Mono

BigQ works well in Mono environments to the extent that we have tested it.  It is recommended that when running under Mono, you execute the containing EXE using --server and after using the Mono Ahead-of-Time Compiler (AOT).
```
mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server myapp.exe
mono --server myapp.exe
```

## Version History

Notes from previous versions (starting with v1.5.0) will be moved here.

v1.8.x
- Persistence support per message (async direct messages only)
- Better, faster disconnect detection
- Bugfixes and refactoring

v1.7.x
- update to support changes in WatsonWebsocket
- create and delete channels from the server
- further refactoring
- bugfixes
- performance improvements
- reduced CPU utilization

v1.6.x
- forced use of heartbeats, moved disconnect detect into heartbeat manager
- major refactor (connection manager, channel manager, variable naming consistency) 
- new events for public channel creation and destroy operations 
- many bugfixes

v1.5.x
- bugfix for disconnect detect
- synchronous and asynchronous channels (previously only async, APIs have changed)
- unicast channels (random single recipient)
- server channels (started on server startup)
- configurable server GUID (instead of hard-coded zero GUID)
- structured success, failure, and event messages (instead of string)
- numerous bugfixes