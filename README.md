![alt tag](https://raw.githubusercontent.com/bigqio/bigq/master/Assets/logo.png)

# BigQ

[![NuGet](https://img.shields.io/nuget/v/FeatureToggle.svg)](https://www.nuget.org/packages/BigQ.Server) BigQ.Server on NuGet

[![NuGet](https://img.shields.io/nuget/v/FeatureToggle.svg)](https://www.nuget.org/packages/BigQ.Client) BigQ.Client on NuGet

BigQ is a essaging platform in C# for TCP, SSL, and Websockets, with unicast, broadcast, multicast, publisher/subscriber messaging and integrated framing, built for both .NET Core and .NET Framework.

## New in v3.0.x

- Task-based callbacks and front-end APIs in both client and server
- Reduced class members, code reduction and cleanup
- Better handling of disconnected clients and connection termination
- Better CLI experience
- Better protection of class members (internal vs public)
- Enums for channel visibility and message distribution type
- Eliminated Core library dependency and reduced Common static methods
- Dependency update for bugfixes and reliability

## Help or Feedback

First things first - do you need help or have feedback?  Contact me at joel dot christner at gmail dot com or file an issue here!

## Description

BigQ is a messaging platform using TCP sockets and websockets.  BigQ intentionally avoids AMQP in favor of a framing protocol similar to HTTP and features sync, async, channel, and private communications. BigQ is written in C# and made available under the MIT license.  BigQ is tested and compatible with .NET Core, .NET Framework, and .NET Framework on Mono.

Core use cases for BigQ:
- Simple sockets wrapper with integrated framing - we make sockets programming easier
- Standard communication layer connecting apps through diverse transports including:
  - TCP, with or without SSL
  - Websockets, with or without SSL
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

Performance in BigQ is good, however, connection and channel management both have high overhead.  If you have a use case with lots of client joins/exits, or channel/creates/destroys, BigQ may not be suitable for your environment.  We'd love your help in making BigQ more efficient.  As of now, client join/exit and channel create/destroy can occur roughly every two to three seconds.

## Components

Two main components to BigQ: client and server.  The server can be run independently or instantiated within your own application.  Clients initiate connections to the server and maintain them to avoid issues with intermediary firewalls.  

## Starting the Server

Refer to the ```ServerTest``` project for a thorough example.
```
using BigQ.Server;
...
// start the server
Server server = new Server(null);		// with a default configuration
Server server = new Server("server.json");	// with a configuration file
Server server = new Server(serverConfig);	// with a configuration object

// set callbacks
server.Callbacks.ServerStopped = ServerStopped;				
server.Callbacks.MessageReceived = MessageReceived;		
server.Callbacks.ClientConnected = ClientConnected;
server.Callbacks.ClientLogin = ClientLogin;
server.Callbacks.ClientDisconnected = ClientDisconnected;
server.Callbacks.LogMessage = LogMessage;

// callback implementations
static async Task ServerStopped() { ... }
static async Task MessageReceived(Message msg) { ... }
static async Task ClientConnected(ServerClient client) { ... }
static async Task ClientLogin(ServerClient client) { ... }
static async Task ClientDisconnected(Client client) { ... } 
```

## Starting the Client

Refer to the ```ClientTest``` project for a thorough example.
```
using BigQ.Client; 
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
client.Callbacks.ChannelCreated = ChannelCreated;
client.Callbacks.ChannelDestroyed = ChannelDestroyed;

// callback implementations
static async Task AsyncMessageReceived(Message msg) { ... }
static async Task<byte[]> SyncMessageReceived(Message msg) { return Encoding.UTF8.GetBytes("Hello!"); }
static async Task ServerConnected() { ... }
static async Task ServerDisconnected() { ... }
static async Task ClientJoinedServer(string clientGuid) { ... }
static async Task ClientLeftServer(string clientGuid) { ... }
static async Task ClientJoinedChannel(string clientGuid, string channelGuid) { ... }
static async Task ClientLeftChannel(string clientGuid, string channelGuid) { ... }
static async Task SubscriberJoinedChannel(string clientGuid, string channelGuid) { ... }
static async Task SubscriberLeftChannel(string clientGuid, string channelGuid) { ... } 
static async Task ChannelCreated(string channelGuid) { ... }
static async Task ChannelDestroyed(string channelGuid) { ... }

// login from the client
Message response;
if (!client.Login(out response)) { // handle failures }
```

## Unicast Messaging

Unicast messages are sent directly between clients without a channel
```
Message response;

// find a client to message
List<ServerClient> clients = client.ListClients(); 

// private message (received by 'AsyncMessageReceived' on recipient)
await client.SendPrivateMessageAsync("[guid]", "Hi there!");

// private async message with persistence
await client.SendPrivateMessageAsync("[guid]", null, "Hi there!", true);

// private sync message (received by 'SyncMessageReceived' on recipient)
string resp = null;
if (!client.SendPrivateMessageSync("[guid]", "Hello!", out resp)) { // handle errors }
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
channels = client.ListChannels(); 
if (!client.Join("[guid]", out response)) { // handle errors }
if (!client.CreateChannel(ChannelType.Broadcast, "My Channel", false, out response)) { // handle errors }

// subscribe to a channel
if (!client.Subscribe("[guid]", out response)) { // handle errors }
if (!client.Unsubscribe("[guid")) { // handle errors }

// list channel members or subscribers
clients = client.ListMembers("[guid]");
clients = client.ListSubscribers("[guid]");

// send message to a channel
if (!client.SendChannel("[guid]", "Hello!")) { // handle errors }
string resp = null;
if (!client.SendChannelSync("[guid]", "Hello!", out resp)) { // handle errors }

// leave a channel, unsubscribe, or delete it if yours
if (!client.Leave("[guid]", out response)) { // handle errors }
if (!client.Unsubscribe("[guid]", out response)) { // handle errors }
if (!client.DeleteChannel("[guid]", out response)) { // handle errors }

```

## Connecting using Websockets

Please refer to the sample test client or Javascript chat application on Github.

## Connecting using SSL

When connecting using SSL, if you are using self-signed certificates, be sure to set ```AcceptInvalidSSLCerts``` to true in the configuration on both the server and client.  Use PFX files for certificates.  Note that for Websockets and SSL, the certificate must be bound to the port in the operating system and installed in the certificate store under the 'computer' account (not the 'personal' account).

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

While BigQ works best in cross-platform environments using .NET Core, it also works well in Mono environments (with .NET Framework) to the extent that we have tested it.  It is recommended that when running under Mono, you execute the containing EXE using --server and after using the Mono Ahead-of-Time Compiler (AOT).
```
mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server myapp.exe
mono --server myapp.exe
```

## Version History

Notes from previous versions can be found in CHANGELOG.md.
