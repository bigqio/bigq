# bigq
messaging platform in C#

For a sample app exercising bigq, please see: https://github.com/bigqio/chat

## help or feedback
first things first - do you need help or have feedback?  Contact me at joel at maraudersoftware.com dot com or file an issue here!

## description
bigq is a messaging platform using TCP sockets and websockets (intentionally not using AMQP by design) featuring sync, async, channel, and private communications. bigq is written in C# and made available under the MIT license.

Core use cases for bigq:
- simple sockets wrapper - we make sockets programming easier
- connecting TCP-based applications with those limited to use of websockets
- real-time messaging like chat applications
- message distribution/publisher-subscriber using channels
- cluster management
- notifications and events

## performance
bigq is still early in development.  While we have high aspirations on performance, it's not there yet.  The software has excellent stability in lower throughput environments with lower rates of network change (adds, removes).  Performance will be a focus area in the coming releases.

## components
Two main components to bigq: client and server.  The server can be run independently or instantiated within your own application.  Clients initiate connections to the server and maintain them to avoid issues with intermediary firewalls.  

## starting the server
Refer to the BigQServerTest project for a thorough example.
```
using BigQ;
...
//
// start the server
//
BigQServer server = new BigQServer(null, 8000, false, false, true, true, 0);

//
// set callbacks
//
server.MessageReceived = MessageReceived;		
server.ServerStopped = ServerStopped;				
server.ClientConnected = ClientConnected;
server.ClientLogin = ClientLogin;
server.ClientDisconnected = ClientDisconnected;
server.LogMessage = LogMessage;

//
// callback implementation, these methods should return true
//
static bool MessageReceived(BigQMessage msg) { ... }
static bool ClientConnected(BigQClient client) { ... }
static bool ClientLogin(BigQClient client) { ... }
static bool ClientDisconnected(BigQClient client) { ... }
static bool LogMessage(string msg) { ... }
```

## starting the client
Refer to the BigQClientTest project for a thorough example.
```
using BigQ;

//
// connect to server
//
BigQClient client = new BigQClient(null, null, "127.0.0.1", 8000, 10000, 0, false);

//
// set callbacks
//
client.AsyncMessageReceived = AsyncMessageReceived;
client.SyncMessageReceived = SyncMessageReceived;
client.ServerDisconnected = ServerDisconnected;
client.LogMessage = LogMessage;

//
// implement callbacks, these methods should return true
// sync message callback should return the data to be returned to requestor
//
static bool AsyncMessageReceived(BigQMessage msg) { ... }
static byte[] SyncMessageReceived(BigQMessage msg) { return Encoding.UTF8.GetBytes("Hello!"); }
static bool ServerDisconnected() { ... }
static bool LogMessage(string msg) { ... }

//
// login
//
BigQMessage response;
if (!client.Login(out response)) { // handle failures }
```

## sending message to another client
```
BigQMessage response;
List<BigQClient> clients;

// 
// find a client to message
//
if (!client.ListClients(out response, out clients)) { // handle errors }

//
// private async message
// received by 'AsyncMessageReceived' on recipient
//
if (!client.SendPrivateMessageAsync(guid, msg)) { // handle errors }

//
// private sync message
// received by 'SyncMessageReceived' on recipient client
// which should return response data
//
if (!client.SendPrivateMessageSync(guid, "Hello!", out response)) { // handle errors }
```

## managing channels on the client
```
BigQMessage response;
List<BigQChannel> channels;
List<BigQClient> clients;

//
// list channels
//
if (!client.ListChannels(out response, out channels)) { // handle errors }

// 
// create or join a channel
//
if (!client.CreateChannel(guid, false, out response)) { // handle errors }
if (!client.JoinChannel(guid, out response)) { // handle errors }

//
// leave a channel, or delete it if yours
//
if (!client.LeaveChannel(guid, out response)) { // handle errors }
if (!client.DeleteChannel(guid, out response)) { // handle errors }

//
// send channel message
// received by 'AsyncMessageReceived' on each client that is a member of that channel
//
if (!client.SendChannelMessage(guid, "Hello!")) { // handle errors }

//
// list channel members
//
if (!client.ListChannelSubscribers(guid, out response, out clients)) { // handle errors }
```

## connecting using websockets
please refer to the sample Javascript chat application on github.

## bigq framing
bigq uses a simple framing mechanism that closely follows HTTP.  A set of headers start each message, with each header ending in a carriage return and newline ```\r\n```.  The headers contain a variety of metadata, and most importantly, ContentLength, which indicates how many bytes are to be read after the header delimiter.  The header delimiter is an additional carriage return and newline ```\r\n``` which follows the carriage return and newline of the final header.  The body is internally treated as a byte array so the connected clients will need to manage encoding.
```
Email: foo@bar.com
ContentType: application/json
ContentLength: 22

{ first_name: 'joel' }
```
