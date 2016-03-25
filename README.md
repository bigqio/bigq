# bigq
scalable messaging platform

## description
bigq is a scalable messaging platform using TCP sockets featuring sync, async, channel, and private communications. bigq is written in C# and made available under the MIT license.

Core use cases for bigq:
- real-time messaging like chat applications
- message distribution/publisher-subscriber using channels
- cluster management
- notifications and events

## components
Two main components to bigq: client and server.  The server can be run independently or instantiated within your own application.  Clients initiate connections to the server and maintain them to avoid issues with intermediary firewalls.  

## starting the server
Refer to the BigQServerTest project for a thorough example.
```
using BigQ;
...
// start the server
BigQServer server = new BigQServer(null, 8000, false, false, true, true, true);

// set callbacks
server.MessageReceived = MessageReceived;		
server.ServerStopped = ServerStopped;				
server.ClientConnected = ClientConnected;
server.ClientLogin = ClientLogin;
server.ClientDisconnected = ClientDisconnected;
server.LogMessage = LogMessage;

// implement, these methods should return true
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
...
// connect to server
BigQClient client = new BigQClient(null, null, "127.0.0.1", 8000, 10000, false);

// set callbacks
client.AsyncMessageReceived = AsyncMessageReceived;
client.SyncMessageReceived = SyncMessageReceived;
client.ServerDisconnected = ServerDisconnected;
client.LogMessage = LogMessage;

// implement callbacks, these methods should return true
// sync message callback should return the data to be returned to requestor
static bool AsyncMessageReceived(BigQMessage msg) { ... }
static object SyncMessageReceived(BigQMessage msg) { return "Hello!"; }
static bool ServerDisconnected() { ... }
static bool LogMessage(string msg) { ... }
```

## enumerating clients on the client
```
if (!client.ListClients()) { // handle errors }
```

## sending a message on the client
```
// private async message
// received by 'AsyncMessageReceived' on recipient client
if (!client.SendPrivateMessageAsync(guid, msg)) { // handle errors }

// private sync message
// received by 'SyncMessageReceived' on recipient client
// which should return response data
BigQMessage response;
if (!client.SendPrivateMessageSync(guid, "Hello!", out response)) { // handle errors }
```

## managing channels on the client
```
if (!client.CreateChannel(guid, false)) { // handle errors }
if (!client.JoinChannel(guid)) { // handle errors }
if (!client.LeaveChannel(guid)) { // handle errors }
if (!client.DeleteChannel(guid)) { // handle errors }

// send channel message
// received by 'AsyncMessageReceived' on each client that is a member of that channel
if (!client.SendChannelMessage(guid, "Hello!")) { // handle errors }
```