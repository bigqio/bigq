# Change Log

## Current Version

v3.0.3

- Updated dependencies (WatsonTcp) to better support graceful and non-graceful termination of client and server (dispose vs ctrl-c)

## Previous Versions

v3.0.x

- Task-based callbacks and front-end APIs in both client and server
- Reduced class members, code reduction and cleanup
- Better handling of disconnected clients and connection termination
- Better CLI experience
- Better protection of class members (internal vs public)
- Enums for channel visibility and message distribution type
- Eliminated Core library dependency and reduced Common static methods
- Dependency update for bugfixes and reliability
 
v2.1.x

- Refactor to reduce code complexity, improve performance, and improve stability

v2.0.x

- Migrate from Mono.Data.Sqlite to System.Data.SQLite
- Retarget to .NET Core 2.0 and .NET Framework 4.6.2
- Major refactor and simplification for better code manageability
- Separate logic for client and server functions, removed client dependencies within the server
- Enums where appropriate including client connection type and message commands
- Removed unnecessary configuration parameters and constructors
- Creation of a common Core library and separate client and server libraries
- Several minor bugfixes

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