# ROADMAP for developping an XEP0114 plugin

1. Opens a TCP port and listen to connection. Check with a generic tcp client that the port is open
2. implement authentication. Connect with slixmpp client. Add tests to simulate the authentication process
3. migrate transport (and parsing) to methods implemented in pyjabber. Use Connection() ?
4. store TCP connection by id

## Open questions

- added a whole new set of functions in Connection Manager, but maybe we could use remoteList instead ? (and add the type in the data structure ?)

- how to mock the authentication process from a component ?

- what is the best mean to pass the domain/secret configuration ? -> store in DB -> get it from arguments (with a json file ?) 

- how to control if the external component server should be started or not -> xep-0114 in plugins ?