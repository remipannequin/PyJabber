# ROADMAP for developping an XEP0114 plugin

1. Opens a TCP port and listen to connection. Check with a generic tcp client that the port is open
2. implement authentication. Connect with slixmpp client
3. migrate transport (and parsing) to methods implemented in pyjabber. Use Connection() ?
4. store TCP connection by id
