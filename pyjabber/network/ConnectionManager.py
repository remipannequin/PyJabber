import re
from asyncio import Transport
from ssl import SSLContext
from typing import List, Optional, Union, NamedTuple
from xml.etree.ElementTree import Element

from loguru import logger

from pyjabber.network.utils.TransportProxy import TransportProxy
from pyjabber.stream.JID import JID
from pyjabber.utils import Singleton


class Peer(NamedTuple):
    ip: str
    port: int


class Client(NamedTuple):
    jid: Optional[JID]
    transport: Transport
    online: bool


class Server(NamedTuple):
    host: str
    transport: Transport


class Component(NamedTuple):
    host: str
    transport: Transport


class ConnectionManager(metaclass=Singleton):
    """
    A singleton class used as a repository of connections during the protocols' lifecycle.

    It keeps track of both client and protocols connections using the following data structures:
    """
    def __init__(self) -> None:
        self._peerList: dict[Peer, Client] = {}
        self._remoteList: dict[Peer, Server] = {}
        self._componentList: dict[Peer, Component] = {}
        self._remoteIncomingList: dict[Peer, Server] = {}

        self._orphan_jids: dict[Peer, JID] = {}
        self._orphan_hosts: dict[Peer, str] = {}


    @property
    def componentList(self):
        return self._componentList

    ###########################################################################
    ############################## LOCAL BOUND ################################
    ###########################################################################


    def connection(self, peer: Peer, transport=None) -> None:
        if peer not in self._peerList:
            self._peerList[peer] = Client(None, transport, False)

    def close(self, peer: Peer) -> None:
        """Closes a connection by sending a '</stream:stream> message' and
            deletes it from the peers list
        """
        try:
            client = self._peerList.pop(peer)
            client.transport.write('</stream:stream>'.encode())
            if client.jid:
                self._orphan_jids[peer] = client.jid

            client.transport.close()
        except KeyError:
            logger.error(f"{peer} not present in the peer list")

    def online(self, jid: JID, online: bool = True):
        for peer, client in self._peerList.items():
            if client.jid == jid and client.online != online:
                self._peerList[peer] = self._peerList[peer]._replace(online=online)

    def get_transport(self, jid: JID) -> List[Client]:
        """Get all the available buffers associated with a JID.

                - If the JID is in the full format <username@domain/resource>, it will only return one buffer (or empty list).
                - If the JID is in the bare format <username@domain>, it will return a list of the buffers for each resource available.

            Both cases return a list.
        """
        if jid.resource:
            return [c for c in self._peerList.values() if jid == c.jid]
        else:
            return [c for c in self._peerList.values() if re.match(f"{str(jid)}/*", str(c.jid))]

    def get_transport_online(self, jid: JID) -> List[Client]:
        """Get all the available buffers associated with a JID
            that are ready to receive messages (online).

                - If the JID is in the full format <username@domain/resource>, it will only return one buffer (or empty list).
                - If the JID is in the bare format <username@domain>, it will return a list of the buffers for each resource available.

            Both cases return a list.
        """
        if jid.resource:
            return [c for c in self._peerList.values() if jid == c.jid and c.online]
        else:
            return [c for c in self._peerList.values()
                    if re.match(f"{str(jid)}/*", str(c.jid)) and c.online]

    def update_transport_peer(self, new_transport: Union[Transport, TransportProxy], peer: Peer):
        try:
            self._peerList[peer] = self._peerList[peer]._replace(transport=new_transport)
        except KeyError:
            raise KeyError("Unable to find client with given peer. Check this inconsistency")

    def update_transport_jid(self, new_transport: Union[Transport, TransportProxy], jid: JID):
        if jid.resource is None:
            raise ValueError("JID must have a resource to update transport")

        match = next((peer for peer, client in self._peerList.items()
                      if client.jid == jid), None)
        if match:
            self._peerList[match] = self._peerList[match]._replace(transport=new_transport)
        else:
            raise KeyError("Unable to find client with given JID. Check this inconsistency")

    def get_jid(self, peer: Peer) -> Union[JID, None]:
        try:
            return self._peerList[peer].jid
        except KeyError:
            self._orphan_jids.pop(peer, None)

    def set_jid(self, peer: Peer, jid: JID, transport: Transport = None) -> None:
        """Set/update the jid of a registered connection.

            An optional transport argument can be provided, in order to set/update the stored buffer
        """
        try:
            self._peerList[peer] = self._peerList[peer]._replace(
                jid=jid,
                transport=transport if transport else self._peerList[peer].transport
            )
        except KeyError:
            raise KeyError(f"Unable to find {peer} during jid/transport update")

    def update_resource(self, peer: Peer, resource: str):
        try:
            self._peerList[peer].jid.resource = resource
        except KeyError:
            raise KeyError(f"Unable to find {peer} during resource update")


    ###########################################################################
    ############################# REMOTE SERVER ###############################
    ###########################################################################


    def connection_server(self, peer: Peer, transport: Transport = None, host: str = None) -> None:
        if peer not in self._remoteList:
            self._remoteList[peer] = Server(host, transport)

    def close_server(self, peer: Peer) -> None:
        """Closes a connection by sending a '</stream:stream> message' and
            deletes it from the remote list
        """
        try:
            client = self._remoteList.pop(peer)
            client.transport.write('</stream:stream>'.encode())
            if client.host:
                self._orphan_hosts[peer] = client.host
        except KeyError:
            logger.error(f"{peer} not present in the remote list")

    def get_host(self, peer: Peer) -> Union[str, None]:
        try:
            return self._remoteList[peer].host
        except KeyError:
            return self._orphan_hosts.pop(peer, None)

    def set_host(self, peer: Peer, host: str, transport: Transport = None) -> None:
        """Set/update the host of a registered server connection.

            An optional transport argument can be provided, in order to set/update the stored buffer
        """
        try:
            self._remoteList[peer] = self._remoteList[peer]._replace(
                host=host,
                transport=transport if transport else self._remoteList[peer].transport
            )
        except KeyError:
            raise KeyError(f"Unable to find {peer} during host/transport update")

    def update_host(self, peer: Peer, host: str) -> None:
        try:
            self._remoteList[peer] = self._remoteList[peer]._replace(host=host)
        except KeyError:
            logger.warning("Unable to find protocols with given peer during host update. Check this inconsistency")

    def get_server_transport_peer(self, peer: Peer = None, host: str = None) -> Union[Transport, None]:
        try:
            return self._remoteList.get(peer).transport
        except KeyError:
            return None

    def get_server_transport_host(self, host: str = None) -> Union[Transport, None]:
        return next((s.transport for s in self._remoteList.values() if s.host == host), None)

    def update_transport_server(self, new_transport: Transport, peer: Peer):
        try:
            self._remoteList[peer] = self._remoteList[peer]._replace(transport=new_transport)
        except KeyError:
            logger.warning("Unable to find protocols with given peer. Check this inconsistency")

    def get_server_host(self, peer: Peer) -> Union[str, None]:
        try:
            return self._remoteList.get(peer).host
        except KeyError:
            return None

    def set_host_server(self, peer: Peer, host: str):
        try:
            self._remoteList[peer] = self._remoteList[peer]._replace(host=host)
        except KeyError:
            raise KeyError(f"Unable to find {peer} during host update")

    def get_connection_ssl_certificate(self, peer: Peer) -> Union[SSLContext, None]:
        try:
            self._remoteList.get(peer).transport.get_extra_info("ssl_object")
        except KeyError:
            return None


    ###########################################################################
    ############################# REMOTE SERVER ###############################
    ###########################################################################

    def connection_server_incoming(self, peer: Peer, transport: Transport = None, host: str = None) -> None:
        if peer not in self._remoteIncomingList:
            self._remoteIncomingList[peer] = Server(host, transport)

    def close_server_incoming(self, peer: Peer) -> None:
        """Closes a connection by sending a '</stream:stream> message' and
            deletes it from the remote incoming list
        """
        try:
            client = self._remoteIncomingList.pop(peer)
            client.transport.write('</stream:stream>'.encode())

            if client.host:
                self._presence_manager.put_nowait(
                    (client.host, Element("presence", attrib={"type": "INTERNAL"}))
                )

            client.transport.close()
        except KeyError:
            logger.error(f"{peer} not present in the remote incoming list")

  
    ###########################################################################
    ########################### EXTERNAL COMPONENT ############################
    ###########################################################################
    def connection_component(self, peer: Tuple[str, int], transport=None, host: str = None) -> None:
        """
            Store a new external component connection.

            :param peer: The peer value in the tuple format ({IP}, {PORT})
            :param transport: The transport object associated to the connection
            :param host: The host bound to the connection
        """
        if peer not in self._componentList:
            self._componentList[peer] = (host, transport)

    def disconnection_component(self, peer: Tuple[str, int]) -> None:
        """
            Remove a stored connection, and fires the DisconnectEvent

            :param peer: The peer value in the tuple format ({IP}, {PORT})
        """

        try:
            self._componentList.pop(peer)
        except KeyError:
            logger.warning(f"Component {peer} not present in the online list")

    def close_component(self, peer: Tuple[str, int]) -> None:
        """
            Closes a connection by sending a '</stream:stream> message' and
            deletes it from the remote list

            :param peer: The peer value in the tuple format ({IP}, {PORT})
        """
        try:
            _, buffer = self._componentList.pop(peer)
            # TODO How to do it properly in XEP-0114 ?
            buffer.write('</stream:stream>'.encode())
            self.disconnection_component(peer)
        except KeyError as e:
            logger.error(f"Component {peer} not present in the online list")

    def get_component_buffer(self, peer: Optional[Tuple[str, int]] = None, host: Optional[str] = None) -> Union[
        Transport, None]:
        """
            Return the buffer associated with the given component
        """
        if peer:
            if peer not in self._componentList:
                logger.error("Missing peer in the connection list. Check it")
                return None
            return self._componentList.get(peer)[1]

        if host:
            try:
                return [buffer[1] for buffer in self._componentList.values() if buffer[0] == host].pop()
            except IndexError:
                pass

        logger.error("Missing peer OR host to search for component transport. Returning None")
        return None

    def set_component_host(self, peer: Tuple[str, int], host:str):
        """Update the component host (subdomain) when it is declared by the component."""
        if peer not in self._componentList:
            logger.error("Missing peer in the connection list. Check it")
            return
        transport = self._componentList.get(peer)[1]
        self._componentList[peer] = (host, transport)


    def get_component_host(self, peer: Tuple[str, int]):
        """
            Return the host associated with the given peer.
            :return: Hostname
        """
        return self._componentList.get(peer)[0] if self._componentList.get(peer) else None


    def has_component_host(self, domain):
        """Return true if a component manages this subdomain"""
        return domain in [b[0] for b in self._componentList.values()]

def is_subdomain_of(parent: str, domain: str) -> bool:
    """Utility function that return true if to_test is a subdomain of domain.
    It is strict: if parent and domain are the same, it returns False"""
    domain = domain.lower()
    parent = parent.lower()

    return domain.endswith("." + parent)
