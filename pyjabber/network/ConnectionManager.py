import asyncio
import re
from typing import List, Optional
from asyncio import Transport
from typing import Dict, Union, Tuple
from loguru import logger

from pyjabber.stream.JID import JID
from pyjabber.utils import Singleton

Peer = Tuple[str, int]
PeerList = Dict[Peer, Tuple[Optional[JID], Transport, List[bool]]]
RemoteList = Dict[Peer, Tuple[Optional[str], Transport]]


class ConnectionManager(metaclass=Singleton):
    """
    A singleton class used as a repository of connections during the server's lifecycle.

    It keeps track of both client and server connections using the following data structures:

        peerList = {
            ("0.0.0.0", "50000"): (<JID(demo@localhost/12341234)>, <asyncio.Transport>, <Online: bool>),
            ("0.0.0.0", "50001"): (<JID(test@localhost/43214321)>, <asyncio.Transport>, <Online: bool>)
        }

        remoteList = {
            ("0.0.0.0", "50000"): ("domain.es", <asyncio.Transport>),
            ("0.0.0.0", "50001"): ("host.com", <asyncio.Transport>)
        }

        componentList = {
            ("0.0.0.0", "50000"): ("sub.localhost", <asyncio.Transport>),
            ("0.0.0.0", "50001"): ("sub1.localhos", <asyncio.Transport>)
        }
    """

    def __init__(self) -> None:

        self._peerList: PeerList = {}
        self._remoteList = {}
        self._componentList = {}

    @property
    def peerList(self):
        return self._peerList

    @property
    def remoteList(self):
        return self._remoteList

    @property
    def componentList(self):
        return self._componentList

    ###########################################################################
    ############################## LOCAL BOUND ################################
    ###########################################################################

    def connection(self, peer: Tuple[str, int], transport=None) -> None:
        """
            Store a new connection, without jid or transport.
            Those will be added in the future with the set_jid method.

            :param peer: The peer value in the tuple format ({IP}, {PORT})
            :param transport: The transport object associated to the connection
        """
        if peer not in self._peerList:
            self._peerList[peer] = (None, transport, [False])

    def disconnection(self, peer: Tuple[str, int]) -> None:
        """
            Remove a stored connection

            :param peer: The peer value in the tuple format ({IP}, {PORT})
        """

        try:
            self._peerList.pop(peer)
        except KeyError:
            logger.warning(f"{peer} not present in the online list")

    def online(self, jid: JID, online: bool = True):
        """
            Set a client connection status to Online.

            Useful for presence behaviour

        :param jid: JID of the client to update status
        :param online: New status. True by default
        """
        for k, v in self._peerList.items():
            if v[0] == jid and v[2][0] != online:
                self._peerList[k] = (v[0], v[1], [online])

    def close(self, peer: Tuple[str, int]) -> None:
        """
            Closes a connection by sending a '</stream:stream> message' and
            deletes it from the peers list

            :param peer: The peer value in the tuple format (<IP>, <PORT>)
        """
        try:
            _, buffer, _ = self._peerList.pop(peer)
            buffer.write('</stream:stream>'.encode())
            self.disconnection(peer)
        except KeyError as e:
            logger.error(f"{peer} not present in the online list")

    def get_buffer(self, jid: JID) -> List[Tuple[JID, Transport, bool]]:
        """
            Get all the available buffers associated with a JID.

                - If the JID is in the full format <username@domain/resource>, it will only return one buffer or none
                - If the JID is in the bare format <username@domain>, it will return a list of the buffers for each resource available.

            Both cases return a list.

            :param jid: The jid to get the buffers for. It can be a full jid or a bare jid
            :return: (JID, TRANSPORT) tuple
        """
        if jid.resource:
            return [(jid_stored, buffer, online[0])
                    for jid_stored, buffer, online in self._peerList.values()
                    if jid == jid_stored]
        else:
            return [(jid_stored, buffer, online[0])
                    for jid_stored, buffer, online in self._peerList.values()
                    if re.match(f"{str(jid)}/*", str(jid_stored))]

    def get_buffer_online(self, jid: JID) -> List[Tuple[JID, Transport, bool]]:
        """
            Get all the available buffers associated with a JID
            that are ready to receive messages (online).

            - If the JID is in the full format <username@domain/resource>, it will only return one buffer or none.
            - If the JID is in the bare format <username@domain>, it will return a list of the buffers for each resource available.

            Both cases return a list.

            :param jid: The jid to get the buffers for. It can be a full jid or a bare jid
            :return: (JID, TRANSPORT) tuple
        """
        if jid.resource:
            return [(jid_stored, buffer, online[0])
                    for jid_stored, buffer, online in self._peerList.values()
                    if jid == jid_stored and online[0] is True]
        else:
            return [(jid_stored, buffer, online[0])
                    for jid_stored, buffer, online in self._peerList.values()
                    if re.match(f"{str(jid)}/*", str(jid_stored))
                    and online[0] is True]

    def update_buffer(self, new_transport: Transport, peer: Tuple[str, int] = None, jid: JID = None):
        if not peer and not jid:
            logger.warning(
                "Missing peer OR jid parameter to update transport in client connection. No action will be performed")
            return

        if peer:
            try:
                jid, old_transport, online = self._peerList[peer]
                self._peerList[peer] = (jid, new_transport, online)
                return
            except KeyError:
                logger.warning("Unable to find client with given peer. Check this inconsistency")
                return

        if jid.resource is None:
            logger.warning("JID must have a resource to update transport")
            return

        match = next(((k, v) for k, v in self._peerList.items() if v[0] == jid), None)
        if match:
            jid, _, online = match[1]
            self._peerList[match[0]] = (jid, new_transport, online)
        else:
            logger.warning("Unable to find client with given JID. Check this inconsistency")

    # def get_connection_certificate(self, peer: Tuple[str, int]):
    #     """
    #         Return the SSL certificate used in the STARTTLS process
    #         If no certificated is bounded with the connection, returns None
    #     """
    #     return next(self._peerList.get(peer)[1].get_extra_info("ssl_object"), None)

    ###########
    ### JID ###
    ###########

    def get_jid(self, peer: Tuple[str, int]) -> Union[JID, None]:
        """
            Return the jid associated with the peername

            :param peer: The peer value in the tuple format ({IP}, {PORT})
        """
        try:
            return self._peerList[peer][0]
        except KeyError:
            return None

    def set_jid(self, peer: Tuple[str, int], jid: JID, transport: Transport = None) -> None:
        """
            Set/update the jid of a registered connection.

            An optional transport argument can be provided, in order to set/update the stored buffer

            :param peer: The peer value in the tuple format ({IP}, {PORT})
            :param jid: The jid to set/update
            :param transport: Transport to use
        """
        try:
            _, old_transport, online = self._peerList[peer]
            self._peerList[peer] = (jid, transport or old_transport, online)
        except KeyError:
            logger.error(f"Unable to find {peer} during jid/transport update")

    def update_resource(self, peer: Tuple[str, int], resource: str):
        try:
            self._peerList[peer][0].resource = resource
        except KeyError:
            logger.error(f"Unable to find {peer} during resource update")

    ###########################################################################
    ############################# REMOTE SERVER ###############################
    ###########################################################################
    def connection_server(self, peer: Tuple[str, int], transport=None, host: str = None) -> None:
        """
            Store a new server connection.

            :param peer: The peer value in the tuple format ({IP}, {PORT})
            :param transport: The transport object associated to the connection
            :param host: The host bound to the connection
        """
        if peer not in self._remoteList:
            self._remoteList[peer] = (host, transport)

    def disconnection_server(self, peer: Tuple[str, int]) -> None:
        """
            Remove a stored connection, and fires the DisconnectEvent

            :param peer: The peer value in the tuple format ({IP}, {PORT})
        """

        try:
            self._remoteList.pop(peer)
        except KeyError:
            logger.warning(f"Server {peer} not present in the online list")

    def update_host(self, peer: Tuple[str, int], host: str) -> None:
        try:
            _, transport = self._remoteList[peer]
            self._remoteList[peer] = (host, transport)
            return
        except KeyError:
            logger.warning("Unable to find server with given peer during host update. Check this inconsistency")
            return

    def close_server(self, peer: Tuple[str, int]) -> None:
        """
            Closes a connection by sending a '</stream:stream> message' and
            deletes it from the remote list

            :param peer: The peer value in the tuple format ({IP}, {PORT})
        """
        try:
            _, buffer = self._remoteList.pop(peer)
            buffer.write('</stream:stream>'.encode())
            self.disconnection_server(peer)
        except KeyError as e:
            logger.error(f"{peer} not present in the online list")

    def get_server_buffer(self, peer: Optional[Tuple[str, int]] = None, host: Optional[str] = None) -> Union[
        Transport, None]:
        """
            Return the buffer associated with the given host
        """
        if peer:
            if peer not in self._remoteList:
                logger.error("Missing peer in the connection list. Check it")
                return None
            return self._remoteList.get(peer)[1]

        if host:
            try:
                return [buffer[1] for buffer in self._remoteList.values() if buffer[0] == host].pop()
            except IndexError:
                pass

        logger.error("Missing peer OR host to search for server transport. Returning None")
        return None

    def update_transport_server(self, new_transport: Transport, peer: Tuple[str, int] = None, host: str = None):
        if not peer and not host:
            logger.warning(
                "Missing peer OR jid parameter to update transport in server connection. No action will be performed")
            return

        if peer:
            try:
                host, _ = self._remoteList[peer]
                self._remoteList[peer] = (host, new_transport)
                return
            except KeyError:
                logger.warning("Unable to find server with given peer. Check this inconsistency")
                return

        match = next(((k, v) for k, v in self._remoteList.items() if v[0] == host), None)
        if match:
            host, _ = match[1]
            self._remoteList[match[0]] = (host, new_transport)
        else:
            logger.warning("Unable to find server with given host. Check this inconsistency")

    def get_server_host(self, peer: Tuple[str, int]):
        """
            Return the host associated with the given peer.
            :return: Hostname
        """
        return self._remoteList.get(peer)[0] if self._remoteList.get(peer) else None

    def set_host_server(self, peer: Tuple[str, int], host: str):
        entry = self._peerList.get(peer)
        if entry:
            self._remoteList[peer] = (host, entry[1])

    def get_connection_certificate_server(self, peer: Tuple[str, int]):
        """
            Return the SSL certificate used in the STARTTLS process
            If no certificated is bounded with the connection, returns None
        """
        if peer not in self._remoteList:
            return None
        return self._remoteList.get(peer)[1].get_extra_info("ssl_object")
    
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
