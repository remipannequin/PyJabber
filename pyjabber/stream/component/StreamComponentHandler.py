from xml.etree import ElementTree as ET
import hashlib
from pyjabber.stream.Signal import Signal
from pyjabber.stream.StreamHandler import StreamHandler, Stage


class StreamComponentHandler(StreamHandler):
    """StreamHandler that manage connection of external component,
    following XEP-0114."""
    def __init__(self, transport, starttls, parser_ref) -> None:
        super().__init__(transport, starttls, parser_ref)
        self._ibr_feature = False
        # remove the tls and sasl stages
        # CONNECTED -> AUTH -> BIND
        self._stages_handlers.pop(Stage.OPENED)
        self._stages_handlers.pop(Stage.SSL)
        self._stages_handlers.pop(Stage.SASL)
        # in connected, go directly to AUTH
        self._stages_handlers[Stage.CONNECTED] = self._handle_init_comp
        self._stages_handlers[Stage.AUTH] = self._handle_handshake
        self._stream_id = None
        # TODO get the subdomain -> secret dictionary from config
        self._secrets = {}
        self._secrets["sub.localhost"] = "test_secret"

    def set_stream_id(self, sid: str, domain: str):
        """Set the stream ID that have been sent to external component"""
        self._stream_id = sid
        self._comp_domain = domain

    def verify_handshake(self, received_handshake: str | None) -> bool:
        """
        Verify XEP-0114 handshake.

        :param received_handshake: hex string received from component
        :return: True if valid, False otherwise
        """
        if not self._stream_id:
            
            return False
        if not self._comp_domain or self._comp_domain not in self._secrets:
            
            return False
        secret = self._secrets[self._comp_domain]
        data = (self._stream_id + secret).encode("utf-8")
        expected = hashlib.sha1(data).hexdigest()
        return received_handshake == expected

    def update_comp_host(self):
        """Update the host name of the component with the domain declared
        by the external component."""
        peer = self._parser_ref._peer
        domain = self._parser_ref._domain_claim
        self._connection_manager.set_component_host(peer, domain)

    def _handle_init_comp(self, _):
        """Init without TLS, nor features. Directly go to AUTH stage."""
        self._stage = Stage.AUTH

    def _handle_handshake(self, element: ET.Element):
        if element.tag != "{jabber:component:accept}handshake":
            return Signal.FORCE_CLOSE
        # get and check handshake
        handshake = element.text
        if self.verify_handshake(handshake):
            # reply with an empty handshake tag
            self._transport.write(b'<handshake/>')
            self._stage = Stage.READY
            # after authentication, change the hostname of the component
            self.update_comp_host()
            return Signal.DONE
        else:
            return Signal.FORCE_CLOSE

