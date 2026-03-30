import asyncio
from typing import Union
from uuid import uuid4
from xml.etree import ElementTree as ET
import hashlib

from loguru import logger

from pyjabber import AppConfig
from pyjabber.network.utils.TransportProxy import TransportProxy
from pyjabber.queues.NewConnection import NewConnectionWrapper
from pyjabber.queues.QueueManager import get_queue, QueueName
from pyjabber.stream.negotiators.StreamNegotiator import Signal, Stage, StreamNegotiator
from pyjabber.stream.handlers.StanzaHandler import InternalServerError
from pyjabber.stream.utils.Stream import Stream, Namespaces
from pyjabber.utils import Exceptions as EX
from pyjabber.stanzas.error import StanzaError as SE
from pyjabber.stream.JID import JID


class ComponentStreamNegotiator(StreamNegotiator):
    def __init__(self, transport, protocol, parser, handler) -> None:
        super().__init__(transport, protocol, parser, handler)

        self._connection_queue = get_queue(QueueName.CONNECTIONS)

        self._stages_handlers = {
            Stage.CONNECTED: self._handle_init_component,
            Stage.AUTH: self._handle_handshake
        }

        self._stages_flags = {
            "auth": False,
        }
        # TODO get secrets from a config somewhere
        self._secrets = {}
        self._secrets["sub.localhost"] = "test_secret"
        self._stream_id = None
        self._domain_claim = None

    async def handle_open_stream(self, elem: ET.Element = None) -> Union[Signal, None]:
        # we need to overrides this method because we need the sream_id to check
        # the handshake
        try:
            if elem.tag == '{http://etherx.jabber.org/streams}stream':
                # The external component declare a domain in the 'to' attribute
                # that we will use 
                self._stream_id = str(uuid4())
                # TODO verify this appears as a subdomain of ours
                self._domain_claim = elem.attrib.get("to")
                
                version = elem.attrib.get("version", "1.0")
                lang = elem.attrib.get(("http://www.w3.org/XML/1998/namespace", "lang"))

                stream = Stream(
                    id=self._stream_id,
                    from_=self._domain_claim,
                    to=None,
                    version=version,
                    xml_lang=lang,
                    xmlns=Namespaces.COMPONENT.value
                )
                self._transport.write(stream.open_tag())
            return await self._stages_handlers[self._stage](elem)
        except EX.NotAuthorizerStreamNegotiationException:
            self._transport.write(SE.not_authorized())
            self._connection_manager.close(self._peer)
        except EX.BadRequestException:
            self._transport.write(SE.bad_request())
            self._connection_manager.close(self._peer)
        except InternalServerError:
            self._transport.write(SE.internal_server_error())
            self._connection_manager.close(self._peer)
        except Exception as e:
            logger.error(e)

    def verify_handshake(self, received_handshake: str | None) -> bool:
        """
        Verify XEP-0114 handshake.

        :param received_handshake: hex string received from component
        :return: True if valid, False otherwise
        """
        if not self._stream_id:
            
            return False
        if not self._domain_claim or self._domain_claim not in self._secrets:
            
            return False
        secret = self._secrets[self._domain_claim]
        data = (self._stream_id + secret).encode("utf-8")
        expected = hashlib.sha1(data).hexdigest()
        return received_handshake == expected

    def update_component_host(self):
        """Update the host name of the component with the domain declared
        by the external component."""
        peer = self._transport.get_extra_info('peername')
        domain = self._domain_claim
        # note, a JID must have both a user and domain, so we cannot use it here
        self._connection_manager.set_component_host(peer, domain)

    async def _handle_init_component(self, _):
        """Init without TLS, nor features. Directly go to AUTH stage."""
        self._stage = Stage.AUTH

    async def _handle_handshake(self, element: ET.Element):
        if element.tag != "{jabber:component:accept}handshake":
            return Signal.FORCE_CLOSE
        # get and check handshake
        handshake = element.text
        if self.verify_handshake(handshake):
            # reply with an empty handshake tag
            self._transport.write(b'<handshake/>')
            self._stages_flags["auth"] = True
            # after authentication, change the hostname of the component
            self.update_component_host()
            return Signal.DONE
        else:
            return Signal.FORCE_CLOSE

