import asyncio
import xml.etree.ElementTree as ET
from asyncio import Transport

from pyjabber import AppConfig
from pyjabber.features.presence.PresenceFeature import Presence
from pyjabber.network.ConnectionManager import ConnectionManager
from pyjabber.plugins.PluginManager import PluginManager
from pyjabber.queues.NewConnection import NewConnectionWrapper
from pyjabber.queues.PendingMessage import PendingMessageWrapper
from pyjabber.queues.QueueManager import get_queue, QueueName
from pyjabber.stream.JID import JID
from pyjabber.utils import ClarkNotation as CN
from pyjabber.utils.Exceptions import InternalServerError
from pyjabber.stream.handlers.StanzaHandler import StanzaHandler


class ComponentStanzaHandler(StanzaHandler):
    def __init__(self, transport: Transport) -> None:
        self._ip = AppConfig.app_config.ip
        self._transport = transport
        self._peername = transport.get_extra_info('peername')

        self._connections = ConnectionManager()

        self._host = self._connections.get_host(self._peername)

        self._presenceManager = Presence()

        self._message_queue = get_queue(QueueName.MESSAGES)
        self._connection_queue: asyncio.Queue = get_queue(QueueName.CONNECTIONS)

        self._message_persistence = AppConfig.app_config.message_persistence

        self._functions = {
            "{jabber:component:accept}iq": self.handle_iq,
            "{jabber:component:accept}message": self.handle_msg,
            "{jabber:component:accept}presence": self.handle_pre,
        }

        get_queue(QueueName.CONNECTIONS).put_nowait(
            NewConnectionWrapper(self._host, False, True)
        )

    async def handle_iq(self, element: ET.Element):
        # TODO ?
        return

    async def handle_pre(self, element: ET.Element):
        # TODO ?
        pass

    async def handle_msg(self, element: ET.Element):
        """Check that the message domain is the one of the component."""
        jid = JID(element.attrib["to"])
        # TODO check that from is right

        # transform namespace jabber:component:accept to jabber:client
        ns, tag = CN.break_down(element.tag)
        # ns should be jabber:component:accept
        CN.update_namespace("jabber:client", element)

        # dispatch message "normally"
        await super().handle_msg(element)
