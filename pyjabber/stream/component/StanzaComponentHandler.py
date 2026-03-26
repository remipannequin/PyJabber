import xml.etree.ElementTree as ET

from pyjabber.stream.JID import JID
from pyjabber.stream.StanzaHandler import StanzaHandler
from pyjabber.utils import ClarkNotation as CN


class StanzaComponentHandler(StanzaHandler):
    """Stanza Handler to manage stanzas received from
    external components (XEP-0114)."""
    def __init__(self, buffer) -> None:
        super().__init__(buffer)

        self._functions = {
            "{jabber:component:accept}iq": self.handle_iq,
            "{jabber:component:accept}message": self.handle_msg,
            "{jabber:component:accept}presence": self.handle_pre
        }

    def handle_iq(self, element: ET.Element):
        # TODO ?
        return

    def handle_pre(self, element: ET.Element):
        # TODO ? 
        pass

    def handle_msg(self, element: ET.Element):
        """Check that the message domain is the one of the component."""
        jid = JID(element.attrib["to"])
        # TODO check that from is right

        # transform namespace jabber:component:accept to jabber:client
        ns, tag = CN.deglose(element.tag)
        #ns should be jabber:component:accept
        CN.update_namespace('jabber:client', element)

        if not jid.resource:
            priority = self._presenceManager.most_priority(jid)
            if not priority and self._message_persistence:
                self._message_queue.put_nowait(('MESSAGE', jid, ET.tostring(element)))
                return None

            all_resources_online = []
            for user in priority:
                all_resources_online += self._connections.get_buffer_online(
                    JID(user=jid.user, domain=jid.domain, resource=user[0]))
            for buffer in all_resources_online:
                buffer[1].write(ET.tostring(element))
        else:
            resource_online = self._connections.get_buffer_online(jid)
            if not resource_online and self._message_persistence:
                self._message_queue.put_nowait(('MESSAGE', jid, ET.tostring(element)))
            else:
                for buffer in resource_online:
                    buffer[1].write(ET.tostring(element))
