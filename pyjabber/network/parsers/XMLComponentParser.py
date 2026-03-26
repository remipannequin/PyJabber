from uuid import uuid4
from xml.etree import ElementTree as ET

from pyjabber.network.parsers.XMLParser import XMLParser
from pyjabber.stream.Stream import Stream, Namespaces
from pyjabber.stream.StreamHandler import Signal
from pyjabber.stream.component.StanzaComponentHandler import StanzaComponentHandler
from pyjabber.stream.component.StreamComponentHandler import StreamComponentHandler
from pyjabber.utils import ClarkNotation as CN

class XMLComponentParser(XMLParser):
    """This is a specialization of XMLParser for external components (XEP-0114)
    It rewrite the startElementNS to intercept the stream id that is sent to the
    component (it must be know to verify handshake). 
    """
    stanza_handler_constructor = StanzaComponentHandler
    stream_handler_constructor = StreamComponentHandler
    server: bool = False
    component: bool = True

    def __init__(self, host, transport, starttls):
        super().__init__(transport, starttls)
        self._domain_claim = None
           
    
    def startElementNS(self, name, qname, attrs):
        """This is basically the same as startElementNS in XMLParser,
        but we capture the stream id that is sent to the client"""
        if self._stack:  # "<stream:stream>" tag already present in the data stack
            elem = ET.Element(
                CN.clarkFromTuple(name),
                attrib={
                    CN.clarkFromTuple(key): item for key,
                    item in dict(attrs).items()})
            self._stack.append(elem)

        elif name[1] == "stream" and name[0] == "http://etherx.jabber.org/streams":
            elem = ET.Element(
                CN.clarkFromTuple(name),
                attrib={
                    CN.clarkFromTuple(key): item for key,
                    item in dict(attrs).items()})
            self._from_claim = elem.attrib.get("from")
            self._stack.append(elem)

            # we nned to know the id to verify handshake
            id = str(uuid4())
            self._domain_claim = attrs.get((None, "to"))
            # TODO check that the the domain is not already taken
            rsp_stream = Stream(
                id=id,
                from_=self._domain_claim,
                to=attrs.get((None, "from")),
                version=attrs.get((None, "version"), "1.0"),
                xml_lang=attrs.get(("http://www.w3.org/XML/1998/namespace", "lang")),
                xmlns=Namespaces.COMPONENT.value
            )

            self._streamHandler.set_stream_id(id, self._domain_claim)
            self._transport.write(rsp_stream.open_tag())
                
            signal = self._streamHandler.handle_open_stream()
            if signal and signal == Signal.DONE:
                self._stanzaHandler = self.stanza_handler_constructor(self._transport)
                self._state = self.StreamState.READY

        else:
            raise Exception()