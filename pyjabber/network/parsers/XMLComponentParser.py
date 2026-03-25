from pyjabber.network.parsers.XMLParser import XMLParser
from pyjabber.stream.component.StanzaComponentHandler import StanzaComponentHandler
from pyjabber.stream.component.StreamComponentHandler import StreamComponentHandler


class XMLComponentParser(XMLParser):
    stanza_handler_constructor = StanzaComponentHandler
    stream_handler_constructor = StreamComponentHandler
    server: bool = False

    def __init__(self, host, transport, starttls):
        super().__init__(transport, starttls)