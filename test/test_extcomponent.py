from unittest.mock import patch, Mock, MagicMock

import pytest

from asyncio import Transport
from xml.etree import ElementTree as ET
from uuid import uuid4

from pyjabber.server import Server
from pyjabber.network.ConnectionManager import ConnectionManager
from pyjabber.server_parameters import Parameters
from pyjabber.stream.component.StreamComponentHandler import StreamComponentHandler
from pyjabber.stream.StreamHandler import Stage
from pyjabber.stream.Signal import Signal


@pytest.fixture
def setup():
    with patch('pyjabber.server.logger') as mock_log, \
         patch('pyjabber.server.DB') as mock_db, \
         patch('pyjabber.server.init_utils') as mock_utils:
        mock_utils.setup_query_local_ip.return_value = '127.0.0.1'
        # mock_utils.setup_ip_by_host.return_value =
        param = Parameters()
        yield Server(param), mock_log, mock_db


def test_parameters():
    """Assert that the parameters have the right port value"""
    param = Parameters()
    assert param.component_port == 5347

def test_connections():
    """Assert that connections list external comp structures"""
    conn = ConnectionManager()
    assert conn.componentList is not None
    assert len(conn.componentList) == 0
    mock_transport = Mock(spec=Transport)
    test_peer = ("1.2.3.4", 1234)
    conn.connection_component(peer=test_peer, transport=mock_transport, host="domain.com")
    assert test_peer in conn.componentList
    assert test_peer not in conn.remoteList
    assert test_peer not in conn.peerList
    assert conn.get_component_buffer(test_peer) is mock_transport
    assert conn.get_component_buffer(host="domain.com") is mock_transport
    # TODO increase coverage....
    conn.disconnection_component(test_peer)
    assert len(conn.componentList) == 0


def test_stream_handler_constructor(setup):
    
    mock_transport = MagicMock()

    mock_parser_red = MagicMock()

    with patch('pyjabber.stream.StreamHandler.metadata') as mock_meta:
        mock_meta.HOST = 'localhost'
        mock_meta.PLUGINS = ['jabber:iq:register']
        handler = StreamComponentHandler(mock_transport, None, mock_parser_red)

    assert handler._ibr_feature == False
    assert list(handler._stages_handlers.keys()) == [
        Stage.CONNECTED, Stage.AUTH
    ]
    assert handler._handle_init_comp == handler._stages_handlers[Stage.CONNECTED]
    assert handler._handle_handshake == handler._stages_handlers[Stage.AUTH]


def test_stream_handler_auth(setup):
    
    mock_transport = MagicMock()
    mock_parser_red = MagicMock()

    stream_id = "062504fe-f665-4fba-97c9-6254ce40e48f"
    component_domain = "sub.localhost"

    with patch('pyjabber.stream.StreamHandler.metadata') as mock_meta:
        mock_meta.HOST = 'localhost'
        mock_meta.PLUGINS = ['jabber:iq:register']
        handler = StreamComponentHandler(mock_transport, None, mock_parser_red)
        handler._stage = Stage.AUTH

    handler.set_stream_id(stream_id, component_domain)
    handshake = "e417473576b7e5adf16d3c8e9ea8f47100ad5f1f"
    assert handler.verify_handshake(handshake)
    for _ in range(10):
        assert not handler.verify_handshake(str(uuid4()))
    in_elt = ET.Element("{jabber:component:accept}handshake")
    in_elt.text = handshake
    # TODO test authentication failed : should send SIGNAL.FORCE_CLOSE
    signal = handler.handle_open_stream(elem=in_elt)
    mock_transport.write.assert_called_with(b'<handshake/>')
    assert handler._stage == Stage.READY
    assert signal == Signal.DONE

