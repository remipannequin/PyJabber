from unittest.mock import patch, Mock

import pytest
from asyncio import Transport

from pyjabber.server import Server
from pyjabber.network.ConnectionManager import ConnectionManager
from pyjabber.server_parameters import Parameters


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

@pytest.fixture
def setup_stream():
    with patch('pyjabber.stream.StreamHandler.metadata') as mock_meta_sh:
        with patch('pyjabber.features.SASLFeature.metadata') as mock_meta_sasl:
            transport = Mock()
            starttls = Mock()
            mock_meta_sh.HOST = 'localhost'
            mock_meta_sasl.HOST = 'localhost'
            mock_protocol = MagicMock()
            mock_protocol.from_claim = None
            yield StreamHandler(transport, starttls, mock_protocol)