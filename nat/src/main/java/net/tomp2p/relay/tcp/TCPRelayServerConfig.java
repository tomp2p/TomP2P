package net.tomp2p.relay.tcp;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.BaseRelayServer;
import net.tomp2p.relay.RelayServerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPRelayServerConfig extends RelayServerConfig {

	private final static Logger LOG = LoggerFactory.getLogger(TCPRelayServerConfig.class);

	public TCPRelayServerConfig(Peer peer) {
		super(peer);
	}

	@Override
	public BaseRelayServer createServer(Message message, PeerConnection peerConnection, Responder responder) {
		if (peer.peerAddress().isRelayed()) {
			// peer is behind a NAT as well -> deny request
			LOG.warn("I cannot be a relay since I'm relayed as well! {}", message);
			responder.response(createResponse(message, Type.DENIED));
			return null;
		}

		LOG.debug("Hello unreachable peer! You'll be relayed over an open TCP connection.");
		TCPRelayServer tcpForwarder = new TCPRelayServer(peerConnection, peer);
		responder.response(createResponse(message, Type.OK));
		return tcpForwarder;
	}

}
