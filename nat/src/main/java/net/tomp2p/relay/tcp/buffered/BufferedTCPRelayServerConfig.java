package net.tomp2p.relay.tcp.buffered;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.BaseRelayServer;
import net.tomp2p.relay.RelayServerConfig;
import net.tomp2p.relay.buffer.MessageBufferConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedTCPRelayServerConfig extends RelayServerConfig {

	private final static Logger LOG = LoggerFactory.getLogger(BufferedTCPRelayServerConfig.class);
	private final MessageBufferConfiguration bufferConfig;

	public BufferedTCPRelayServerConfig(MessageBufferConfiguration bufferConfig) {
		this.bufferConfig = bufferConfig;
	}
	
	@Override
	public void start(Peer peer) {
		// nothing to do
	}
	
	@Override
	public BaseRelayServer createServer(Message message, PeerConnection peerConnection, Responder responder, Peer peer) {
		if (peer.peerAddress().isRelayed()) {
			// peer is behind a NAT as well -> deny request
			LOG.warn("I cannot be a relay since I'm relayed as well! {}", message);
			responder.response(createResponse(message, Type.DENIED, peer.peerBean().serverPeerAddress()));
			return null;
		}

		LOG.debug("Hello unreachable peer! You'll be relayed over a buffered open TCP connection.");
		BufferedTCPRelayServer tcpForwarder = new BufferedTCPRelayServer(peerConnection, peer, bufferConfig);
		responder.response(createResponse(message, Type.OK, peer.peerBean().serverPeerAddress()));
		return tcpForwarder;
	}
}
