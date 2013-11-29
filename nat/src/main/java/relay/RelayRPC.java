package relay;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler {

	public static final byte RELAY_COMMAND = 77;

	private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);
	private ConnectionConfiguration config;
	
	private Peer peer;

	public RelayRPC(Peer peer) {
		super(peer.getPeerBean(), peer.getConnectionBean(), RELAY_COMMAND);
		this.peer = peer;
		config = new DefaultConnectionConfiguration();
	}

	public FutureResponse setupRelay(PeerAddress other, final ChannelCreator channelCreator) {

		final Message message = createMessage(other, RELAY_COMMAND, Type.REQUEST_1);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), config);
		LOG.debug("send RPC message {}", message);
		requestHandler.sendTCP(channelCreator);

		return futureResponse;

	}

	@Override
	public Message handleResponse(Message message, PeerConnection peerConnection, boolean sign) throws Exception {
		if (!(message.getType() == Type.REQUEST_1 && message.getCommand() == RELAY_COMMAND)) {
			throw new IllegalArgumentException("Message content is wrong");
		}
		
		LOG.debug("received RPC message {}", message);
		
		if(peerBean().serverPeerAddress().isRelay()) {
			//peer is behind a NAT as well -> deny request
			return createResponseMessage(message, Type.DENIED);
		} else {
			PermanentConnectionRPC permanentConnection = new PermanentConnectionRPC(peer, message.getSender());
			peer.setDirectDataRPC(permanentConnection);
			return createResponseMessage(message, Type.OK);
		}
	}
}
