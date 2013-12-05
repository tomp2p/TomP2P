package net.tomp2p.relay;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDirect;
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

	private static final Logger logger = LoggerFactory.getLogger(RelayRPC.class);
	private ConnectionConfiguration config;

	private Peer peer;

	public RelayRPC(Peer peer) {
		super(peer.getPeerBean(), peer.getConnectionBean(), RELAY_COMMAND);
		this.peer = peer;
		config = new DefaultConnectionConfiguration();
	}

	public RelayConnectionFuture setupRelay(final PeerAddress other, final ChannelCreator channelCreator) {
		logger.debug("Setting up relay connection to peer {}", other);

		final RelayConnectionFuture connectionFuture = new RelayConnectionFuture(other);

		final Message message = createMessage(other, RELAY_COMMAND, Type.REQUEST_1);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), config);
		logger.debug("send RPC message {}", message);
		requestHandler.sendTCP(channelCreator);

		futureResponse.addListener(new BaseFutureListener<FutureResponse>() {
			@Override
			public void operationComplete(FutureResponse future) throws Exception {
				if (future.isSuccess() && future.getResponse().getType() == Type.OK) {
					logger.debug("Peer {} is ready to act as a relay", other);
					openPermanentConnection(connectionFuture, other);
				} else if (future.getResponse() != null && future.getResponse().getType() == Type.DENIED){
					connectionFuture.setFailed("Peer " + other + " denied to act as a relay. The peer is probably behind a relay, too");
				} else {
					connectionFuture.setFailed("Relay RPC failed: " + future.getFailedReason());
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.error("Error creating connection to relay peer {}: {}", other, t);
				connectionFuture.setFailed(t);
				connectionFuture.done();
			}
		});

		return connectionFuture;

	}

	private void openPermanentConnection(final RelayConnectionFuture rcf, final PeerAddress other) {
		// create permanent peer connection to relay peer
		final FuturePeerConnection fpc = peer.createPeerConnection(other);
		FutureDirect fd = peer.sendDirect(fpc).setObject(true).start();
		fd.addListener(new BaseFutureListener<FutureDirect>() {
			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				if(future.isSuccess()) {
					rcf.futurePeerConnection(fpc);
					rcf.done();
				}
			}
			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				rcf.setFailed(t);
			}
		});
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
		if (!(message.getType() == Type.REQUEST_1 && message.getCommand() == RELAY_COMMAND)) {
			throw new IllegalArgumentException("Message content is wrong");
		}

		logger.debug("received RPC message {}", message);

		if (peerBean().serverPeerAddress().isRelay()) {
			// peer is behind a NAT as well -> deny request
			responder.response(createResponseMessage(message, Type.DENIED));
		} else {
			PermanentConnectionRPC permanentConnection = new PermanentConnectionRPC(peer, message.getSender());
			peer.setDirectDataRPC(permanentConnection);
			responder.response(createResponseMessage(message, Type.OK));
		}
	}
}
