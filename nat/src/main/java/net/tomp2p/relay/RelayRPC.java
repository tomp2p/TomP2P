package net.tomp2p.relay;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler {

	private static final Logger logger = LoggerFactory.getLogger(RelayRPC.class);
	private ConnectionConfiguration config;

	private Peer peer;

	private RelayRPC(Peer peer) {
		super(peer.getPeerBean(), peer.getConnectionBean());
		register(RPC.Commands.RELAY.getNr());
		this.peer = peer;
		config = new DefaultConnectionConfiguration();
	}

	public static RelayRPC setup(Peer peer) {
		return new RelayRPC(peer);
	}

	public FutureDone<PeerConnection> setupRelay(final ChannelCreator channelCreator, FuturePeerConnection fpc) {
		logger.debug("Setting up relay connection to peer {}", fpc.remotePeer());

		final FutureDone<PeerConnection> futureDone = new FutureDone<PeerConnection>();

		final Message message = createMessage(fpc.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_1);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
		        peerBean(), connectionBean(), config);
		logger.debug("send RPC message {}", message);

		fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
			public void operationComplete(final FuturePeerConnection futurePeerConnection) throws Exception {
				if (futurePeerConnection.isSuccess()) {
					requestHandler.sendTCP(channelCreator, futurePeerConnection.getObject()).addListener(
					        new BaseFutureAdapter<FutureResponse>() {
						        public void operationComplete(FutureResponse future) throws Exception {
							        if (future.isSuccess()) {
								        futureDone.setDone(futurePeerConnection.getObject());
							        } else {
								        futureDone.setFailed(future);
							        }
						        }
					        });
				} else {
					futureDone.setFailed(futurePeerConnection);
				}
			}
		});
		return futureDone;
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign,
	        Responder responder) throws Exception {
		if (!(message.getType() == Type.REQUEST_1 && message.getCommand() == RPC.Commands.RELAY.getNr())) {
			throw new IllegalArgumentException("Message content is wrong");
		}

		logger.debug("received RPC message {}", message);

		if (peerBean().serverPeerAddress().isRelayed()) {
			// peer is behind a NAT as well -> deny request
			responder.response(createResponseMessage(message, Type.DENIED));
		} else {
			new RelayForwarder(peerConnection, peer);
			responder.response(createResponseMessage(message, Type.OK));
		}
	}
}
