package net.tomp2p.relay;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);
	private final ConnectionConfiguration config;
	private final Peer peer;
	
	private RelayNeighborRPC relayNeighborRPC;
	private RelayForwarderRPC relayForwarderRPC;

	private RelayRPC(Peer peer) {
		super(peer.getPeerBean(), peer.getConnectionBean());
		register(RPC.Commands.RELAY.getNr());
		this.peer = peer;
		config = new DefaultConnectionConfiguration();
	}

	public static RelayRPC setup(Peer peer) {
		return new RelayRPC(peer);
	}
	
	public FutureResponse sendPeerMap(PeerAddress peerAddress, List<Map<Number160, PeerStatatistic>> map, FutureChannelCreator fcc) {
		final Message message = createMessage(peerAddress, RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
		message.setKeepAlive(true);
		//TODO: neighbor size limit is 256, we might have more here
		message.setNeighborsSet(new NeighborSet(-1, RelayUtils.flatten(map)));
		
		final FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
		        peerBean(), connectionBean(), config);
		
		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			public void operationComplete(FutureChannelCreator future) throws Exception {
	            if(future.isSuccess()) {
	            	requestHandler.sendTCP(future.getChannelCreator());
	            } else {
	            	futureResponse.setFailed(future);
	            }
            }
		});
		return futureResponse;
	}
	
	public FutureResponse forwardMessage(PeerConnection peerConnection, Buffer buf) {
		final Message message = createMessage(peerConnection.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_2);
		message.setKeepAlive(true);
		message.setBuffer(buf);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), config);
		return requestHandler.sendTCP(peerConnection);
    }		

	public FutureDone<PeerConnection> setupRelay(final ChannelCreator channelCreator, FuturePeerConnection fpc) {
		final FutureDone<PeerConnection> futureDone = new FutureDone<PeerConnection>();
		final Message message = createMessage(fpc.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_1);
		message.setKeepAlive(true);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
		        peerBean(), connectionBean(), config);
		LOG.debug("Setting up relay connection to peer {}, message {}", fpc.remotePeer(), message);

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
		LOG.debug("received RPC message {}", message);
		if(message.getType() == Type.REQUEST_1 && message.getCommand() == RPC.Commands.RELAY.getNr()) {
			handleSetup(message, peerConnection, responder);
		} else if (message.getType() == Type.REQUEST_2 && message.getCommand() == RPC.Commands.RELAY.getNr()) {
			handleData(message, responder);
		} else if (message.getType() == Type.REQUEST_3 && message.getCommand() == RPC.Commands.RELAY.getNr()) {
			handleMap(message, responder);
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	private void handleSetup(Message message, PeerConnection peerConnection, Responder responder) {
		if (peerBean().serverPeerAddress().isRelayed()) {
			// peer is behind a NAT as well -> deny request
			responder.response(createResponseMessage(message, Type.DENIED));
		} else {
			relayNeighborRPC = new RelayNeighborRPC(peer, peerConnection.remotePeer());
			relayForwarderRPC = new RelayForwarderRPC(peerConnection, peer, this);
			responder.response(createResponseMessage(message, Type.OK));
		}
	}
	
	private void handleData(Message message, Responder responderToRelay) throws Exception {
		Buffer requestBuffer = message.getBuffer(0);
		Message realMessage = RelayUtils.decodeMessage(requestBuffer, new InetSocketAddress(0), new InetSocketAddress(0));
		LOG.debug("Received message from relay peer: {}", realMessage);
		realMessage.restoreContentReferences();
        NoDirectResponse responder = new NoDirectResponse();
        //TODO: Not sure what to do with the peer connection and sign
        peer.getConnectionBean().dispatcher().getAssociatedHandler(realMessage).handleResponse(realMessage, null, false, responder);
        LOG.debug("Send reply message to relay peer: {}", responder.getResponse());
        Message response = createResponseMessage(message, Type.OK);
        response.setBuffer(RelayUtils.encodeMessage(responder.getResponse()));
        responderToRelay.response(response);
    }
	
	private void handleMap(Message message, Responder responder) {
		Collection<PeerAddress> map = message.getNeighborsSet(0).neighbors();
		if(relayNeighborRPC == null) {
			relayNeighborRPC.setMap(RelayUtils.unflatten(map, message.getSender()));
		} else {
			LOG.error("need to call setup relay first");
		}
		Message response = createResponseMessage(message, Type.OK);
		responder.response(response);
    }
	
	public RelayNeighborRPC relayNeighborRPC() {
		return relayNeighborRPC;
	}
	
	public RelayForwarderRPC relayForwarderRPC() {
		return relayForwarderRPC;
	}
}
