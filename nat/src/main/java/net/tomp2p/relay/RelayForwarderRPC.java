package net.tomp2p.relay;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RelayForwarder is responsible for forwarding all messages that are
 * received on a relay peer, but are intended for an unreachable peer that is
 * connected to the relay peer.
 * 
 * @author Raphael Voellmy
 * 
 */
public class RelayForwarderRPC extends DispatchHandler implements PeerStatusListener {

	private final static Logger LOG = LoggerFactory.getLogger(RelayForwarderRPC.class);

	// connection to unreachable peer
	private final PeerConnection peerConnection;
	private List<Map<Number160, PeerStatatistic>> peerMap = null;
	private volatile PeerAddress unreachablePeer;
	private final RelayRPC relayRPC;

	/**
	 * 
	 * @param peerConnection
	 *            A peer connection to an unreachable peer that is permanently
	 *            open
	 * @param peer
	 *            The relay peer
	 */
	public RelayForwarderRPC(final PeerConnection peerConnection, final Peer peer, final RelayRPC relayRPC) {
		super(peer.peerBean(), peer.connectionBean());
		this.peerConnection = peerConnection.changeRemotePeer(peerConnection.remotePeer().changeRelayed(true));
		peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			@Override
            public void operationComplete(FutureDone<Void> future) throws Exception {
				peer.peerBean().removePeerStatusListeners(RelayForwarderRPC.this);
				peer.connectionBean().dispatcher().removeIoHandler(unreachablePeer.peerId());
            }
		});
		
		this.unreachablePeer = peerConnection.remotePeer().changeRelayed(true);
		this.relayRPC = relayRPC;
		LOG.debug("created forwarder from peer {} to peer {}", peer.peerAddress(), unreachablePeer);
	}
	
	@Override
    public boolean peerFailed(PeerAddress remotePeer, PeerException exception) {
	    //not handled here
	    return false;
    }

	@Override
    public boolean peerFound(PeerAddress remotePeer, PeerAddress referrer, PeerConnection peerConnection2) {
		boolean firstHand = referrer == null;
		boolean secondHand = remotePeer.equals(referrer);
		boolean samePeerConnection = peerConnection.equals(peerConnection2);
		//if firsthand, then full trust, if second hand and a stable peerconnection, we can trust as well
		if((firstHand || (secondHand && samePeerConnection)) 
				&& remotePeer.peerId().equals(unreachablePeer.peerId()) 
				&& remotePeer.isRelayed()) {
			//we got new information about this peer, e.g. its active relays
			LOG.debug("update the unreachable peer to {} based on {}, ref {}", unreachablePeer, remotePeer, referrer);
			unreachablePeer = remotePeer;
		}
	    return false;
    }
	
	public void register(Peer peer) {
		for (Commands command : RPC.Commands.values()) {
			if (command != RPC.Commands.RELAY) {
				peer.connectionBean().dispatcher()
				        .registerIoHandler(unreachablePeer.peerId(), this, command.getNr());
			}
		}
		peer.peerBean().addPeerStatusListeners(this);
	}
	
	public static void register(PeerConnection peerConnection, Peer peer, RelayRPC relayRPC) {
		RelayForwarderRPC relayForwarderRPC = new RelayForwarderRPC(peerConnection, peer, relayRPC);
		relayForwarderRPC.register(peer);
	}
	
	public static RelayForwarderRPC find(Peer peer, Number160 peerId) {
		//we can search for any command, except RELAY, which is not handled here
		return (RelayForwarderRPC) peer.connectionBean().dispatcher().searchHandler(
				peerId, RPC.Commands.NEIGHBOR.getNr());
    }

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnectionUnused, final boolean sign,
	        final Responder responder) throws Exception {
		//TODO
		// the sender should have the ip/port from the relay peer, the peerId
		// from the unreachable peer, in order to have 6 relays instead of 5
		final PeerAddress sender = unreachablePeer; 

		// special treatment for ping and neighbor
		if (message.command() == RPC.Commands.PING.getNr()) {
			LOG.debug("Received message {} to handle ping for unreachable peer {}", message, unreachablePeer);
			handlePing(message, responder, sender);
		} else if (message.command() == RPC.Commands.NEIGHBOR.getNr()) {
			LOG.debug("Received message {} to handle neighbor request for unreachable peer {}", message, unreachablePeer);
			handleNeigbhor(message, responder, sender);
		} else {
			LOG.debug("Received message {} to forward to unreachable peer {}", message, unreachablePeer);
			handleRelay(message, responder, sender);
		}
	}

	private void handleRelay(final Message message, final Responder responder, final PeerAddress sender)
	        throws InvalidKeyException, SignatureException, IOException {
		// Send message via direct message through the open connection to the
		// unreachable peer
		message.restoreContentReferences();
		final Buffer buf = RelayUtils.encodeMessage(message);

		FutureResponse fr = relayRPC.forwardMessage(peerConnection, buf);

		fr.addListener(new BaseFutureAdapter<FutureResponse>() {
			public void operationComplete(FutureResponse future) throws Exception {
				if (future.isSuccess()) {
					Buffer buffer = future.responseMessage().buffer(0);
					Message responseFromUnreachablePeer = RelayUtils.decodeMessage(buffer, message.recipientSocket(),
					        message.senderSocket());
					responseFromUnreachablePeer.restoreContentReferences();
					responseFromUnreachablePeer.sender(sender);
					responseFromUnreachablePeer.recipient(message.sender());
					LOG.debug("response from unreachable peer: {}", responseFromUnreachablePeer);
					responder.response(responseFromUnreachablePeer);
				} else {
					responder.failed(Type.USER1, "Relaying message failed: " + future.failedReason());
				}
			}
		});

	}

	private void handlePing(Message message, Responder responder, PeerAddress sender) {
		LOG.debug("peerconnection open? {}", peerConnection.isOpen());
		Message response = createResponseMessage(message, peerConnection.isOpen() ? Type.OK : Type.EXCEPTION, sender);
		responder.response(response);
	}

	public void handleNeigbhor(final Message message, Responder responder, PeerAddress sender) throws IOException {
		if (message.keyList().size() < 2) {
			throw new IllegalArgumentException("We need the location and domain key at least");
		}
		if (!(message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2
		        || message.type() == Type.REQUEST_3 || message.type() == Type.REQUEST_4)
		        && (message.command() == RPC.Commands.NEIGHBOR.getNr())) {
			throw new IllegalArgumentException("Message content is wrong");
		}
		Number160 locationKey = message.key(0);

		Collection<PeerAddress> neighbors = neighbors(locationKey, NeighborRPC.NEIGHBOR_SIZE);
		if (neighbors == null) {
			// return empty neighbor set
			Message response = createResponseMessage(message, Type.NOT_FOUND, sender);
			response.neighborsSet(new NeighborSet(-1, Collections.<PeerAddress>emptyList()));
			responder.response(response);
			return;
		}

		// Create response message and set neighbors
		final Message responseMessage = createResponseMessage(message, Type.OK, sender);
		
		//TODO: the relayed peer must be up-to-date here
		//neighbors.add(peerConnection.remotePeer());
		
		LOG.debug("found the following neighbors {}", neighbors);
		
		NeighborSet neighborSet = new NeighborSet(NeighborRPC.NEIGHBOR_LIMIT, neighbors);
		responseMessage.neighborsSet(neighborSet);
		
		//we can't do fast get here, as we only send over the neighbors and not the keys stored
		responder.response(responseMessage);
	}
	
	private SortedSet<PeerAddress> neighbors(Number160 id, int atLeast) {
        LOG.trace("Answering routing request on behalf of unreachable peer {}, neighbors of {}", unreachablePeer, id);
        if(peerMap == null) {
            return null;
        } else {
            return PeerMap.closePeers(unreachablePeer.peerId(), id, NeighborRPC.NEIGHBOR_SIZE, peerMap);
        }
    }
	
	public Collection<PeerAddress> all() {
		Collection<PeerStatatistic> result1 = new ArrayList<PeerStatatistic>();
		for(Map<Number160, PeerStatatistic> map:peerMap) {
			result1.addAll(map.values());
		}
		Collection<PeerAddress> result2 = new ArrayList<PeerAddress>();
	    for(PeerStatatistic peerStatatistic:result1) {
	    	result2.add(peerStatatistic.peerAddress());
	    }
	    return result2;
    }

	public void setMap(List<Map<Number160, PeerStatatistic>> peerMap) {
	    this.peerMap = peerMap;
    }
}
