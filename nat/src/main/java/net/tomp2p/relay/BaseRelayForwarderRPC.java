package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureResponse;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class that provides the basic functionality to handle relay requests
 * at the relay server. It also acts as a dispatcher for different kinds
 * of messages.
 * 
 * The RelayForwarder is responsible for forwarding all messages that are
 * received on a relay peer, but are intended for an unreachable peer that is
 * connected to the relay peer. Every unreachable node has an own instance of
 * this class at the relay server.
 * 
 * @author Nico Rutishauser
 * @author Raphael Voellmy
 *
 */
public abstract class BaseRelayForwarderRPC extends DispatchHandler implements PeerStatusListener {

	private final static Logger LOG = LoggerFactory.getLogger(BaseRelayForwarderRPC.class);

	private PeerAddress unreachablePeer;
	private List<Map<Number160, PeerStatatistic>> peerMap = null;

	public BaseRelayForwarderRPC(Peer peer, PeerConnection peerConnection) {
		super(peer.peerBean(), peer.connectionBean());
		this.unreachablePeer = peerConnection.remotePeer().changeRelayed(true);
	}

	public final PeerAddress getUnreachablePeerAddress() {
		return unreachablePeer;
	}

	protected final void setUnreachablePeerAddress(PeerAddress unreachablePeer) {
		assert unreachablePeer != null;
		this.unreachablePeer = unreachablePeer;
	}

	protected final Number160 getUnreachablePeerId() {
		return unreachablePeer.peerId();
	}

	@Override
	public boolean peerFailed(PeerAddress remotePeer, PeerException exception) {
		// not handled here
		return false;
	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder)
			throws Exception {
		// TODO
		// the sender should have the ip/port from the relay peer, the peerId
		// from the unreachable peer, in order to have 6 relays instead of 5
		final PeerAddress sender = getUnreachablePeerAddress();

		// special treatment for ping and neighbor
		if (message.command() == RPC.Commands.PING.getNr()) {
			LOG.debug("Received message {} to handle ping for unreachable peer {}", message, sender);
			handlePing(message, responder, sender);
		} else if (message.command() == RPC.Commands.NEIGHBOR.getNr()) {
			LOG.debug("Received message {} to handle neighbor request for unreachable peer {}", message, sender);
			handleNeigbhor(message, responder, sender);
		} else {
			LOG.debug("Received message {} to forward to unreachable peer {}", message, sender);
			handleRelay(message, responder, sender);
		}
	}
	
	/**
	 * When a message for the relay peer has been received
	 * @param message
	 * @param responder
	 * @param sender
	 */
	protected abstract void handleRelay(Message message, Responder responder, PeerAddress sender) throws Exception;

	/**
	 * When a ping message is received
	 * @param message
	 * @param responder
	 * @param sender
	 */
	protected abstract void handlePing(Message message, Responder responder, PeerAddress sender);

	/**
	 * When a neighbor message is received
	 * @param message
	 * @param responder
	 * @param sender
	 */
	protected void handleNeigbhor(final Message message, Responder responder, PeerAddress sender) {
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
        LOG.trace("Answering routing request on behalf of unreachable peer {}, neighbors of {}", getUnreachablePeerAddress(), id);
        if(peerMap == null) {
            return null;
        } else {
            return PeerMap.closePeers(getUnreachablePeerId(), id, NeighborRPC.NEIGHBOR_SIZE, peerMap);
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

	/**
	 * Send a message to the firewalled peer
	 * 
	 * @param message the message to send
	 * @return a future response
	 */
	public abstract FutureResponse sendSingle(final Message message);
}
