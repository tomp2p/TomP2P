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
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
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

	public final PeerAddress unreachablePeerAddress() {
		return unreachablePeer;
	}

	protected final void unreachablePeerAddress(PeerAddress unreachablePeer) {
		assert unreachablePeer != null;
		this.unreachablePeer = unreachablePeer;
	}

	protected final Number160 unreachablePeerId() {
		return unreachablePeer.peerId();
	}

	@Override
	public boolean peerFailed(PeerAddress remotePeer, PeerException exception) {
		// not handled here
		return false;
	}

	/**
	 * Receive a message at the relay server from a given peer
	 */
	@Override
	public final void handleResponse(Message message, PeerConnection peerConnection, boolean sign, final Responder responder)
			throws Exception {
		// TODO
		// the sender should have the ip/port from the relay peer, the peerId
		// from the unreachable peer, in order to have 6 relays instead of 5
		handleResponse(message, responder);
	}

	/**
	 * Receive a message at the relay server from a given peer
	 */
	public final void handleResponse(Message message, final Responder responder) {
		// special treatment for ping and neighbor
		if (message.command() == RPC.Commands.PING.getNr()) {
			LOG.debug("Received message {} to handle ping for unreachable peer {}", message, unreachablePeer);
			handlePing(message, responder, unreachablePeer);
		} else if (message.command() == RPC.Commands.NEIGHBOR.getNr()) {
			LOG.debug("Received message {} to handle neighbor request for unreachable peer {}", message, unreachablePeer);
			handleNeigbhor(message, responder, unreachablePeer);
		} else {
			LOG.debug("Received message {} to forward to unreachable peer {}", message, unreachablePeer);
			FutureDone<Message> response = forwardToUnreachable(message);
			response.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
				@Override
				public void operationComplete(FutureDone<Message> future) throws Exception {
					if (future.isSuccess()) {
						Message answerMessage = future.object();
						LOG.debug("Returing from relay to requester: {}", answerMessage);
						responder.response(answerMessage);
					} else {
						responder.failed(Type.USER1, "Relaying message failed: " + future.failedReason());
					}
				}
			});
		}
	}

	/**
	 * Forwards a message to the unrachable peer. The implementation should notify the unreachable peer
	 * and return a response as soon as possible.
	 * 
	 * @param message the message that is intended for the unreachable peer
	 * @param sender the requester
	 * @return the response to the requester
	 */
	public abstract FutureDone<Message> forwardToUnreachable(Message message);

	/**
	 * When a ping message is received
	 * 
	 * @param message
	 * @param responder
	 * @param sender
	 */
	protected abstract void handlePing(Message message, Responder responder, PeerAddress sender);

	/**
	 * When a neighbor message is received
	 * 
	 * @param message
	 * @param responder
	 * @param sender
	 */
	private void handleNeigbhor(final Message message, Responder responder, PeerAddress sender) {
		if (message.keyList().size() < 2) {
			throw new IllegalArgumentException("We need the location and domain key at least");
		}
		if (!(message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2 || message.type() == Type.REQUEST_3 || message
				.type() == Type.REQUEST_4) && (message.command() == RPC.Commands.NEIGHBOR.getNr())) {
			throw new IllegalArgumentException("Message content is wrong");
		}
		Number160 locationKey = message.key(0);

		Collection<PeerAddress> neighbors = getNeighbors(locationKey, NeighborRPC.NEIGHBOR_SIZE);
		if (neighbors == null) {
			// return empty neighbor set
			Message response = createResponseMessage(message, Type.NOT_FOUND, sender);
			response.neighborsSet(new NeighborSet(-1, Collections.<PeerAddress> emptyList()));
			responder.response(response);
			return;
		}

		// Create response message and set neighbors
		final Message responseMessage = createResponseMessage(message, Type.OK, sender);

		// TODO: the relayed peer must be up-to-date here
		// neighbors.add(peerConnection.remotePeer());

		LOG.debug("found the following neighbors {}", neighbors);

		NeighborSet neighborSet = new NeighborSet(NeighborRPC.NEIGHBOR_LIMIT, neighbors);
		responseMessage.neighborsSet(neighborSet);

		// we can't do fast get here, as we only send over the neighbors and not the keys stored
		responder.response(responseMessage);
	}

	private SortedSet<PeerAddress> getNeighbors(Number160 id, int atLeast) {
		LOG.trace("Answering routing request on behalf of unreachable peer {}, neighbors of {}", unreachablePeerAddress(),
				id);
		if (peerMap == null) {
			return null;
		} else {
			return PeerMap.closePeers(unreachablePeerId(), id, NeighborRPC.NEIGHBOR_SIZE, peerMap);
		}
	}

	/**
	 * Returns the current peer map from the mobile device
	 */
	public final Collection<PeerAddress> getPeerMap() {
		Collection<PeerAddress> peerAddresses = new ArrayList<PeerAddress>();
		if (peerMap == null || peerMap.isEmpty()) {
			return peerAddresses;
		}

		Collection<PeerStatatistic> statistics = new ArrayList<PeerStatatistic>();
		for (Map<Number160, PeerStatatistic> map : peerMap) {
			statistics.addAll(map.values());
		}
		for (PeerStatatistic peerStatatistic : statistics) {
			peerAddresses.add(peerStatatistic.peerAddress());
		}
		return peerAddresses;
	}

	/**
	 * Update the peerMap of the unreachable peer
	 */
	public final void setPeerMap(List<Map<Number160, PeerStatatistic>> peerMap) {
		this.peerMap = peerMap;
	}
}
