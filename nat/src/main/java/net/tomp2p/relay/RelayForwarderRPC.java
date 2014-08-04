package net.tomp2p.relay;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
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
import net.tomp2p.rcon.RconRPC;
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
public class RelayForwarderRPC extends DispatchHandler {

	private final static Logger LOG = LoggerFactory.getLogger(RelayForwarderRPC.class);

	// connection to unreachable peer
	private final PeerConnection peerConnection;
	private List<Map<Number160, PeerStatatistic>> peerMap = null;

	private final RelayRPC relayRPC;

	/**
	 * This variable is needed, because a relay peer overwrites every RPC of an
	 * unreachable peer with another RPC called {@link RelayForwarderRPC}. It
	 * guarantees the existence of a {@link RconRPC} object in the iohandlers
	 * map of the {@link Dispatcher}. Without this variable, no reverse
	 * connections would be possible.
	 * 
	 * @author jonaswagner
	 */
	private final RconRPC rconRPC;

	/**
	 * 
	 * @param peerConnection
	 *            A peer connection to an unreachable peer that is permanently
	 *            open
	 * @param peer
	 *            The relay peer
	 */
	public RelayForwarderRPC(PeerConnection peerConnection, Peer peer, RelayRPC relayRPC, RconRPC rconRPC) {
		super(peer.peerBean(), peer.connectionBean());
		PeerAddress unreachablePeer = peerConnection.remotePeer();
		this.peerConnection = peerConnection;
		this.relayRPC = relayRPC;
		this.rconRPC = rconRPC;
		LOG.debug("created forwarder from peer {} to peer {}", peer.peerAddress(), unreachablePeer);
	}

	public void register(Peer peer) {
		for (Commands command : RPC.Commands.values()) {
			if (command != RPC.Commands.RELAY && command != RPC.Commands.RCON) {
				peer.connectionBean().dispatcher().registerIoHandler(peerConnection.remotePeer().peerId(), this, command.getNr());
			} else if (command == RPC.Commands.RCON) {
				// We must register the rconRPC for every unreachable peer that
				// we serve as a relay. Without this registration, no reverse
				// connection setup is possible.
				peer.connectionBean().dispatcher()
						.registerIoHandler(peerConnection.remotePeer().peerId(), rconRPC, RPC.Commands.RCON.getNr());
			}
		}
	}

	public static void register(PeerConnection peerConnection, Peer peer, RelayRPC relayRPC, RconRPC rconRPC) {
		RelayForwarderRPC relayForwarderRPC = new RelayForwarderRPC(peerConnection, peer, relayRPC, rconRPC);
		relayForwarderRPC.register(peer);
	}

	// TODO: make sure if a peerconnection is dead, unregister is called
	public static void unregister(Peer peer, Number160 unreachablePeer) {
		peer.connectionBean().dispatcher().removeIoHandler(unreachablePeer);
	}

	public static RelayForwarderRPC find(Peer peer, Number160 peerId) {
		// we can search for any command, except RELAY, which is not handled
		// here
		return (RelayForwarderRPC) peer.connectionBean().dispatcher().searchHandler(peerId, RPC.Commands.NEIGHBOR.getNr());
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnectionUnused, final boolean sign, final Responder responder)
			throws Exception {
		// the sender should have the ip/port from the releay peer, the peerId
		// from the unreachabel peer
		final PeerAddress sender = peerBean().serverPeerAddress().changePeerId(peerConnection.remotePeer().peerId());

		// special treatment for ping and neighbor
		if (message.command() == RPC.Commands.PING.getNr()) {
			LOG.debug("Received message {} to handle ping for unreachable peer {}", message, peerConnection.remotePeer());
			handlePing(message, responder, sender);
		} else if (message.command() == RPC.Commands.NEIGHBOR.getNr()) {
			LOG.debug("Received message {} to handle neighbor request for unreachable peer {}", message, peerConnection.remotePeer());
			handleNeigbhor(message, responder, sender);
		} else {
			LOG.debug("Received message {} to forward to unreachable peer {}", message, peerConnection.remotePeer());
			handleRelay(message, responder, sender);
		}
	}

	private void handleRelay(final Message message, final Responder responder, final PeerAddress sender) throws InvalidKeyException,
			SignatureException, IOException {
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
		if (!(message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2 || message.type() == Type.REQUEST_3 || message.type() == Type.REQUEST_4)
				&& (message.command() == RPC.Commands.NEIGHBOR.getNr())) {
			throw new IllegalArgumentException("Message content is wrong");
		}
		Number160 locationKey = message.key(0);

		SortedSet<PeerAddress> neighbors = neighbors(locationKey, NeighborRPC.NEIGHBOR_SIZE);
		if (neighbors == null) {
			// return empty neighbor set
			Message response = createResponseMessage(message, Type.NOT_FOUND, sender);
			response.neighborsSet(new NeighborSet(-1));
			responder.response(response);
			return;
		}

		// Create response message and set neighbors
		final Message responseMessage = createResponseMessage(message, Type.OK, sender);

		LOG.debug("found the following neighbors {}", neighbors);
		NeighborSet neighborSet = new NeighborSet(NeighborRPC.NEIGHBOR_LIMIT, neighbors);
		responseMessage.neighborsSet(neighborSet);

		// we can't do fast get here, as we only send over the neighbors and not
		// the keys stored
		responder.response(responseMessage);
	}

	private SortedSet<PeerAddress> neighbors(Number160 id, int atLeast) {
		LOG.trace("Answering routing request on behalf of unreachable peer {}, neighbors of {}", peerConnection.remotePeer(), id);
		if (peerMap == null) {
			return null;
		} else {
			return PeerMap.closePeers(peerConnection.remotePeer().peerId(), id, NeighborRPC.NEIGHBOR_SIZE, peerMap);
		}
	}

	public Collection<PeerAddress> all() {
		Collection<PeerStatatistic> result1 = new ArrayList<PeerStatatistic>();
		for (Map<Number160, PeerStatatistic> map : peerMap) {
			result1.addAll(map.values());
		}
		Collection<PeerAddress> result2 = new ArrayList<PeerAddress>();
		for (PeerStatatistic peerStatatistic : result1) {
			result2.add(peerStatatistic.peerAddress());
		}
		return result2;
	}

	public void setMap(List<Map<Number160, PeerStatatistic>> peerMap) {
		this.peerMap = peerMap;
	}

	public PeerConnection peerConnection() {
		return peerConnection;
	}
}
