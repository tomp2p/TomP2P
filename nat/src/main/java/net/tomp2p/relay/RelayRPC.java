package net.tomp2p.relay;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.holep.HolePRPC;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.relay.buffer.BufferedRelayClient;
import net.tomp2p.relay.buffer.BufferedRelayServer;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler implements OfflineListener {

	private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);

	private final Peer peer;

	// Holds a map of server configuations for multiple relay types
	private final Map<RelayType, RelayServerConfig> serverConfigs;

	// holds the server for each client
	private final Map<Number160, BaseRelayServer> servers;

	// holds the client for each server
	private ConcurrentHashMap<Number160, BaseRelayClient> clients;

	/**
	 * This variable is needed, because a relay overwrites every RPC of an
	 * unreachable peer with another RPC called {@link RelayForwarderRPC}. This
	 * variable is forwarded to the {@link RelayForwarderRPC} in order to
	 * guarantee the existence of a {@link RconRPC}. Without this variable, no
	 * reverse connections would be possible.
	 * 
	 * @author jonaswagner
	 */
	private final RconRPC rconRPC;

	/**
	 * This variable is needed, because a relay overwrites every RPC of an
	 * unreachable peer with another RPC called {@link RelayForwarderRPC}. This
	 * variable is forwarded to the {@link RelayForwarderRPC} in order to
	 * guarantee the existence of a {@link HolePRPC}. Without this variable, no
	 * hole punch connections would be possible.
	 * 
	 * @author jonaswagner
	 */
	private final HolePRPC holePunchRPC;

	/**
	 * Register the RelayRPC. After the setup, the peer is ready to act as a
	 * relay if asked by an unreachable peer.
	 * 
	 * @param peer
	 *            The peer to register the RelayRPC
	 * @param rconRPC the reverse connection RPC
	 * @return
	 */
	public RelayRPC(Peer peer, RconRPC rconRPC, HolePRPC holePRPC, Map<RelayType, RelayServerConfig> serverConfigs) {
		super(peer.peerBean(), peer.connectionBean());
		this.peer = peer;
		this.serverConfigs = serverConfigs;
		this.servers = new ConcurrentHashMap<Number160, BaseRelayServer>();
		this.clients = new ConcurrentHashMap<Number160, BaseRelayClient>();
		this.rconRPC = rconRPC;
		this.holePunchRPC = holePRPC;

		// register this handler
		register(RPC.Commands.RELAY.getNr());
	}

	/**
	 * Receive a message at the relay server and the relay client
	 */
	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder)
			throws Exception {
		LOG.debug("Received RPC message {}", message);
		if (message.type() == Type.REQUEST_1 && message.command() == RPC.Commands.RELAY.getNr()) {
			// The relay server received a setup request from an unreachable peer
			handleSetup(message, peerConnection, responder);
		} else if (message.type() == Type.REQUEST_2 && message.command() == RPC.Commands.RELAY.getNr()) {
			// The unreachable peer receives wrapped messages from the relay
			handlePiggyBackedMessage(message, responder);
		} else if (message.type() == Type.REQUEST_3 && message.command() == RPC.Commands.RELAY.getNr()) {
			// the relay server receives the update of the routing table regularly from the unrachable peer
			handleMap(message, responder);
		} else if (message.type() == Type.REQUEST_4 && message.command() == RPC.Commands.RELAY.getNr()) {
			// An unreachable peer requests the buffer at the relay peer
			// or a buffer is transmitted to the unreachable peer directly
			handleBuffer(message, responder);
		} else if (message.type() == Type.REQUEST_5 && message.command() == RPC.Commands.RELAY.getNr()) {
			// A late response
			handleLateResponse(message, peerConnection, sign, responder);
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	public Peer peer() {
		return this.peer;
	}

	/**
	 * Convenience method
	 * 
	 * @return the signature factory
	 */
	private SignatureFactory signatureFactory() {
		return connectionBean().channelServer().channelServerConfiguration().signatureFactory();
	}

	/**
	 * Convenience method
	 * 
	 * @return the dispatcher of this peer
	 */
	private Dispatcher dispatcher() {
		return peer().connectionBean().dispatcher();
	}

	/**
	 * @return all unreachable peers currently connected to this relay node
	 */
	public Set<PeerAddress> unreachablePeers() {
		Set<PeerAddress> unreachablePeers = new HashSet<PeerAddress>(servers.size());
		for (BaseRelayServer forwarder : servers.values()) {
			unreachablePeers.add(forwarder.unreachablePeerAddress());
		}
		return unreachablePeers;
	}

	/**
	 * Add a client to the list
	 */
	public void addClient(BaseRelayClient connection) {
		clients.put(connection.relayAddress().peerId(), connection);
	}

	/**
	 * Remove a client from the list
	 */
	public void removeClient(BaseRelayClient connection) {
		clients.remove(connection.relayAddress().peerId());
	}

	/**
	 * Handle the setup where an unreachable peer connects to this one
	 */
	private void handleSetup(Message message, final PeerConnection peerConnection, Responder responder) {
		// The relay peer receives the setup message from the unreachable peer
		if (message.intList().isEmpty()) {
			throw new IllegalArgumentException("Setup message should contain an integer value specifying the type");
		}

		// get the relayType the client requests
		RelayType relayType = RelayType.values()[message.intAt(0)];

		if (serverConfigs.containsKey(relayType)) {
			BaseRelayServer server = serverConfigs.get(relayType).createServer(message, peerConnection, responder, peer);
			if (server != null) {
				server.addOfflineListener(this);
				registerRelayServer(server);
			}
		} else {
			LOG.warn("Relay client {} requested to serve as relay with type {}. This peer does not support this type.",
					message.sender(), relayType);
			responder.response(createResponseMessage(message, Type.DENIED));
		}
	}

	@Override
	public void onUnreachableOffline(PeerAddress unreachablePeer, BaseRelayServer server) {
		// clean up
		servers.remove(unreachablePeer);
		peerBean().removePeerStatusListener(server);
		connectionBean().dispatcher().removeIoHandler(peer.peerID(), unreachablePeer.peerId());
	}

	private void registerRelayServer(BaseRelayServer server) {
		for (Commands command : RPC.Commands.values()) {
			if (command == RPC.Commands.RCON) {
				// We must register the rconRPC for every unreachable peer that
				// we serve as a relay. Without this registration, no reverse
				// connection setup is possible.
				dispatcher().registerIoHandler(peer.peerID(), server.unreachablePeerId(), rconRPC, command.getNr());
			} else if (command == RPC.Commands.HOLEP) {
				// We must register the holePunchRPC for every unreachable peer that
				// we serve as a relay. Without this registration, no reverse
				// connection setup is possible.
				dispatcher().registerIoHandler(peer.peerID(), server.unreachablePeerId(), holePunchRPC, command.getNr());
			} else if (command == RPC.Commands.RELAY) {
				// Register this class to handle all relay messages (currently used when a slow message
				// arrives)
				dispatcher().registerIoHandler(peer.peerID(), server.unreachablePeerId(), this, command.getNr());
			} else {
				dispatcher().registerIoHandler(peer.peerID(), server.unreachablePeerId(), server, command.getNr());
			}
		}

		peer.peerBean().addPeerStatusListener(server);
		servers.put(server.unreachablePeerId(), server);
	}

	/**
	 * The unreachable peer received an envelope message with another message inside (piggypacked)
	 */
	private void handlePiggyBackedMessage(Message message, final Responder responderToRelay) throws Exception {
		// TODO: check if we have right setup

		// this contains the real sender
		Collection<PeerSocketAddress> peerSocketAddresses = message.peerSocketAddresses();
		final InetSocketAddress sender;
		if (!peerSocketAddresses.isEmpty()) {
			sender = PeerSocketAddress.createSocketTCP(peerSocketAddresses.iterator().next());
		} else {
			sender = new InetSocketAddress(0);
		}

		Buffer requestBuffer = message.buffer(0);
		Message realMessage = RelayUtils.decodeRelayedMessage(requestBuffer.buffer(), message.recipientSocket(), sender,
				signatureFactory());
		realMessage.restoreContentReferences();

		LOG.debug("Received message from relay peer: {}", realMessage);

		final Message envelope = createResponseMessage(message, Type.OK);
		final Responder responder = new Responder() {
			// TODO: add reply leak handler
			@Override
			public void response(Message responseMessage) {
				LOG.debug("Send reply message to relay peer: {}", responseMessage);
				try {
					if (responseMessage.sender().isRelayed() && !responseMessage.sender().peerSocketAddresses().isEmpty()) {
						responseMessage.peerSocketAddresses(responseMessage.sender().peerSocketAddresses());
					}
					envelope.buffer(RelayUtils.encodeMessage(responseMessage, signatureFactory()));
				} catch (Exception e) {
					LOG.error("Cannot piggyback the response", e);
					failed(Type.EXCEPTION, e.getMessage());
				}
				responderToRelay.response(envelope);
			}

			@Override
			public void failed(Type type, String reason) {
				responderToRelay.failed(type, reason);
			}

			@Override
			public void responseFireAndForget() {
				responderToRelay.responseFireAndForget();
			}
		};

		DispatchHandler dispatchHandler = dispatcher().associatedHandler(realMessage);
		if (dispatchHandler == null) {
			responder.failed(Type.EXCEPTION, "handler not found, probably not relaying peer anymore");
		} else {
			dispatchHandler.handleResponse(realMessage, null, false, responder);
		}
	}

	/**
	 * Updates the peer map of an unreachable peer on the relay peer, so that
	 * the relay peer can respond to neighbor RPC on behalf of the unreachable
	 * peer
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleMap(Message message, Responder responder) {
		LOG.debug("Handle foreign map update {}", message);
		BaseRelayServer server = servers.get(message.sender().peerId());
		if (server != null) {
			Collection<PeerAddress> map = message.neighborsSet(0).neighbors();
			Message response = createResponseMessage(message, Type.OK);
			server.setPeerMap(RelayUtils.unflatten(map, message.sender()), message, response);
			responder.response(response);
		} else {
			LOG.error("No forwarder for peer {} found. Need to setup relay first");
			responder.response(createResponseMessage(message, Type.NOT_FOUND));
		}
	}

	/**
	 * There are two cases, when this method is called:<br>
	 * <ul>
	 * <li>The relay buffers messages for unreachable peers (like Android devices). They get notified when the
	 * buffer is full or request the buffer content by themselves through this request.</li>
	 * <li>The relay peer has buffered the messages and is able to transmit them through an already existing
	 * channel (e.g. in buffered tcp case). The unreachable peer can then process the buffer directly, without
	 * the need to obtain it at the relay peer.</li>
	 * </ul>
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleBuffer(final Message message, final Responder responder) {
		BaseRelayServer server = servers.get(message.sender().peerId());
		BaseRelayClient client = clients.get(message.sender().peerId());
		if (server != null && server instanceof BufferedRelayServer) {
			LOG.debug("Handle buffer request from unreachable peer {} to server", message.sender());
			BufferedRelayServer bufferedServer = (BufferedRelayServer) server;
			Message response = createResponseMessage(message, Type.OK);

			// add all buffered messages
			Buffer bufferedMessages = bufferedServer.collectBufferedMessages();
			if (bufferedMessages != null) {
				response.buffer(bufferedMessages);
			}

			LOG.debug("Responding all buffered messages to Android device {}", message.sender());
			responder.response(response);
		} else if (client != null && client instanceof BufferedRelayClient) {
			LOG.debug("Handle message with buffer from server {} to unreachable client", message.sender());
			BufferedRelayClient bufferedClient = (BufferedRelayClient) client;
			FutureDone<Void> futureDone = new FutureDone<Void>();
			bufferedClient.onReceiveMessageBuffer(message, futureDone);
			futureDone.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
				@Override
				public void operationComplete(FutureDone<Void> future) throws Exception {
					// all buffered messages have been processed or at least started to process
					responder.response(createResponseMessage(message, Type.OK));
				}
			});
		} else {
			responder.failed(Type.EXCEPTION, "This message type is intended for buffering forwarders only");
		}
	}

	/**
	 * There are two possibilites for this case:
	 * <ol>
	 * <li>This peer did a request which was now finally answered by a slow peer.</li>
	 * <li>This is the relay peer of the requester which now receives the late response</li>
	 * </ol>
	 * 
	 * @param message contains the (piggybacked) response
	 * @param responder
	 * @param peerConnection
	 * @param sign
	 */
	private void handleLateResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) {
		if (!message.sender().isSlow() || message.bufferList().isEmpty()) {
			throw new IllegalArgumentException(
					"Late response does not come from slow peer or does not contain the buffered message");
		}

		Message realMessage = null;
		try {
			realMessage = RelayUtils.decodeRelayedMessage(message.buffer(0).buffer(), message.recipientSocket(),
					message.senderSocket(), signatureFactory());
		} catch (Exception e) {
			LOG.error("Cannot decode the late response", e);
			responder.response(createResponseMessage(message, Type.EXCEPTION));
			return;
		}

		LOG.debug("Received late response from slow peer: {}", realMessage);
		// only the case when a unreachable peer makes a request to another slow, unreachable peer
		Map<Integer, FutureResponse> pendingRequests = dispatcher().getPendingRequests();
		FutureResponse pendingRequest = pendingRequests.remove(realMessage.messageId());
		if (pendingRequest != null) {
			// we waited for this response, answer it
			pendingRequest.response(realMessage);

			// send ok, not fire and forget - style
			LOG.debug("Successfully answered pending request {} with {}", pendingRequest.request(), realMessage);
			responder.response(createResponseMessage(message, Type.OK, message.recipient()));
		} else if (peer().peerAddress().isSlow()) {
			// we're a slow peer but the pending request was not found. Don't send a reply, else we might end
			// in a loop. Just trust in the timeout at the requester (might also be this peer).
			LOG.error("No pending request found for message {}. Ignore it.", realMessage);
		} else {
			// handle Relayed <--> Relayed.
			// This could be a pending message for one of the relayed peers, not for this peer
			BaseRelayServer forwarder = servers.get(realMessage.recipient().peerId());
			if (forwarder == null) {
				LOG.error("Forwarder for the relayed peer not found. Cannot send late response {}", realMessage);
				responder.response(createResponseMessage(message, Type.NOT_FOUND));
			} else {
				LOG.debug("We're just a relay peer. Send wrapped late response to requester wrapper: {} content: {}",
						message, realMessage);
				// because buffer is re-encoded when forwarding it to unreachable
				message.restoreBuffers();
				forwarder.forwardToUnreachable(message);
			}
		}
	}
}
