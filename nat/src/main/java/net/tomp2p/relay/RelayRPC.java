package net.tomp2p.relay;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.relay.android.AndroidForwarderRPC;
import net.tomp2p.relay.android.AndroidOfflineListener;
import net.tomp2p.relay.android.MessageBufferConfiguration;
import net.tomp2p.relay.android.gcm.GCMSenderRPC;
import net.tomp2p.relay.android.gcm.IGCMSender;
import net.tomp2p.relay.android.gcm.RemoteGCMSender;
import net.tomp2p.relay.tcp.OpenTCPForwarderRPC;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);
	private final Peer peer;
	private final ConnectionConfiguration config;

	// only used to serve unreachable android devices
	private final MessageBufferConfiguration bufferConfig;

	// holds the forwarder for each client
	private final Map<Number160, BaseRelayForwarderRPC> forwarders;

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
	 * In case this relay handles Android devices and is capable of sending GCM messages, this variable is
	 * used. If this relay needs to server Android devices but is not capable of sending GCM messages, GCM
	 * requests are forwarded to another {@link GCMSenderRPC} at another peer. In this case, this variable is
	 * <code>null</code>.
	 */
	private final IGCMSender gcmSenderRPC;

	/**
	 * Register the RelayRPC. After the setup, the peer is ready to act as a
	 * relay if asked by an unreachable peer.
	 * 
	 * @param peer
	 *            The peer to register the RelayRPC
	 * @param rconRPC the reverse connection RPC
	 * @param bufferConfig
	 * @param gcmSendRetries
	 * @param gcmAuthenticationKey
	 * @param gcmAuthToken the authentication key for Google cloud messaging
	 * @return
	 */
	public RelayRPC(Peer peer, RconRPC rconRPC, IGCMSender gcmSenderRPC, MessageBufferConfiguration bufferConfig,
			ConnectionConfiguration config) {
		super(peer.peerBean(), peer.connectionBean());
		this.peer = peer;
		this.gcmSenderRPC = gcmSenderRPC;
		this.bufferConfig = bufferConfig;
		this.config = config;
		this.forwarders = new ConcurrentHashMap<Number160, BaseRelayForwarderRPC>();
		this.rconRPC = rconRPC;

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
			// The relay peer receives the setup message from the unreachable peer
			if (message.intList().isEmpty()) {
				throw new IllegalArgumentException("Setup message should contain an integer value specifying the type");
			}

			Integer deviceType = message.intAt(0);
			if (deviceType == RelayType.OPENTCP.ordinal()) {
				// request from unreachable peer to the relay
				handleSetupTCP(message, peerConnection, responder);
			} else if (deviceType == RelayType.ANDROID.ordinal()) {
				// request from mobile device to the relay
				handleSetupAndroid(message, peerConnection, responder);
			} else {
				throw new IllegalArgumentException("Unknown relay type: " + deviceType);
			}
		} else if (message.type() == Type.REQUEST_2 && message.command() == RPC.Commands.RELAY.getNr()) {
			// The unreachable peer receives wrapped messages from the relay
			handlePiggyBackedMessage(message, responder);
		} else if (message.type() == Type.REQUEST_3 && message.command() == RPC.Commands.RELAY.getNr()) {
			// the relay server receives the update of the routing table regularly from the unrachable peer
			handleMap(message, responder);
		} else if (message.type() == Type.REQUEST_4 && message.command() == RPC.Commands.RELAY.getNr()) {
			// An android unreachable peer requests the buffer
			handleBufferRequest(message, responder);
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
		Set<PeerAddress> unreachablePeers = new HashSet<PeerAddress>(forwarders.size());
		for (BaseRelayForwarderRPC forwarder : forwarders.values()) {
			unreachablePeers.add(forwarder.unreachablePeerAddress());
		}
		return unreachablePeers;
	}

	/**
	 * Open a TCP connection to the unreachable peer
	 */
	private void handleSetupTCP(Message message, final PeerConnection peerConnection, Responder responder) {
		if (peerBean().serverPeerAddress().isRelayed()) {
			// peer is behind a NAT as well -> deny request
			LOG.warn("I cannot be a relay since I'm relayed as well! {}", message);
			responder.response(createResponseMessage(message, Type.DENIED));
			return;
		}

		// register relay forwarder
		OpenTCPForwarderRPC tcpForwarder = new OpenTCPForwarderRPC(peerConnection, peer, config);
		registerRelayForwarder(tcpForwarder);

		LOG.debug("I'll be your relay! {}", message);
		responder.response(createResponseMessage(message, Type.OK));
	}

	/**
	 * An android device is behind a firewall and wants to be relayed
	 */
	private void handleSetupAndroid(Message message, final PeerConnection peerConnection, Responder responder) {
		/** The registration ID */
		if (message.bufferList().size() < 1) {
			LOG.error("Device {} did not send any GCM registration id", peerConnection.remotePeer());
			responder.response(createResponseMessage(message, Type.DENIED));
			return;
		}

		String registrationId = RelayUtils.decodeString(message.buffer(0));
		if (registrationId == null) {
			LOG.error("Cannot decode the registrationID from the message");
			responder.response(createResponseMessage(message, Type.DENIED));
			return;
		}

		/** Update interval */
		Integer mapUpdateInterval = message.intAt(1);
		if (mapUpdateInterval == null) {
			LOG.error("Android device did not send the peer map update interval.");
			responder.response(createResponseMessage(message, Type.DENIED));
			return;
		}

		/** GCM handing */
		IGCMSender sender = null;
		if (message.neighborsSetList().isEmpty()) {
			// no known GCM servers, check GCM ability of this peer
			if (gcmSenderRPC == null) {
				LOG.error("This relay is unable to serve unreachable Android devices because no GCM Authentication Key is configured");
				responder.response(createResponseMessage(message, Type.DENIED));
				return;
			} else {
				sender = gcmSenderRPC;
			}
		} else {
			// device sent well-known GCM servers to use
			Collection<PeerAddress> gcmServers = message.neighborsSet(0).neighbors();
			sender = new RemoteGCMSender(peer, this, config, gcmServers);
		}

		LOG.debug("Hello Android device! You'll be relayed over GCM. {}", message);
		AndroidForwarderRPC forwarderRPC = new AndroidForwarderRPC(peer, peerConnection.remotePeer(), bufferConfig,
				registrationId, sender, mapUpdateInterval, new AndroidOfflineListener() {
					@Override
					public void onAndroidOffline() {
						forwarders.remove(peerConnection.remotePeer());
					}
				});
		registerRelayForwarder(forwarderRPC);

		responder.response(createResponseMessage(message, Type.OK));
	}

	private void registerRelayForwarder(BaseRelayForwarderRPC forwarder) {
		for (Commands command : RPC.Commands.values()) {
			if (command == RPC.Commands.RCON) {
				// We must register the rconRPC for every unreachable peer that
				// we serve as a relay. Without this registration, no reverse
				// connection setup is possible.
				dispatcher().registerIoHandler(peer.peerID(), forwarder.unreachablePeerId(), rconRPC, command.getNr());
			} else if (command == RPC.Commands.RELAY) {
				// Register this class to handle all relay messages (currently used when a slow message
				// arrives)
				dispatcher().registerIoHandler(peer.peerID(), forwarder.unreachablePeerId(), this, command.getNr());
			} else {
				dispatcher().registerIoHandler(peer.peerID(), forwarder.unreachablePeerId(), forwarder, command.getNr());
			}
		}
		
		peer.peerBean().addPeerStatusListener(forwarder);
		forwarders.put(forwarder.unreachablePeerId(), forwarder);
	}

	/**
	 * The unreachable peer received an envelope message with another message insice (piggypacked)
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
		Message realMessage = RelayUtils.decodeRelayedMessage(requestBuffer, message.recipientSocket(), sender,
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
		Collection<PeerAddress> map = message.neighborsSet(0).neighbors();
		BaseRelayForwarderRPC forwarder = forwarders.get(message.sender().peerId());
		if (forwarder != null) {
			forwarder.setPeerMap(RelayUtils.unflatten(map, message.sender()));
		} else {
			LOG.error("No forwarder for peer {} found. Need to setup relay first");
			responder.response(createResponseMessage(message, Type.NOT_FOUND));
			return;
		}

		Message response = createResponseMessage(message, Type.OK);
		if(forwarder instanceof AndroidForwarderRPC) {
			AndroidForwarderRPC androidForwarder = ((AndroidForwarderRPC) forwarder);
			if (message.neighborsSet(1) != null) {
				// update the GCM servers
				androidForwarder.changeGCMServers(message.neighborsSet(1).neighbors());
			}
			
			// Use the situation to send the buffer to the mobile phone
			Buffer bufferedMessages = androidForwarder.collectBufferedMessages();
			if(bufferedMessages != null) {
				response.buffer(bufferedMessages);
			}
		}
		
		responder.response(response);
	}

	/**
	 * The relay buffers messages for unreachable peers (like Android devices). They get notified when the
	 * buffer is full
	 * or request the buffer content by themselves through this request.
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleBufferRequest(Message message, Responder responder) {
		LOG.debug("Handle buffer request of unreachable peer {}", message.sender());
		BaseRelayForwarderRPC forwarderRPC = forwarders.get(message.sender().peerId());
		if (forwarderRPC instanceof AndroidForwarderRPC) {
			AndroidForwarderRPC androidForwarder = (AndroidForwarderRPC) forwarderRPC;

			try {
				Message response = createResponseMessage(message, Type.OK);
				
				// add all buffered messages
				Buffer bufferedMessages = androidForwarder.collectBufferedMessages();
				if(bufferedMessages != null) {
					response.buffer(bufferedMessages);
				}

				LOG.debug("Responding all buffered messages to Android device {}", message.sender());
				responder.response(response);
			} catch (Exception e) {
				LOG.error("Cannot respond with buffered messages.", e);
				responder.response(createResponseMessage(message, Type.EXCEPTION));
			}
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
			realMessage = RelayUtils.decodeRelayedMessage(message.buffer(0), message.recipientSocket(),
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
			BaseRelayForwarderRPC forwarder = forwarders.get(realMessage.recipient().peerId());
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