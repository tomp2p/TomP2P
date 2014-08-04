package net.tomp2p.rcon;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayForwarderRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RPC handles two things. First of all, it makes sure that messaging via
 * reverse connection setup is possible. Second, it is also able to keep the
 * established @link {@link PeerConnection} and store it to the @link
 * {@link PeerBean}.
 * 
 * @author jonaswagner
 * 
 */
public class RconRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(RconRPC.class);

	private final Peer peer;
	private final ConnectionConfiguration config;
	private static final int POSITION_ZERO = 0;
	private static final int MESSAGE_VERSION = 1;

	public RconRPC(final Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.RCON.getNr());
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
	}

	/**
	 * This method is called from the {@link Dispatcher} and handles the reverse
	 * connection at each step.
	 * 
	 * REQUEST_1 = relay rcon forwarding. REQUEST_2 = open a TCP channel and
	 * transmit {@link PeerConnection}. REQUEST_3 = use now open
	 * {@link PeerConnection} to transmit original message (and eventually store
	 * the {@link PeerConnection}). REQUEST_4 = store the {@link PeerConnection}
	 * on the unreachable peer side (only called via startSetupRcon from the
	 * PeerNAT)
	 * 
	 * @param message
	 * @param peerConnection
	 * @param sign
	 * @param responder
	 */
	@Override
	public void handleResponse(final Message message,
			final PeerConnection peerConnection, final boolean sign,
			final Responder responder) throws Exception {
		LOG.warn("received RconRPC message {}", message);
		if (message.type() == Message.Type.REQUEST_1
				&& message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the relay peer
			LOG.warn("handle RconForward for message: " + message);
			handleRconForward(message, responder);
		} else if (message.type() == Message.Type.REQUEST_2
				&& message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the unreachable peer
			LOG.warn("handle RconSetup for message: " + message);
			handleRconSetup(message, responder);
		} else if (message.type() == Message.Type.REQUEST_3
				&& message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the requesting peer
			LOG.warn("handle RconAfterconnect for message: " + message);
			handleRconAfterconnect(message, responder, peerConnection);
		} else if (message.type() == Message.Type.REQUEST_4
				&& message.command() == RPC.Commands.RCON.getNr()) {
			// only called if the PeerConnection should remain open
			LOG.warn("handle openConnection for message: " + message);
			handleOpenConnection(message, responder, peerConnection);
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	/**
	 * This methods is responsible for forwarding the rconSetupMessage from the
	 * relay to the unreachable Peer. It extracts the already existing
	 * {@link PeerConnection} of the unreachable peer and forwards then a new
	 * message with {@link Type} .REQUEST_2.
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleRconForward(final Message message,
			final Responder responder) {
		// the existing peerConnection to the
		// unreachable peer
		final PeerConnection peerConnection;
		// get the relayForwarderRPC via Dispatcher to retrieve the existing
		// peerConnection
		final RelayForwarderRPC relayForwarderRPC = extractRelayForwarderRPC(message);
		if (relayForwarderRPC != null) {
			peerConnection = relayForwarderRPC.peerConnection();
			final Message forwardMessage = createForwardMessage(message,
					peerConnection);
			// we don't want to use another sendDirect anymore since we don't
			// have to send data, thats why we use sendSingle(...)
			FutureResponse futureResponse = new FutureResponse(forwardMessage);
			futureResponse = RelayUtils.sendSingle(peerConnection,
					futureResponse, peer.peerBean(), peer.connectionBean(),
					config);
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
				@Override
				public void operationComplete(final FutureResponse future)
						throws Exception {
					if (future.isSuccess()) {
						// Indicate the reachable peer that the message was
						// successfully forwarded
						responder.response(createResponseMessage(message,
								Type.OK));
					} else {
						handleFail(message, responder,
								"Exception while forwarding the rconMessage to the unreachable");
					}
				}
			});
		} else {
			handleFail(message, responder,
					"no relayForwarder Registered for peerId="
							+ message.recipient().peerId().toString());
		}
	}

	/**
	 * This method extracts a registered RelayForwarderRPC from the
	 * {@link Dispatcher}. This RelayForwarder can then be used to extract the
	 * {@link PeerConnection} to the unreachable Peer we want to contact.
	 * 
	 * @param message
	 * @return relayForwarderRPC
	 */
	private RelayForwarderRPC extractRelayForwarderRPC(final Message message) {
		RelayForwarderRPC relayForwarderRPC = null;
		final Dispatcher dispatcher = peer.connectionBean().dispatcher();
		final Map<Integer, DispatchHandler> ioHandlers = dispatcher
				.searchHandlerMap(message.recipient().peerId());
		for (Map.Entry<Integer, DispatchHandler> element : ioHandlers
				.entrySet()) {
			if (element.getValue().getClass().equals(RelayForwarderRPC.class)) {
				relayForwarderRPC = (RelayForwarderRPC) element.getValue();
				break;
			}
		}
		return relayForwarderRPC;
	}

	/**
	 * This method creates the message which is sent from the relay peer to the
	 * unreachable peer.
	 * 
	 * @param message
	 * @param peerConnection
	 * @return forwardMessage
	 */
	private Message createForwardMessage(final Message message,
			final PeerConnection peerConnection) {
		// creates the Message to forward to the unreachable peer
		final Message forwardMessage = new Message();
		forwardMessage.type(Message.Type.REQUEST_2);
		forwardMessage.command(RPC.Commands.RCON.getNr());
		forwardMessage.sender(peer.peerAddress());
		forwardMessage.recipient(peerConnection.remotePeer());
		forwardMessage.version(MESSAGE_VERSION); // TODO jwa remove magic number
													// and find out why
		// we need the versionnumber

		// transmit PeerAddress of reachablePeer
		final NeighborSet ns = new NeighborSet(1);
		ns.add(message.sender());
		forwardMessage.neighborsSet(ns);

		// use same message id for new message to identify the cached message
		// afterwards
		forwardMessage.messageId(message.messageId());

		// we need to keep the peerConnection between the relay and the
		// unreachable peer open
		forwardMessage.keepAlive(true);

		// check if we keep the connection open afterwards
		if (!(message.longAt(POSITION_ZERO) == null)) {
			forwardMessage.longValue(message.longAt(POSITION_ZERO));
		}

		return forwardMessage;
	}

	/**
	 * This method handles the reverse connection setup on the unreachable peer
	 * side. It extracts the {@link PeerAddress} from the reachable peer and
	 * creates a new {@link PeerConnection} to it. Then it informs the reachable
	 * peer that it is ready via a new message with {@link Type}.REQUEST3.
	 * 
	 * @param message
	 * @param responder
	 * @throws TimeoutException
	 */
	private void handleRconSetup(final Message message,
			final Responder responder) throws TimeoutException {
		final PeerAddress originalSender;
		if (!message.neighborsSet(POSITION_ZERO).neighbors().isEmpty()) {
			// extract the PeerAddress from the reachable peer
			originalSender = (PeerAddress) message.neighborsSet(POSITION_ZERO)
					.neighbors().toArray()[POSITION_ZERO];
			// create new PeerConnectin to the reachable peer
			final FuturePeerConnection fpc = peer
					.createPeerConnection(originalSender);
			fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
				@Override
				public void operationComplete(final FuturePeerConnection future)
						throws Exception {
					if (future.isSuccess()) {
						PeerConnection peerConnection = future.peerConnection();
						if (peerConnection != null) {
							final Message setupMessage = createSetupMessage(
									message, peerConnection);
							FutureResponse futureResponse = new FutureResponse(
									setupMessage);
							futureResponse = RelayUtils.sendSingle(
									peerConnection, futureResponse,
									peer.peerBean(), peer.connectionBean(),
									config);
							futureResponse
									.addListener(new BaseFutureAdapter<FutureResponse>() {
										@Override
										public void operationComplete(
												final FutureResponse future)
												throws Exception {
											if (future.isSuccess()) {
												responder
														.response(createResponseMessage(
																message,
																Type.OK));
											} else {
												handleFail(
														message,
														responder,
														"Exception while setting up the reverse connection from the unreachable to the original peer!");
											}
										}
									});
						} else {
							handleFail(message, responder,
									"the peerConnection was null!");
						}
					} else {
						handleFail(message, responder,
								"no channel could be established");
					}
				}
			});
		} else {
			handleFail(message, responder,
					"the original sender was not transmittet in the neighborsSet!");
		}
	}

	/**
	 * This method creates the Message which is sent from the unreachable peer
	 * to the reachable peer.
	 * 
	 * @param message
	 * @param peerConnection
	 * @return setupMessage
	 */
	private Message createSetupMessage(final Message message,
			final PeerConnection peerConnection) {
		Message setupMessage = new Message();
		setupMessage.type(Message.Type.REQUEST_3);
		setupMessage.command(RPC.Commands.RCON.getNr());
		setupMessage.sender(peer.peerAddress());
		setupMessage.recipient(peerConnection.remotePeer());
		setupMessage.version(MESSAGE_VERSION); // TODO remove magic number and
												// find out why
												// we need the versionnumber

		// use same message id for new message
		setupMessage.messageId(message.messageId());
		setupMessage.keepAlive(true);

		// check if we keep the connection open afterwards
		if (!(message.longAt(POSITION_ZERO) == null)) {
			setupMessage.longValue(message.longAt(POSITION_ZERO));
		}

		return setupMessage;
	}

	/**
	 * This method takes the now established {@link PeerConnection} to the
	 * unreachable peer and sends the original created message to it.
	 * 
	 * @param message
	 * @param responder
	 * @param peerConnection
	 */
	private void handleRconAfterconnect(final Message message,
			final Responder responder, final PeerConnection peerConnection) {
		// get the original message
		final ConcurrentHashMap<Integer, Message> cachedMessages = peer
				.connectionBean().sender().cachedMessages();
		final Message cachedMessage = cachedMessages
				.remove(message.messageId());
		if (cachedMessage != null) {
			FutureResponse futureResponse = new FutureResponse(cachedMessage);
			futureResponse = RelayUtils.sendSingle(peerConnection,
					futureResponse, peer.peerBean(), peer.connectionBean(),
					config);
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
				@Override
				public void operationComplete(final FutureResponse future)
						throws Exception {
					if (future.isSuccess()) {
						LOG.debug("Original Message was sent successfully to unreachablePeer with PeerAddress{"
								+ message.sender() + "}");
						// check if the PeerConnection should be stored in the
						// PeerBean
						if (message.longAt(POSITION_ZERO) != null) {
							storePeerConnection(message, peerConnection);
						}
						responseAndKeepAlive(message, responder);
					} else {
						handleFail(message, responder,
								"The Original Message could not be sent!!!");
					}
				}
			});
		} else {
			handleFail(message, responder,
					"There was no original message for RconMessageId="
							+ message.messageId()
							+ "! This should not happen!!!");
		}
	}

	/**
	 * This method is only invoked if startSetupRcon() is called. It stores the
	 * peerConnection on the unreachable peer side.
	 * 
	 * @param message
	 * @param responder
	 * @param peerConnection
	 */
	private void handleOpenConnection(final Message message,
			final Responder responder, final PeerConnection peerConnection) {
		storePeerConnection(message, peerConnection);
		responseAndKeepAlive(message, responder);
	}

	/**
	 * This method stores the now open {@link PeerConnection} of the unreachable
	 * peer to the {@link PeerBean}.
	 * 
	 * @param message
	 * @param peerConnection
	 */
	private void storePeerConnection(final Message message,
			final PeerConnection peerConnection) {
		// extract the amount of seconds which the connection should remain open
		final long current = message.longAt(POSITION_ZERO);
		final Integer seconds = (int) current;

		// TODO jwa check concurrency!
		// insert the connection to a HashMap and store it on the PeerBean
		// final Map<PeerConnection, Integer> connection = Collections
		// .synchronizedMap(new HashMap<PeerConnection, Integer>());
		final HashMap<PeerConnection, Integer> connection = new HashMap<PeerConnection, Integer>();
		connection.put(peerConnection, seconds);
		peerConnection.closeFuture().addListener(
				new BaseFutureAdapter<FutureDone<Void>>() {
					@Override
					public void operationComplete(final FutureDone<Void> future)
							throws Exception {
						// remove the open PeerConnection to the other Peer from
						// openPeerConnections in the PeerBean
						LOG.debug("Permanent PeerConnection to peer="
								+ message.sender() + " has been closed.");
						peer.peerBean().openPeerConnections()
								.remove(message.sender().peerId());
					}
				});
		// put the now open PeerConnection into the openPeerConnections-Map in
		// the PeerBean
		final ConcurrentHashMap<Number160, HashMap<PeerConnection, Integer>> openPeerConnections = peer
				.peerBean().openPeerConnections();
		openPeerConnections.put(message.sender().peerId(), connection);
	}

	/**
	 * This method checks and sets the keepAlive Flag on a message. The message
	 * must have set the keepAlive flag to true. If not, the PeerConnection to
	 * the other Peer closes itself (even if it is a relay).
	 * 
	 * @param message
	 * @param responder
	 */
	private void responseAndKeepAlive(final Message message,
			final Responder responder) {
		if (message.isKeepAlive()) {
			responder.response(createResponseMessage(message, Type.OK));
		} else {
			message.keepAlive(true);
			responder.response(createResponseMessage(message, Type.OK));
		}
	}

	/**
	 * This method is called if something went wrong while the reverse
	 * connection setup. It responds then with a {@link Type}.EXCEPTION message.
	 * 
	 * @param message
	 * @param responder
	 * @param failReason
	 */
	private void handleFail(final Message message, final Responder responder,
			final String failReason) {
		LOG.error(failReason);
		responder.response(createResponseMessage(message, Type.EXCEPTION));
	}
}
