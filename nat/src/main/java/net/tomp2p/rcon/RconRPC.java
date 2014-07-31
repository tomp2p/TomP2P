package net.tomp2p.rcon;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final int POSITION_ZERO = 0;
	private final Peer peer;
	private final ConnectionConfiguration config;
	private static final Logger LOG = LoggerFactory.getLogger(RconRPC.class);

	public RconRPC(Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.RCON.getNr());
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
	}

	/**
	 * REQUEST_1 = relay rcon forwarding. REQUEST_2 = open socket and transmit
	 * PeerConnection. REQUEST_3 = use now open PeerConnection to transmit
	 * original message (and eventually store the {@link PeerConnection}).
	 * (optional) REQUEST_4 = store the {@link PeerConnection} on the
	 * unreachable peer side
	 */
	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder)
			throws Exception {
		LOG.warn("received RconRPC message {}", message);
		if (message.type() == Message.Type.REQUEST_1 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the relay peer
			LOG.warn("handle RconForward for message: " + message);
			handleRconForward(message, responder);
		} else if (message.type() == Message.Type.REQUEST_2 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the unreachable peer
			LOG.warn("handle RconSetup for message: " + message);
			handleRconSetup(message, responder);
		} else if (message.type() == Message.Type.REQUEST_3 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the requesting peer
			LOG.warn("handle RconAfterconnect for message: " + message);
			handleRconAfterconnect(message, responder, peerConnection);
		} else if (message.type() == Message.Type.REQUEST_4 && message.command() == RPC.Commands.RCON.getNr()) {
			// only called if the PeerConnection should remain open (startSetupRcon(...))
			LOG.warn("handle openConnection for message: " + message);
			handleOpenConnection(message, responder, peerConnection);
		} else {
			throw new IllegalArgumentException("Message content is wrong");
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
	private void handleOpenConnection(Message message, Responder responder, PeerConnection peerConnection) {
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
	private void storePeerConnection(Message message, final PeerConnection peerConnection) {
		// extract the amount of seconds which the connection should remain open
		long current = message.longAt(POSITION_ZERO);
		Integer seconds = (int) current;

		// insert the connection to a HashMap and store it on the PeerBean
		final Map<PeerConnection, Integer> connection = Collections
				.synchronizedMap(new HashMap<PeerConnection, Integer>());
		connection.put(peerConnection, seconds);

		peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			@Override
			public void operationComplete(FutureDone<Void> future) throws Exception {
				connection.remove(peerConnection);
			}
		});

		ConcurrentHashMap<Number160, HashMap<PeerConnection, Integer>> openPeerConnections = peer.peerBean()
				.openPeerConnections();
		openPeerConnections.put(message.sender().peerId(), (HashMap<PeerConnection, Integer>) connection);
	}

	private void responseAndKeepAlive(Message message, Responder responder) {
		if (message.isKeepAlive()) {
			responder.response(createResponseMessage(message, Type.OK));
		} else {
			message.keepAlive(true);
			responder.response(createResponseMessage(message, Type.OK));
		}
	}

	/**
	 * This method takes the now established {@link PeerConnection} to the
	 * unreachable peer and sends the original created message to it.
	 * 
	 * @param message
	 * @param responder
	 * @param peerConnection
	 */
	private void handleRconAfterconnect(final Message message, final Responder responder,
			final PeerConnection peerConnection) {
		// get the original message
		ConcurrentHashMap<Integer, Message> cachedMessages = peer.connectionBean().sender().cachedMessages();

		Message cachedMessage = cachedMessages.remove(message.messageId());
		if (cachedMessage != null) {
			FutureResponse futureResponse = new FutureResponse(cachedMessage);
			futureResponse = RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(),
					peer.connectionBean(), config);
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {

				@Override
				public void operationComplete(FutureResponse future) throws Exception {
					if (future.isSuccess()) {
						LOG.debug("Original Message sent to unreachablePeer with PeerAddress{" + message.sender() + "}");
						checkStorePeerConnection(message, peerConnection);
						responseAndKeepAlive(message, responder);
					} else {
						LOG.error("The Original Message could not be sent!!!");
						responder.response(createResponseMessage(message, Type.EXCEPTION));
					}
				}
			});

		} else {
			LOG.error("There was no original message for RconMessageId=" + message.messageId()
					+ "! This should not happen!!!");
		}

	}

	/**
	 * This method checks if the PeerConnection should be stored in the
	 * PeerBean.
	 * 
	 * @param message
	 * @param peerConnection
	 */
	private void checkStorePeerConnection(Message message, PeerConnection peerConnection) {
		if (message.longAt(POSITION_ZERO) != null) {
			storePeerConnection(message, peerConnection);
		}
	}

	/**
	 * This method handles the reverse connection setup on the unreachable peer
	 * side. It extracts the PeerAddress from the reachable peer and creates a
	 * new {@link PeerConnection} to it. Then it informs the reachable peer that
	 * it is ready via a new message with {@link Type}.REQUEST3.
	 * 
	 * @param message
	 * @param responder
	 * @throws TimeoutException
	 */
	private void handleRconSetup(final Message message, final Responder responder) throws TimeoutException {
		PeerAddress originalSender = null;

		if (message.neighborsSet(POSITION_ZERO).neighbors().isEmpty()) {
			LOG.error("the original sender was not transmittet in the neighborsSet!");
		} else {
			// extract the PeerAddress from the reachable peer
			originalSender = (PeerAddress) message.neighborsSet(POSITION_ZERO).neighbors().toArray()[POSITION_ZERO];

			final FuturePeerConnection fpc = peer.createPeerConnection(originalSender);

			fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {

				@Override
				public void operationComplete(FuturePeerConnection future) throws Exception {
					if (future.isSuccess()) {
						PeerConnection peerConnection = future.peerConnection();
						if (peerConnection != null) {
							Message setupMessage = createSetupMessage(message, peerConnection);

							FutureResponse futureResponse = new FutureResponse(setupMessage);
							futureResponse = RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(),
									peer.connectionBean(), config);
							futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {

								@Override
								public void operationComplete(FutureResponse future) throws Exception {
									if (future.isSuccess()) {
										responder.response(createResponseMessage(message, Type.OK));
									} else {
										LOG.error("Exception while setting up the reverse connection from the unreachable to the original peer!");
										responder.response(createResponseMessage(message, Type.EXCEPTION));
									}
								}
							});
						} else {
							LOG.error("the peerConnection was null!");
							responder.response(createResponseMessage(message, Type.EXCEPTION));
						}
					} else {
						LOG.error("no channel could be established");
						responder.response(createResponseMessage(message, Type.EXCEPTION));
					}
				}
			});
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
	private Message createSetupMessage(final Message message, PeerConnection peerConnection) {
		Message setupMessage = new Message();
		setupMessage.type(Message.Type.REQUEST_3);
		setupMessage.command(RPC.Commands.RCON.getNr());
		setupMessage.sender(peer.peerAddress());
		setupMessage.recipient(peerConnection.remotePeer());
		setupMessage.version(1); // TODO remove magic number and find out why
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
	 * This methods is responsible for forwarding the rconSetupMessage to the
	 * unreachable Peer. It extracts the already existing {@link PeerConnection}
	 * to the unreachable peer and forwards then a new message with {@link Type}
	 * .REQUEST_2.
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleRconForward(final Message message, final Responder responder) {

		// the open peerConnection to the
		// unreachable peer
		PeerConnection peerConnection = null;

		// get the relayForwarderRPC via Dispatcher to retrieve the existing
		// peerConnection
		RelayForwarderRPC relayForwarderRPC = extractRelayForwarderRPC(message);

		if (relayForwarderRPC != null) {
			peerConnection = relayForwarderRPC.peerConnection();

			Message forwardMessage = createForwardMessage(message, peerConnection);

			// we don't want to use another sendDirect anymore since we don't
			// have to send data
			FutureResponse futureResponse = new FutureResponse(forwardMessage);
			futureResponse = RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(),
					peer.connectionBean(), config);
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {

				@Override
				public void operationComplete(FutureResponse future) throws Exception {
					if (future.isSuccess()) {
						// Indicate the reachable peer that the message was
						// successfully forwarded
						responder.response(createResponseMessage(message, Type.OK));
					} else {
						LOG.error("Exception while forwarding the rconMessage to the unreachable");
						responder.response(createResponseMessage(message, Type.EXCEPTION));
					}

				}
			});

		} else {
			LOG.error("no relayForwarder Registered for peerId=" + message.recipient().peerId().toString());
			responder.response(createResponseMessage(message, Type.UNKNOWN_ID));
		}
	}

	/**
	 * This method extracts a registered RelayForwarderRPC from the
	 * {@link Dispatcher}. This RelayForwarder can then be used to extract the
	 * {@link PeerConnection} to the unreachable Peer we want to contact.
	 * 
	 * @param {@link Message} message
	 * @return {@link RelayForwarderRPC} relayForwarderRPC
	 */
	private RelayForwarderRPC extractRelayForwarderRPC(Message message) {
		RelayForwarderRPC relayForwarderRPC = null;
		Dispatcher dispatcher = peer.connectionBean().dispatcher();
		Map<Integer, DispatchHandler> ioHandlers = dispatcher.searchHandlerMap(message.recipient().peerId());
		for (Map.Entry<Integer, DispatchHandler> element : ioHandlers.entrySet()) {
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
	private Message createForwardMessage(Message message, PeerConnection peerConnection) {
		// create Message to forward
		Message forwardMessage = new Message();
		forwardMessage.type(Message.Type.REQUEST_2);
		forwardMessage.command(RPC.Commands.RCON.getNr());
		forwardMessage.sender(peer.peerAddress());
		forwardMessage.recipient(peerConnection.remotePeer());
		forwardMessage.version(1); // TODO remove magic number and find out why
									// we need the versionnumber

		// transmit PeerAddress of reachablePeer
		NeighborSet ns = new NeighborSet(1);
		ns.add(message.sender());
		forwardMessage.neighborsSet(ns);

		// use same message id for new message
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
}
