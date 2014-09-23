package net.tomp2p.rcon;

import java.util.ArrayList;
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
import net.tomp2p.relay.BaseRelayForwarderRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RPC handles two things. First of all, it makes sure that messaging via
 * reverse connection setup is possible. Second, it is also able to keep the
 * established {@link PeerConnection} and store it to the {@link PeerBean}. Rcon
 * means reverse connection.
 * 
 * @author jonaswagner
 * 
 */
public class RconRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(RconRPC.class);

	private final Peer peer;
	private final ConnectionConfiguration config;
	private static final int POSITION_ZERO = 0;

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
	 * REQUEST_1 = relay rcon forwarding.<br />
	 * REQUEST_2 = open a TCP channel and transmit {@link PeerConnection}.<br />
	 * REQUEST_3 = use now open {@link PeerConnection} to transmit original message
	 * (and eventually store the {@link PeerConnection}).<br />
	 * REQUEST_4 = store the {@link PeerConnection} on the unreachable peer side
	 * (only called via startSetupRcon from the PeerNAT)
	 * 
	 * @param message
	 * @param peerConnection
	 * @param sign
	 * @param responder
	 */
	@Override
	public void handleResponse(final Message message, final PeerConnection peerConnection, final boolean sign,
			final Responder responder) throws Exception {
		if (message.type() == Message.Type.REQUEST_1 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the relay peer
			LOG.info("handle RconForward for message: " + message);
			handleRconForward(message, responder);
		} else if (message.type() == Message.Type.REQUEST_2 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the unreachable peer
			LOG.info("handle RconSetup for message: " + message);
			handleRconSetup(message, responder);
		} else if (message.type() == Message.Type.REQUEST_3 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the requesting peer
			LOG.info("handle RconAfterconnect for message: " + message);
			handleRconAfterconnect(message, responder, peerConnection);
		} else if (message.type() == Message.Type.REQUEST_4 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the unreachable peer (only called if the PeerConnection should remain open)
			LOG.info("handle openConnection for message: " + message);
			handleOpenConnection(message, responder, peerConnection);
		} else {
			LOG.warn("received invalid RconRPC message {}", message);
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	/**
	 * This methods is responsible for forwarding the rconSetupMessage from the
	 * relay to the unreachable Peer. It extracts the already existing {@link PeerConnection} of the
	 * unreachable peer and forwards then a new
	 * message with {@link Type#REQUEST_2}.
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleRconForward(final Message message, final Responder responder) {
		// get the relayForwarderRPC via Dispatcher to retrieve the existing peerConnection
		final BaseRelayForwarderRPC forwarder = extractRelayForwarderRPC(message.recipient().peerId());
		if (forwarder != null) {
			final Message forwardMessage = createForwardMessage(message, forwarder.unreachablePeerAddress());
			forwarder.handleResponse(forwardMessage, responder);
		} else {
			handleFail(message, responder, "no relayForwarder Registered for peerId="
					+ message.recipient().peerId().toString());
		}
	}

	/**
	 * This method extracts a registered RelayForwarderRPC from the {@link Dispatcher}. This RelayForwarder
	 * can then be used to extract the {@link PeerConnection} to the unreachable Peer we want to contact.
	 * 
	 * @param unreachablePeerId the unreachable peer
	 * @return forwarder
	 */
	private BaseRelayForwarderRPC extractRelayForwarderRPC(Number160 unreachablePeerId) {
		final Dispatcher dispatcher = peer.connectionBean().dispatcher();
		final Map<Integer, DispatchHandler> ioHandlers = dispatcher.searchHandlerMap(unreachablePeerId);
		for (Map.Entry<Integer, DispatchHandler> element : ioHandlers.entrySet()) {
			if (element.getValue() instanceof BaseRelayForwarderRPC) {
				return (BaseRelayForwarderRPC) element.getValue();
			}
		}
		return null;
	}

	/**
	 * This method creates the message which is sent from the relay peer to the
	 * unreachable peer.
	 * 
	 * @param message
	 * @param peerConnection
	 * @return forwardMessage
	 */
	private Message createForwardMessage(final Message message, final PeerAddress recipient) {
		// creates the Message to forward to the unreachable peer
		Message forwardMessage = createMessage(recipient, RPC.Commands.RCON.getNr(), Message.Type.REQUEST_2);

		// transmit PeerAddress of reachablePeer
		final NeighborSet ns = new NeighborSet(1, new ArrayList<PeerAddress>(1));
		ns.add(message.sender());
		forwardMessage.neighborsSet(ns);

		// use same message id for new message to identify the cached message afterwards
		forwardMessage.messageId(message.messageId());

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
	 * peer that it is ready via a new message with {@link Type#REQUEST_3}.
	 * 
	 * @param message
	 * @param responder
	 * @throws TimeoutException
	 */
	private void handleRconSetup(final Message message, final Responder responder) throws TimeoutException {
		final PeerAddress originalSender;
		if (!message.neighborsSet(POSITION_ZERO).neighbors().isEmpty()) {
			// extract the PeerAddress from the reachable peer
			originalSender = (PeerAddress) message.neighborsSet(POSITION_ZERO).neighbors().toArray()[POSITION_ZERO];
			// create new PeerConnectin to the reachable peer
			final FuturePeerConnection fpc = peer.createPeerConnection(originalSender);
			fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
				@Override
				public void operationComplete(final FuturePeerConnection future) throws Exception {
					if (future.isSuccess()) {
						PeerConnection peerConnection = future.peerConnection();
						if (peerConnection != null) {
							final Message setupMessage = createSetupMessage(message, peerConnection);
							FutureResponse futureResponse = RelayUtils.send(peerConnection, peer.peerBean(),
									peer.connectionBean(), config, setupMessage);
							futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
								@Override
								public void operationComplete(final FutureResponse future) throws Exception {
									if (future.isSuccess()) {
										responder.response(createResponseMessage(message, Type.OK));
									} else {
										handleFail(message, responder,
												"Exception while setting up the reverse connection from the unreachable to the original peer!");
									}
								}
							});
						} else {
							handleFail(message, responder, "the peerConnection was null!");
						}
					} else {
						handleFail(message, responder, "no channel could be established");
					}
				}
			});
		} else {
			handleFail(message, responder, "the original sender was not transmittet in the neighborsSet!");
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
	private Message createSetupMessage(final Message message, final PeerConnection peerConnection) {
		Message setupMessage = createMessage(peerConnection.remotePeer(), RPC.Commands.RCON.getNr(), Message.Type.REQUEST_3);

		// use same message id for new message
		setupMessage.messageId(message.messageId());

		// keep the new connection open
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
	private void handleRconAfterconnect(final Message message, final Responder responder, final PeerConnection peerConnection) {
		// get the original message
		final ConcurrentHashMap<Integer, Message> cachedMessages = peer.connectionBean().sender().cachedMessages();
		final Message cachedMessage = cachedMessages.remove(message.messageId());
		if (cachedMessage != null) {
			FutureResponse futureResponse = RelayUtils.send(peerConnection, peer.peerBean(), peer.connectionBean(), config,
					cachedMessage.messageId(1));
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
				@Override
				public void operationComplete(final FutureResponse future) throws Exception {
					if (future.isSuccess()) {
						LOG.warn("Original Message was sent successfully to unreachablePeer with PeerAddress{"
								+ message.sender() + "}");
						// check if the PeerConnection should be stored in the PeerBean
						if (message.longAt(POSITION_ZERO) != null) {
							storePeerConnection(message, peerConnection, responder);
						} else {
							// we must make sure that the PeerConnection is
							// closed, because it takes a lot of resources from
							// the running pc
							responder.response(createResponseMessage(message.keepAlive(false), Type.OK));
							peerConnection.close();
						}
					} else {
						handleFail(message, responder, "The Original Message could not be sent!!!");
					}
				}
			});
		} else {
			handleFail(message, responder, "There was no original message for RconMessageId=" + message.messageId()
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
	private void handleOpenConnection(final Message message, final Responder responder, final PeerConnection peerConnection) {
		storePeerConnection(message, peerConnection, responder);
	}

	/**
	 * This method stores the now open {@link PeerConnection} of the unreachable
	 * peer to the {@link PeerBean}.
	 * 
	 * @param message
	 * @param peerConnection
	 */
	private void storePeerConnection(final Message message, final PeerConnection peerConnection, final Responder responder) {
		peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			@Override
			public void operationComplete(final FutureDone<Void> future) throws Exception {
				// remove the open PeerConnection to the other Peer from openPeerConnections in the PeerBean
				LOG.warn("Permanent PeerConnection to peer=" + message.sender() + " has been closed.");
				peer.peerBean().openPeerConnections().remove(message.sender().peerId());
			}
		});
		// put the now open PeerConnection into the openPeerConnections-Map in
		// the PeerBean
		final ConcurrentHashMap<Number160, PeerConnection> openPeerConnections = peer.peerBean().openPeerConnections();
		openPeerConnections.put(message.sender().peerId(), peerConnection);

		responseAndKeepAlive(message, responder);
	}

	/**
	 * This method checks and sets the keepAlive Flag on a message. The message
	 * must have set the keepAlive flag to true. If not, the PeerConnection to
	 * the other Peer closes itself (even if it is a relay).
	 * 
	 * @param message
	 * @param responder
	 */
	private void responseAndKeepAlive(final Message message, final Responder responder) {
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
	private void handleFail(final Message message, final Responder responder, final String failReason) {
		LOG.error(failReason);
		responder.response(createResponseMessage(message, Type.EXCEPTION));
	}
}
