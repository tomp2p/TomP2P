package net.tomp2p.relay;

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
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.utils.MessageUtils;

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
	 * 
	 * @param message
	 * @param peerConnection
	 * @param sign
	 * @param responder
	 */
	@Override
	public void handleResponse(final Message message, final PeerConnection peerConnection, final boolean sign,
			final Responder responder) throws Exception {
		LOG.warn("received RconRPC message {}", message);
		if (message.type() == Message.Type.REQUEST_1 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the relay peer
			LOG.debug("handle RconForward for message: " + message);
			handleRconForward(message, responder);
		} else if (message.type() == Message.Type.REQUEST_2 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the unreachable peer
			LOG.debug("handle RconSetup for message: " + message);
			handleRconSetup(message, responder);
		} else if (message.type() == Message.Type.REQUEST_3 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the requesting peer
			LOG.debug("handle RconAfterconnect for message: " + message);
			handleRconAfterconnect(message, responder, peerConnection);
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
		final BaseRelayForwarderRPC forwarder = extractRelayForwarder(message);
		if (forwarder != null) {
			final Message forwardMessage = createForwardMessage(message, forwarder.unreachablePeerAddress());
			forwarder.handleResponse(forwardMessage, responder);
		} else {
			handleFail(message, responder, "No RelayForwarder registered for peerId="
					+ message.recipient().peerId().toString());
		}
	}

	/**
	 * This method extracts a registered {@link BaseRelayForwarderRPC} from the {@link Dispatcher}. This RelayForwarder
	 * can then be used to extract the {@link PeerConnection} to the unreachable Peer we want to contact.
	 * 
	 * @param unreachablePeerId the unreachable peer
	 * @return forwarder
	 */
	private BaseRelayForwarderRPC extractRelayForwarder(final Message message) {
		final Dispatcher dispatcher = peer.connectionBean().dispatcher();
		final Map<Integer, DispatchHandler> ioHandlers = dispatcher.searchHandlerMap(peer.peerID(), message.recipient().peerId());
		for (DispatchHandler handler : ioHandlers.values()) {
			if (handler instanceof BaseRelayForwarderRPC) {
				return (BaseRelayForwarderRPC) handler;
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

		if (!message.intList().isEmpty()) {
			// store the message id for new message to identify the cached message afterwards
			forwardMessage.intValue(message.intAt(0));
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
		if (!message.neighborsSetList().isEmpty()) {
			// extract the PeerAddress from the reachable peer
			final PeerAddress originalSender = (PeerAddress) message.neighborsSetList().get(0).neighbors().toArray()[0];
			// create new PeerConnectin to the reachable peer
			final FuturePeerConnection fpc = peer.createPeerConnection(originalSender);
			fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
				@Override
				public void operationComplete(final FuturePeerConnection future) throws Exception {
					if (future.isSuccess() && future.peerConnection() != null) {
						// successfully created a connection from unreachable to the requester
						final PeerConnection peerConnection = future.peerConnection();
						
						final Message readyMessage = createReadyForRequestMessage(message, peerConnection.remotePeer());
						FutureResponse futureResponse = MessageUtils.send(peerConnection, peer.peerBean(),
								peer.connectionBean(), config, readyMessage);
						futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
							@Override
							public void operationComplete(final FutureResponse future) throws Exception {
								// ready message is sent to reachable peer
								if (future.isSuccess()) {
									// store the connection if necessary
									if(message.intList().isEmpty()) {
										storePeerConnection(message, peerConnection);
									}
									
									// keep the connection open until at least next message arrives
									responder.response(createResponseMessage(message, Type.OK).keepAlive(true));
								} else {
									LOG.error("Cannot setup the reverse connection to the peer. Reason: {}", future.failedReason());
									handleFail(message, responder,
											"Exception while setting up the reverse connection from the unreachable to the original peer!");
								}
							}
						});
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
	 * This method creates a message to indicate that the connection is ready for requests.
	 * It is sent from the unreachable peer to the reachable peer.
	 * 
	 * @param message
	 * @param receiver
	 * @return setupMessage
	 */
	private Message createReadyForRequestMessage(final Message message, final PeerAddress receiver) {
		Message readyForRequestMessage = createMessage(receiver, RPC.Commands.RCON.getNr(), Message.Type.REQUEST_3);

		if (!message.intList().isEmpty()) {
			// forward the message id to indentify the cached message afterwards
			readyForRequestMessage.intValue(message.intAt(0));
		}

		// keep the new connection open
		readyForRequestMessage.keepAlive(true);

		return readyForRequestMessage;
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
		// get the original message or create one for permanent connection
		if (message.intList().isEmpty()) {
			LOG.debug("This reverse connection is used permanently. Store it!");
			storePeerConnection(message, peerConnection);
			responder.response(createResponseMessage(message, Type.OK).keepAlive(true));
		} else {
			ConcurrentHashMap<Integer, FutureResponse> cachedRequests = peer.connectionBean().sender().cachedRequests();
			final FutureResponse cachedRequest = cachedRequests.remove(message.intAt(0));
			final Message cachedMessage = cachedRequest.request();
			LOG.debug("This reverse connection is only used for sending a direct message {}", cachedMessage);

			// send the message to the unreachable peer through the open channel
			FutureResponse futureResponse = MessageUtils.send(peerConnection, peer.peerBean(), peer.connectionBean(), config,
					cachedMessage);
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
				@Override
				public void operationComplete(final FutureResponse future) throws Exception {
					if (future.isSuccess()) {
						LOG.debug("Successfully transmitted request message {} to unreachablePeer {}", cachedMessage,
								peerConnection.remotePeer());
						cachedRequest.response(future.responseMessage());
						
						// we must make sure that the PeerConnection is closed, because it takes a lot of
						// resources from the running pc
						Message responseMessage = createResponseMessage(message, Type.OK).keepAlive(false);
						LOG.debug("Returning OK for delivering single message over reverse connection {}", responseMessage);
						responder.response(responseMessage);
						peerConnection.close();
					} else {
						cachedRequest.failed("Cannot send the request message", future);
						handleFail(message, responder, "The AfterConnectMessage could not be sent!");
					}
				}
			});
		}
	}

	/**
	 * This method stores the now open {@link PeerConnection} of the unreachable
	 * peer to the {@link PeerBean}.
	 * 
	 * @param message
	 * @param peerConnection
	 */
	private void storePeerConnection(final Message message, final PeerConnection peerConnection) {
		peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			@Override
			public void operationComplete(final FutureDone<Void> future) throws Exception {
				// remove the open PeerConnection to the other Peer from openPeerConnections in the PeerBean
				LOG.warn("Permanent PeerConnection {} to peer {} has been closed.", peerConnection, message.sender());
				peer.peerBean().openPeerConnections().remove(message.sender().peerId());
			}
		});

		// put the now open PeerConnection into the openPeerConnections-Map in the PeerBean
		LOG.debug("Storing peerconnection {} to peer {} permanently: {}", peerConnection, message.sender().peerId());
		final ConcurrentHashMap<Number160, PeerConnection> openPeerConnections = peer.peerBean().openPeerConnections();
		openPeerConnections.put(message.sender().peerId(), peerConnection);
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
