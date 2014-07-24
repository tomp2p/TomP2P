package net.tomp2p.rcon;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.relay.RelayForwarderRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

/**
 * 
 * @author jonaswagner
 * 
 */
public class RconRPC extends DispatchHandler {

	private static final int MAX_TIMEOUT_CYCLES = 10000000;
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
	 * REQUEST_1 = relay rcon forwarding REQUEST_2 = open socket and transmit
	 * PeerConnection REQUEST_3 = open socket and connect via PeerConnection
	 */
	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder)
			throws Exception {
		LOG.debug("received RconRPC message {}", message);
		if (message.type() == Message.Type.REQUEST_1 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the relay peer
			LOG.debug("handle RconForward for message: " + message);
			handleRconForward(message, responder);
		} else if (message.type() == Message.Type.REQUEST_2 && message.command() == RPC.Commands.RCON.getNr()) {
			// the message reached the unreachable peer
			LOG.debug("handle RconSetup for message: " + message);
			handleRconSetup(message, responder);
		} else if (message.type() == Message.Type.REQUEST_3 && message.command() == RPC.Commands.RCON.getNr()) {
			// TODO JWA the message reached the requesting peer

			LOG.debug("handle RconAfterconnect for message: " + message);
			handleRconAfterconnect(message, responder, peerConnection);
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	private void handleRconAfterconnect(Message message, Responder responder, PeerConnection peerConnection) {
		ConcurrentHashMap<Integer, Message> cachedMessages = peer.connectionBean().sender().cachedMessages();
		
		if (cachedMessages.containsKey(message.messageId())) {
			
			FutureResponse futureResponse = new FutureResponse(cachedMessages.get(message.messageId()));
			RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(), peer.connectionBean(), config);
			LOG.debug("Original Message sent to unreachablePeer with PeerAddress {}" + message.sender());
			
		} else {
			LOG.error("There was no original message found for RconMessageId=" + message.messageId() + "! This should not happen...");
		}
		
		responder.response(createResponseMessage(message, Type.OK));
	}

	private void handleRconSetup(final Message message, final Responder responder) throws TimeoutException {
		// TODO JWA handle setup
		System.out.println(message.toString());

		PeerAddress originalSender = null;
		PeerConnection peerConnection = null;

		if (message.neighborsSet(0).size() == 0 || message.neighborsSet(0).neighbors().isEmpty()) {
			LOG.error("the original sender was not transmittet in the neighborsSet!");
		} else {
			originalSender = (PeerAddress) message.neighborsSet(0).neighbors().toArray()[0];
			FuturePeerConnection fpc = peer.createPeerConnection(originalSender);

			// TODO JWA discuss this with Thomas Bocek
			LOG.debug("entering loop");
			int timeout = 0;
			while (!fpc.isCompleted() && timeout < MAX_TIMEOUT_CYCLES) {
				// wait for maximal 10000000 cycles
				timeout++;
			}
			LOG.debug("exiting loop");

			if (timeout == MAX_TIMEOUT_CYCLES) {
				throw new TimeoutException();
			}

			if (!fpc.isSuccess()) {
				LOG.error("no channel could be established");
			} else {
				peerConnection = fpc.peerConnection();
			}
		}

		if (peerConnection != null) {
			Message setupMessage = createSetupMessage(message, peerConnection);

			FutureResponse futureResponse = new FutureResponse(setupMessage);
			RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(), peer.connectionBean(), config);

		} else {
			LOG.error("the peerConnection was null!");
		}

		responder.response(createResponseMessage(message, Type.OK));
	}

	/**
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
		return setupMessage;
	}

	/**
	 * This methods is responsible for forwarding the rconSetupMessage to the unreachable Peer.
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleRconForward(Message message, Responder responder) {

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
			RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(), peer.connectionBean(), config);

			// Indicate the reachable peer that the message was forwarded
			responder.response(createResponseMessage(message, Type.OK));

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

		return forwardMessage;
	}

}
