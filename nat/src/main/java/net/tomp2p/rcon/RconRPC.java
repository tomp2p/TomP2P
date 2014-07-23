package net.tomp2p.rcon;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayForwarderRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.utils.Utils;

/**
 * 
 * @author jonaswagner
 *
 */
public class RconRPC extends DispatchHandler {

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
	 * REQUEST_1 = relay rcon forwarding
	 * REQUEST_2 = open socket and transmit PeerConnection
	 * REQUEST_3 = open socket and connect via PeerConnection
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
			responder.response(createResponseMessage(message, Type.OK));
			System.out.println("SUCCESS HIT");
			System.out.println(message);
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	private void handleRconSetup(final Message message, final Responder responder) {
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
			while (!fpc.isCompleted() || timeout < 10000000) {
				//wait
				timeout++;
			}
			LOG.debug("exiting loop");
			
			if (fpc.isFailed()) {
				LOG.error("no channel could be established");
			} else {
				peerConnection = fpc.peerConnection();
				peer.sendDirect(peerConnection).object("yomama is so fat...!");
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

	private void handleRconForward(Message message, Responder responder) {

		RelayForwarderRPC relayForwarderRPC = null;
		PeerConnection peerConnection = null; // the peerConnection to the
												// unreachable peer

		// get the relayForwarderRPC via Dispatcher to retrieve the existing
		// peerConnection
		Dispatcher dispatcher = peer.connectionBean().dispatcher();
		Map<Integer, DispatchHandler> ioHandlers = dispatcher.searchHandlerMap(message.recipient().peerId());
		for (Map.Entry<Integer, DispatchHandler> element : ioHandlers.entrySet()) {
			if (element.getValue().getClass().equals(RelayForwarderRPC.class)) {
				relayForwarderRPC = (RelayForwarderRPC) element.getValue();
				break;
			}
		}

		if (relayForwarderRPC != null) {
			peerConnection = relayForwarderRPC.peerConnection();
		} else {
			LOG.error("no relayForwarder Registered for peerId=" + message.recipient().peerId().toString());
		}

		Message forwardMessage = createForwardMessage(message, peerConnection);

		// we don't want to use another sendDirect anymore since we don't have to send data
		FutureResponse futureResponse = new FutureResponse(forwardMessage);
		RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(), peer.connectionBean(), config);

		responder.response(createResponseMessage(message, Type.OK));
	}

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
		
		// we need to keep the peerConnection between the relay and the unreachable peer open
		forwardMessage.keepAlive(true);

		return forwardMessage;
	}

}
