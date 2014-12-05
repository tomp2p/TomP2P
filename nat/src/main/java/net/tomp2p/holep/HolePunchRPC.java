package net.tomp2p.holep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayForwarderRPC;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Pair;

public class HolePunchRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(HolePunchRPC.class);

	private final Peer peer;
	private final ConnectionConfiguration config;

	public HolePunchRPC(Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.HOLEP.getNr());
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception {
		// This means, that a new Holepunch has been initiated.
		if (message.type() == Message.Type.REQUEST_1) {
			LOG.debug("New HolePunch process initiated from peer " + message.sender().peerId() + " to peer " + message.recipient().peerId()
					+ " on ports: " + message.intList().toString());
			forwardPorts(message, peerConnection, responder);
		}
		// This means that peer1 has answered
		else if (message.type() == Message.Type.REQUEST_2) {
			LOG.debug("HolePunch initiated on peer: " + message.recipient().peerId());
			handleHolePunch(message, peerConnection, responder);
		} else if (message.type() == Message.Type.REQUEST_3) {
			LOG.debug("HolePunch initiated on peer: " + message.recipient().peerId());
			handleHolePunchReply(message, peerConnection, responder);
		} else {
			throw new IllegalArgumentException("Message Content is wrong!");
		}
	}

	private void handleHolePunchReply(Message message, PeerConnection peerConnection, Responder responder) {
		responder.response(createResponseMessage(message, Type.OK));
	}

	private void handleHolePunch(final Message message, final PeerConnection peerConnection, final Responder responder) {
		final List<Integer> remotePorts = message.intList();
		// this list holds all the port mappings for the hole punch procedure
		final List<Pair<Integer, Integer>> portMappings = new ArrayList<Pair<Integer, Integer>>();
		final PeerAddress originalSender = (PeerAddress) message.neighborsSetList().get(0).neighbors().toArray()[0];
		final AtomicInteger countDown = new AtomicInteger(remotePorts.size());

		for (int i = 0; i < remotePorts.size(); i++) {
			final int currentPort = remotePorts.get(i);
			final PeerAddress recipient = originalSender.changeFirewalledUDP(false).changeRelayed(false)
					.changePorts(currentPort, currentPort);
			final HolePunchRPC thisInstance = this;

			final FutureChannelCreator fcc = peer.connectionBean().reservation().create(3, 0);
			fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {

				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						Message dummyMessage = thisInstance.createMessage(recipient, RPC.Commands.HOLEP.getNr(), Message.Type.REQUEST_3);
						HolePuncher holePuncher = new HolePuncher(dummyMessage, future.channelCreator(), peer, future.channelCreator()
								.randomPort(), currentPort, recipient);
						holePuncher.createAndSendUDP();

						portMappings.add(new Pair<Integer, Integer>(holePuncher.remotePort(), holePuncher.localPort()));
					} else {
						handleFail(message, responder, "could not create channel!");
					}
					countDown.decrementAndGet();
					if (countDown.get() == 0) {
						createResponseAndReply(message, responder, portMappings, originalSender);
					}
				}

				/**
				 * This method simply mapps all the portmappings into the
				 * intList of the response {@link Message} and then sends it
				 * back to the sender.
				 * 
				 * @param message
				 * @param responder
				 * @param portMappings
				 * @param originalSender
				 */
				public void createResponseAndReply(final Message message, final Responder responder,
						final List<Pair<Integer, Integer>> portMappings, final PeerAddress originalSender) {
					Message replyMessage = createMessage(originalSender, Commands.HOLEP.getNr(), Message.Type.OK);
					replyMessage.messageId(message.messageId());
					for (Pair<Integer, Integer> pair : portMappings) {
						if (!(pair == null || pair.isEmpty() || pair.element0() == null || pair.element1() == null)) {
							replyMessage.intValue(pair.element0());
							replyMessage.intValue(pair.element1());
						}
					}
					responder.response(replyMessage);
				}
			});
		}
	}

	/**
	 * This method first forwards a initHolePunch request to start the hole
	 * punching procedure on the target peer. Then it waits for the response
	 * from the target peer and forwards this response back to the initiating
	 * peer.
	 * 
	 * @param message
	 * @param peerConnection
	 * @param responder
	 */
	private void forwardPorts(final Message message, PeerConnection peerConnection, final Responder responder) {
		final BaseRelayForwarderRPC forwarder = extractRelayForwarder(message);
		if (forwarder != null) {
			final Message forwardMessage = createForwardPortsMessage(message, forwarder.unreachablePeerAddress());

			FutureDone<Message> response = forwarder.forwardToUnreachable(forwardMessage);
			response.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
				@Override
				public void operationComplete(FutureDone<Message> future) throws Exception {
					if (future.isSuccess()) {
						Message answerMessage = createResponseMessage(message, Message.Type.OK);
						for (Integer i : future.object().intList()) {
							answerMessage.intValue(i);
						}
						answerMessage.command(Commands.HOLEP.getNr());

						LOG.debug("Returing from relay to requester: {}", answerMessage);
						responder.response(answerMessage);
					} else {
						responder.failed(Type.DENIED, "Relaying message failed: " + future.failedReason());
					}
				}
			});
		} else {
			handleFail(message, responder, "No RelayForwarder registered for peerId=" + message.recipient().peerId().toString());
		}
	}

	private Message createForwardPortsMessage(Message message, PeerAddress recipient) {
		Message forwardMessage = createMessage(recipient, RPC.Commands.HOLEP.getNr(), Message.Type.REQUEST_2);
		forwardMessage.version(message.version());
		forwardMessage.messageId(message.messageId());

		// forward all ports to the unreachable peer2
		for (Integer port : message.intList()) {
			forwardMessage.intValue(port);
		}

		// transmit PeerAddress of unreachable Peer1
		final NeighborSet ns = new NeighborSet(1, new ArrayList<PeerAddress>(1));
		ns.add(message.sender());
		forwardMessage.neighborsSet(ns);

		return forwardMessage;
	}

	/**
	 * This method extracts a registered {@link BaseRelayForwarderRPC} from the
	 * {@link Dispatcher}. This RelayForwarder can then be used to extract the
	 * {@link PeerConnection} to the unreachable Peer we want to contact.
	 * 
	 * @param unreachablePeerId
	 *            the unreachable peer
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
	 * This method is called if something went wrong while the hole punching
	 * procedure. It responds then with a {@link Type}.EXCEPTION message.
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