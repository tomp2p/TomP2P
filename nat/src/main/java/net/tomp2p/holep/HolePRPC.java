package net.tomp2p.holep;

import java.util.ArrayList;

import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.HolePInitiator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.strategy.AbstractHolePStrategy;
import net.tomp2p.holep.strategy.HolePStrategy;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayServer;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jonas Wagner
 * 
 *         This class is responsible for the transmission of the holep setup and
 *         reply message for the hole punch procedure. The class will also start
 *         the hole punch procedure on the target peer side.
 * 
 */
public class HolePRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(HolePRPC.class);
	private final Peer peer;

	public HolePRPC(final Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.HOLEP.getNr());
		this.peer = peer;
	}

	@Override
	public void handleResponse(final Message message, final PeerConnection peerConnection, boolean sign, final Responder responder) throws Exception {
		// This means, that a new Holepunch has been initiated.
		if (message.type() == Message.Type.REQUEST_1) {
			LOG.debug("New HolePunch process initiated from peer " + message.sender().peerId() + " to peer " + message.recipient().peerId()
					+ " on ports: " + message.intList().toString());
			forwardHolePunchMessage(message, responder);
		}
		// This means that the initiating peer has sent a holep setup message to
		// this peer
		else if (message.type() == Message.Type.REQUEST_2) {
			LOG.debug("HolePunch initiated on peer: " + message.recipient().peerId());
			handleHolePunch(message, responder);
		} else {
			throw new IllegalArgumentException("Message Content is wrong!");
		}
	}

	/**
	 * This method is called by handleResponse(...) and initiates the hole
	 * punching procedure on the nat peer that needs to be contacted. It creates
	 * an {@link AbstractHolePStrategy} and waits then for the reply
	 * {@link Message} which the peer that needs to be contacted sends back to
	 * the initiating peer. The reply{@link Message} contains information about
	 * the holes which are punched currently.
	 * 
	 * @param message
	 * @param responder
	 */
	private void handleHolePunch(final Message message, final Responder responder) {
		final NATType type = ((HolePInitiatorImpl) peer.peerBean().holePunchInitiator()).natType();
		//TODO jwa does this work?
		final HolePStrategy holePuncher = type.holePuncher(peer, message.intAt(0), peer.connectionBean().DEFAULT_UDP_IDLE_SECONDS, message);
		final FutureDone<Message> replyMessage = holePuncher.replyHolePunch();
		replyMessage.addListener(new BaseFutureAdapter<FutureDone<Message>>() {

			@Override
			public void operationComplete(FutureDone<Message> future) throws Exception {
				if (future.isSuccess()) {
					responder.response(future.object());
				} else {
					handleFail(message, responder, "Fail while initiating the hole punching");
				}
			}
		});
	}

	/**
	 * This method first forwards a initHolePunch request to start the hole
	 * punching procedure on the target peer. Then it waits for the response
	 * from the target peer and forwards this response back to the initiating
	 * peer.
	 * 
	 * @param message
	 * @param responder
	 */
	private void forwardHolePunchMessage(final Message message, final Responder responder) {
		final BaseRelayServer forwarder = extractRelayForwarder(message);
		if (forwarder != null) {
			final Message forwardMessage = createForwardPortsMessage(message, forwarder.unreachablePeerAddress());
			final FutureDone<Message> response = forwarder.forwardToUnreachable(forwardMessage);
			response.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
				@Override
				public void operationComplete(FutureDone<Message> future) throws Exception {
					if (future.isSuccess()) {
						final Message answerMessage = createAnswerMessage(message, future.object());
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

	/**
	 * This method creates a the setUpMessage which needs to be forwarded to the
	 * unreachable peer.
	 * 
	 * @param message
	 * @param recipient
	 * @return forwardMessage
	 */
	private Message createForwardPortsMessage(final Message message, final PeerAddress recipient) {
		final Message forwardMessage = createMessage(recipient, RPC.Commands.HOLEP.getNr(), Message.Type.REQUEST_2);
		forwardMessage.version(message.version());
		forwardMessage.messageId(message.messageId());

		// forward all ports to the unreachable peer2
		forwardMessage.intValue(message.intAt(0));
		duplicateBuffer(message, forwardMessage);

		// transmit PeerAddress of unreachable Peer1
		final NeighborSet ns = new NeighborSet(1, new ArrayList<PeerAddress>(1));
		ns.add(message.sender());
		forwardMessage.neighborsSet(ns);

		return forwardMessage;
	}

	/**
	 * This method creates the message which is sent back from the relay to the
	 * initiating peer.
	 * 
	 * @param replyMessage
	 * @param future
	 * @return
	 */
	private Message createAnswerMessage(final Message originalMessage, final Message replyMessage) {
		final Message answerMessage = createResponseMessage(originalMessage, Message.Type.OK);
		answerMessage.command(Commands.HOLEP.getNr());

		// forward port information of unreachable peer2
		answerMessage.intValue(replyMessage.intAt(0));
		duplicateBuffer(replyMessage, answerMessage);

		return answerMessage;
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
	private BaseRelayServer extractRelayForwarder(final Message message) {
		final Dispatcher dispatcher = peer.connectionBean().dispatcher();
		return (BaseRelayServer) dispatcher.searchHandler(BaseRelayServer.class, peer.peerID(), message.recipient().peerId());
	}

	/**
	 * This method simply duplicates the Buffer of a message.
	 * 
	 * @param originalMessage
	 * @param copyMessage
	 */
	private void duplicateBuffer(final Message originalMessage, final Message copyMessage) {
		for (Buffer buf : originalMessage.bufferList()) {
			copyMessage.buffer(new Buffer(buf.buffer().duplicate()));
		}
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