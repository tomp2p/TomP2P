package net.tomp2p.holep;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RSASignatureFactory;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.connection.TimeoutFactory;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2POutbound;
import net.tomp2p.message.TomP2PSinglePacketUDP;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayForwarderRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

public class HolePunchRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(HolePunchRPC.class);

	private final Peer peer;
	private final ConnectionConfiguration config;
	private final ChannelClientConfiguration clientConfig;

	public HolePunchRPC(Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.HOLEP.getNr());
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
		this.clientConfig = PeerBuilder.createDefaultChannelClientConfiguration();
		try {
			this.clientConfig.senderUDP(InetAddress.getByName("192.168.56.1"));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
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
		} else if (message.type() == Message.Type.OK) {
			LOG.debug("HolePunch initiated on peer: " + message.recipient().peerId());
			handleHolePunchReply(message, peerConnection, responder);
		} else {
			throw new IllegalArgumentException("Message Content is wrong!");
		}
	}

	private void handleHolePunchReply(Message message, PeerConnection peerConnection, Responder responder) {
		// TODO Auto-generated method stub
		
	}

	private void handleHolePunch(final Message message, final PeerConnection peerConnection, final Responder responder) {
		final List<Integer> remotePorts = message.intList();
		// this list holds all the port mappings for the hole punch procedure
		final List<Pair<Integer, Integer>> portMappings = new ArrayList<Pair<Integer, Integer>>();
		final PeerAddress originalSender = (PeerAddress) message.neighborsSetList().get(0).neighbors().toArray()[0];

		for (int i = 0; i < remotePorts.size(); i++) {
			final int currentPort = remotePorts.get(i);
			final PeerAddress recipient = originalSender.changeFirewalledUDP(false).changeRelayed(false)
					.changePorts(-1, currentPort);
			
			final Message dummyMessage = this.createMessage(recipient, RPC.Commands.HOLEP.getNr(), Message.Type.REQUEST_4);
			final FutureResponse futureResponse = new FutureResponse(dummyMessage);
			
			final FutureChannelCreator fcc = peer.connectionBean().reservation().create(3, 0);
			fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {

				@SuppressWarnings("static-access")
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						ChannelCreator channelCreator = future.channelCreator();
						
						// we must predefine a socket in order to make sure that the outgoing port is known to us
						final InetAddress inetAddress = peer.peerBean().serverPeerAddress().createSocketUDP().getAddress();
						int outgoingPort = channelCreator.randomPort();
						InetSocketAddress socket = new InetSocketAddress(inetAddress, outgoingPort);
						
						// send Dummy twice in order to get the [ASSURED] Tag on the conntrack module
						peer.connectionBean().sender().punchHole(futureResponse, dummyMessage, channelCreator, socket);
						peer.connectionBean().sender().punchHole(futureResponse, dummyMessage, channelCreator, socket);
						
						portMappings.add(new Pair<Integer, Integer>(currentPort, socket.getPort()));
					} else {
						handleFail(message, responder, "could not create channel!");
					}
				}
			});
			
			while (!fcc.isCompleted()) {
				//wait
			}
		}
		Message replyMessage = createMessage(originalSender, Commands.HOLEP.getNr(), Message.Type.OK);
		replyMessage.messageId(message.messageId());
		for (Pair<Integer, Integer> pair : portMappings) {
			replyMessage.intValue(pair.element0());
			replyMessage.intValue(pair.element1());
		}
		responder.response(replyMessage);
	}

	private void forwardPorts(Message message, PeerConnection peerConnection, final Responder responder) {
		final BaseRelayForwarderRPC forwarder = extractRelayForwarder(message);
		if (forwarder != null) {
			final Message forwardMessage = createForwardPortsMessage(message, forwarder.unreachablePeerAddress());
			
			FutureDone<Message> response = forwarder.forwardToUnreachable(forwardMessage);
			response.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
				@Override
				public void operationComplete(FutureDone<Message> future) throws Exception {
					if (future.isSuccess()) {
						Message answerMessage = future.object();
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