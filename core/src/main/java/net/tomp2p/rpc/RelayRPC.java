package net.tomp2p.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.List;
import java.util.Map;

import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.ClientChannel;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.Responder;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.peers.PeerAddress.PeerAddressBuilder;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Triple;

public class RelayRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(RelayRPC.class);

	private final Peer peer;


	/**
	 * Register the RelayRPC. After the setup, the peer is ready to act as a
	 * relay if asked by an unreachable peer.
	 * 
	 * @param peer
	 *            The peer to register the RelayRPC
	 * @param rconRPC the reverse connection RPC
	 * @return
	 */
	
	private List<Map<Number160,Map<Number160, PeerStatistic>>> peerMaps;
	
	final private ConcurrentCacheMap<Number160, Pair<InetSocketAddress, ChannelSender>> activeRelays = new ConcurrentCacheMap<>(60, 10000);
	
	public RelayRPC(Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		this.peer = peer;
		// register this handler
		register(RPC.Commands.RELAY.getNr());
	}
	
	public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> sendSetupMessage(
			final PeerAddress candidate) {
		
		final Message message = createMessage(candidate, RPC.Commands.RELAY.getNr(), Type.REQUEST_1);
		message.keepAlive(true);
		return connectionBean().channelServer().sendUDP(message);
		
	}
	
	public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> sendPeerMap(
			final PeerAddress relayPeer, 
			final List<Map<Number160, 
			PeerStatistic>> map, 
			final ClientChannel channel) {
		
		final Message message = createMessage(relayPeer, RPC.Commands.RELAY.getNr(), Type.REQUEST_2);

		NeighborSet ns = new NeighborSet(5, RelayUtils.flatten(map));
		message.neighborsSet(ns);
		LOG.debug("send neighbors " + ns);
		return connectionBean().channelServer().sendUDP(message);
	}
	
	public Pair<FutureDone<Message>, FutureDone<SctpChannelFacade>> sendReverseConnectionMessage(
			final PeerAddress rendezVous, final PeerAddress firewalledPeer) {
		
		final Message messageRelay = createMessage(rendezVous, RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
		if(peerBean().serverPeerAddress().ipv4Socket() != null) {
			messageRelay.peerSocketAddress(peerBean().serverPeerAddress().ipv4Socket());
		} else if(peerBean().serverPeerAddress().ipv6Socket() != null) {
			messageRelay.peerSocketAddress(peerBean().serverPeerAddress().ipv6Socket());
		} else {
			return null; //todo return failure
		}
		//if I'm behind port preserving NAT, fire message to open connection
		final Message holePunchingMessage = createMessage(firewalledPeer, RPC.Commands.RELAY.getNr(), Type.REQUEST_5);
		try {
			connectionBean().channelServer().fireUDP(holePunchingMessage, connectionBean().dispatcher().peerBean().serverPeerAddress());
		} catch (InvalidKeyException | SignatureException | IOException e) {
			e.printStackTrace();
		}
		
		messageRelay.sctp(true);
		return connectionBean().channelServer().sendUDP(messageRelay);
		//fire up holes!	
	}
	
	@Override
	public void handleResponse(Responder r, Message message, boolean sign, Promise<SctpChannelFacade, Exception, Void> p, ChannelSender sender) throws Exception {
		LOG.debug("handle relay RPC");
		if (message.type() == Type.REQUEST_1 && message.command() == RPC.Commands.RELAY.getNr()) {
			//no capacity restrictions yet
			activeRelays.putIfAbsent(message.sender().peerId(), Pair.create(message.senderSocket(), sender));
			r.response(handleSetup(message));
		} else if (message.type() == Type.REQUEST_2 && message.command() == RPC.Commands.RELAY.getNr()) {
			//no capacity restrictions yet
			activeRelays.putIfAbsent(message.sender().peerId(), Pair.create(message.senderSocket(), sender));
		} else if (message.type() == Type.REQUEST_3 && message.command() == RPC.Commands.RELAY.getNr() 
				&& dispatcher().isPrimaryTarget(message.recipient().peerId())) {
			//we now got the holepunching request
			
			PeerAddressBuilder builder = PeerAddress.builder();
			if(!message.peerSocket4AddressList().isEmpty()) {
				builder.ipv4Socket(message.peerSocket4Address(0));
			} else if(!message.peerSocket6AddressList().isEmpty()) {
				builder.ipv6Socket(message.peerSocket6Address(0));
			} else {
				//TOOD: fail
			}
			builder.peerId(message.recipient().peerId());
			
			final Message holePunchingMessage = createMessage(builder.build(), RPC.Commands.RELAY.getNr(), Type.REQUEST_4);
			r.response(createResponseMessage(message, Type.OK));
			
			connectionBean().channelServer().fireUDP(holePunchingMessage, connectionBean().dispatcher().peerBean().serverPeerAddress());
			
		} else if (message.type() == Type.REQUEST_4 && message.command() == RPC.Commands.RELAY.getNr()) {
			//this will never arrive... if yes, ignore it
			LOG.debug("got hole punching message, seems that hole was already punched");
			final Message holePunchingMessage = createMessage(message.sender(), RPC.Commands.RELAY.getNr(), Type.REQUEST_5);
			try {
				connectionBean().channelServer().fireUDP(holePunchingMessage, connectionBean().dispatcher().peerBean().serverPeerAddress());
			} catch (InvalidKeyException | SignatureException | IOException e) {
				e.printStackTrace();
			}
		} else if (message.type() == Type.REQUEST_5 && message.command() == RPC.Commands.RELAY.getNr()) {
			//this will never arrive... if yes, ignore it
			LOG.debug("holepunching established");
			//SCTP already handled!!
		}
		
		else {
			//forward
			Pair<InetSocketAddress, ChannelSender> pr = activeRelays.get(message.recipient().peerId());
			if(pr != null) {
				message.restoreBuffers();
				message.restoreContentReferences();
				message.recipientSocket(pr.element0());
				pr.element1().send(message).element0().addListener(new BaseFutureAdapter<FutureDone<Message>>() {
					@Override
					public void operationComplete(FutureDone<Message> future) throws Exception {
						System.err.println("GOT IT");
						r.response(future.object());
					}
				});
			} else {
				LOG.debug("no acive relays found for {}, only for {}", message.sender().peerId(), activeRelays.keySet());
			}
		}
	}
	
	private Message handleSetup(Message message) {
		final Number160 unreachablePeerId = message.sender().peerId();
		//TODO: add myself as relay
		//peerBean().notifyPeerFound(unreachablePeerConnectionCopy.remotePeer(), null, unreachablePeerConnectionCopy, null);
		
		for (RPC.Commands command : RPC.Commands.values()) {
			//if (command != RPC.Commands.RELAY) {
				// Register this class to handle all relay messages (currently used when a slow message
				// arrives)
				LOG.debug("register {} for peer {} on behalf of {}", command.toString(), peer.peerID(), unreachablePeerId);
				dispatcher().registerIoHandler(peer.peerID(), unreachablePeerId, this, command.getNr());
			//}
		}
		return createResponseMessage(message, Type.OK).keepAlive(true);
	}
	
	private void handleMap(Message message, Responder responder) {
		LOG.debug("Handle foreign map update {}", message);
		
		/*final Forwarder forwarder = dispatcher().searchHandler(Forwarder.class, peer.peerAddress().peerId(), message.sender().peerId());		
		
		if (forwarder != null) {
			Collection<PeerAddress> map = message.neighborsSet(0).neighbors();
			Message response = createResponseMessage(message, Type.OK);
			List<Message> buffered = forwarder.buffered();
			if(buffered != null) {
				ByteBuf bb = RelayUtils.composeMessageBuffer(buffered, peer.connectionBean().resourceConfiguration().signatureFactory());
				response.buffer(new Buffer(bb));
			}
			forwarder.setPeerMap(RelayUtils.unflatten(map, message.sender()), message, response);
			responder.response(response);
		} else {
			LOG.error("No forwarder for peer {} found. Need to setup relay first");
			responder.response(createResponseMessage(message, Type.NOT_FOUND));
		}*/
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
	/*public Set<PeerAddress> unreachablePeers() {
		Set<PeerAddress> unreachablePeers = new HashSet<PeerAddress>(servers.size());
		for (BaseRelayServer forwarder : servers.values()) {
			unreachablePeers.add(forwarder.unreachablePeerAddress());
		}
		return unreachablePeers;
	}*/

	/**
	 * Add a client to the list
	 */
	/*public void addClient(BaseRelayClient connection) {
		clients.put(connection.relayAddress().peerId(), connection);
	}*/

	/**
	 * Remove a client from the list
	 */
	/*public void removeClient(BaseRelayClient connection) {
		clients.remove(connection.relayAddress().peerId());
	}*/

	/**
	 * Handle the setup where an unreachable peer connects to this one
	 */
	
}
