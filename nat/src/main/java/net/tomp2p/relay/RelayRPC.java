package net.tomp2p.relay;

import java.util.List;
import java.util.Map;

import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.core.SctpPorts;
import net.tomp2p.connection.ChannelClient;
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
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.ConcurrentCacheMap;
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
	
	final private ConcurrentCacheMap<Number160, ChannelSender> activeRelays = new ConcurrentCacheMap<>(60, 10000);
	
	public RelayRPC(Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		this.peer = peer;
		// register this handler
		register(RPC.Commands.RELAY.getNr());
	}
	
	public Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> sendSetupMessage(
			final PeerAddress candidate, final ChannelClient channelCreator) {
		
		final Message message = createMessage(candidate, RPC.Commands.RELAY.getNr(), Type.REQUEST_1);
		message.keepAlive(true);
		return channelCreator.sendUDP(message);
		
	}
	
	public FutureDone<Message> sendPeerMap(final PeerAddress relayPeer, final List<Map<Number160, PeerStatistic>> map, 
			final ClientChannel channel) {
		
		final Message message = createMessage(relayPeer, RPC.Commands.RELAY.getNr(), Type.REQUEST_2);

		NeighborSet ns = new NeighborSet(5, RelayUtils.flatten(map));
		message.neighborsSet(ns);
		LOG.debug("send neighbors " + ns);
		// append relay-type specific data (if necessary)
		//relayConfig.prepareMapUpdateMessage(message);
		message.keepAlive(true);
		FutureDone<Message> f = new FutureDone<>();
		channel.send(message, f);
		return f;
	}
	
	public Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> sendRendezvousMessage(
			final PeerAddress remote, final ChannelClient channelCreator, final int port) {
		
		final Message message = createMessage(remote, RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
		message.keepAlive(true);
		message.intAt(port);
		return channelCreator.sendUDP(message);
		
		//fire up holes!
		
	}
	
	@Override
	public void handleResponse(Responder r, Message message, boolean sign, Promise<SctpChannelFacade, Exception, Void> p, ChannelSender sender) throws Exception {
		if (message.type() == Type.REQUEST_1 && message.command() == RPC.Commands.RELAY.getNr()) {
			//no capacity restrictions yet
			activeRelays.putIfAbsent(message.recipient().peerId(), sender);
			r.response(handleSetup(message));
		} else if (message.type() == Type.REQUEST_2 && message.command() == RPC.Commands.RELAY.getNr()) {
			//no capacity restrictions yet
			activeRelays.putIfAbsent(message.recipient().peerId(), sender);
		} else if (message.type() == Type.REQUEST_3 && message.command() == RPC.Commands.RELAY.getNr()) {
			int portRequester = message.intAt(0);
			ChannelSender storedSender = activeRelays.get(message.sender().peerId());
			if(storedSender != null) {
				message.restoreBuffers();
				message.restoreContentReferences();
				storedSender.send(message).first.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
					@Override
					public void operationComplete(FutureDone<Message> future) throws Exception {
						r.response(future.object());
					}
				});
			} else {
				//this is executed by the unreachable peer
				int portRequester2 = message.intAt(0);
				int port = SctpPorts.getInstance().generateDynPort();
				
				//fire up holes!
				
				Message response = createResponseMessage(message, Type.OK);
				response.intValue(port);
				r.response(response);
			}
			
			
		} else {
			//forward
			ChannelSender storedSender = activeRelays.get(message.sender().peerId());
			if(storedSender != null) {
				message.restoreBuffers();
				message.restoreContentReferences();
				storedSender.send(message).first.addListener(new BaseFutureAdapter<FutureDone<Message>>() {
					@Override
					public void operationComplete(FutureDone<Message> future) throws Exception {
						r.response(future.object());
					}
				});
			} else {
				LOG.debug("no dispatcher");
			}
		}
	}
	
	private Message handleSetup(Message message) {
		final Number160 unreachablePeerId = message.sender().peerId();
		//TODO: add myself as relay
		//peerBean().notifyPeerFound(unreachablePeerConnectionCopy.remotePeer(), null, unreachablePeerConnectionCopy, null);
		
		for (Commands command : RPC.Commands.values()) {
			//if (command != RPC.Commands.RELAY) {
				// Register this class to handle all relay messages (currently used when a slow message
				// arrives)
				LOG.debug("register this for {}, {}",command.toString(), message);
				dispatcher().registerIoHandler(peer.peerID(), unreachablePeerId, this, command.getNr());
			//}
		}
		return createResponseMessage(message, Type.OK);
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
