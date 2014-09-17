package net.tomp2p.nat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.DistributedRelay;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PeerMapUpdateTask is responsible for periodically sending the unreachable
 * peer's PeerMap to its relays. This is important as the relay peers respond to
 * routing requests on behalf of the unreachable peers
 * 
 */
class PeerMapUpdateTask extends TimerTask {

	private static final Logger LOG = LoggerFactory.getLogger(PeerMapUpdateTask.class);

	final private RelayRPC relayRPC;
	final private BootstrapBuilder bootstrapBuilder;
	final private DistributedRelay distributedRelay;
	final private Collection<PeerAddress> manualRelays;
	final private int maxFail;

	/**
	 * Create a new peer map update task.
	 * 
	 * @param relayRPC
	 *            the RelayRPC of this peer
	 * @param bootstrapBuilder
	 *            bootstrap builder used to find neighbors of this peer
	 * @param distributedRelay
	 *            set of the relay addresses
	 */
	public PeerMapUpdateTask(RelayRPC relayRPC, BootstrapBuilder bootstrapBuilder, DistributedRelay distributedRelay,
			Collection<PeerAddress> manualRelays, int maxFail) {
		this.relayRPC = relayRPC;
		this.bootstrapBuilder = bootstrapBuilder;
		this.distributedRelay = distributedRelay;
		this.manualRelays = manualRelays;
		this.maxFail = maxFail;
	}

	@Override
	public void run() {
		// don't cancel, as we can be relayed again in future, only cancel if this peer shuts down.
		if (relayRPC.peer().isShutdown()) {
			this.cancel();
			return;
		}

		// bootstrap to get updated peer map and then push it to the relay peers
		FutureBootstrap fb = bootstrapBuilder.start();
		fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					List<Map<Number160, PeerStatatistic>> peerMapVerified = relayRPC.peer().peerBean().peerMap()
							.peerMapVerified();
					
					// make a copy for faster release of the lock
					final Collection<BaseRelayConnection> relays;
					synchronized (distributedRelay.relays()) {
						relays = new ArrayList<BaseRelayConnection>(distributedRelay.relays());
					}
					
					// send the map to all relay peers
					for (final BaseRelayConnection relay : relays) {
						sendPeerMap(relay, peerMapVerified);
					}
				}
			}
		});
		final FutureRelay futureRelay2 = new FutureRelay();
		distributedRelay.setupRelays(futureRelay2, manualRelays, maxFail);
		relayRPC.peer().notifyAutomaticFutures(futureRelay2);
	}

	/**
	 * Send the peer map of an unreachable peer to a relay peer, so that the
	 * relay peer can reply to neighbor requests on behalf of the unreachable
	 * peer.
	 * 
	 * @param peerAddress
	 *            The peer address of the relay peer
	 * @param map
	 *            The unreachable peer's peer map.
	 * @return
	 */
	public void sendPeerMap(final BaseRelayConnection connection, List<Map<Number160, PeerStatatistic>> map) {
		final Message message = relayRPC.createMessage(connection.relayAddress(), RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
		// TODO: neighbor size limit is 256, we might have more here
		message.neighborsSet(new NeighborSet(-1, RelayUtils.flatten(map)));
		
		final FutureResponse fr = connection.sendToRelay(message);
		fr.addListener(new BaseFutureAdapter<BaseFuture>() {
			public void operationComplete(BaseFuture future) throws Exception {
				if (future.isFailed()) {
					LOG.warn("failed to update peer map on relay peer {}: {}", connection.relayAddress(), future.failedReason());
				} else {
					LOG.trace("Updated peer map on relay {}", connection.relayAddress());
				}
			}
		});
	}
}