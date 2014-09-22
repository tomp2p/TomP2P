package net.tomp2p.relay;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PeerMapUpdateTask is responsible for periodically sending the unreachable
 * peer's PeerMap to its relays. This is important as the relay peers respond to
 * routing requests on behalf of the unreachable peers
 * 
 */
public class PeerMapUpdateTask extends TimerTask implements PeerMapChangeListener {

	private static final Logger LOG = LoggerFactory.getLogger(PeerMapUpdateTask.class);
	private static final long BOOTSTRAP_TIMEOUT_MS = 10000;

	private final RelayRPC relayRPC;
	private final BootstrapBuilder bootstrapBuilder;
	private final DistributedRelay distributedRelay;
	private final Collection<PeerAddress> manualRelays;
	private final int maxFail;

	// status whether the map has been updated in the meantime
	private final AtomicBoolean updated;

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

		// register to peer map events
		this.updated = new AtomicBoolean(true);
		relayRPC.peer().peerBean().peerMap().addPeerMapChangeListener(this);
	}

	@Override
	public void run() {
		// don't cancel, as we can be relayed again in future, only cancel if this peer shuts down.
		if (relayRPC.peer().isShutdown()) {
			this.cancel();
			return;
		}

		if (bootstap() && updated.get()) {
			// reset the status since it is now sent to alle relay peers
			updated.set(false);
			
			// send the peer map to the relays
			updatePeerMap();
		}

		// try to add more relays
		final FutureRelay futureRelay2 = new FutureRelay();
		distributedRelay.setupRelays(futureRelay2, manualRelays, maxFail);
		relayRPC.peer().notifyAutomaticFutures(futureRelay2);
	}

	@Override
	public boolean cancel() {
		// unregister from peer map events
		relayRPC.peer().peerBean().peerMap().removePeerMapChangeListener(this);
		return super.cancel();
	}

	@Override
	public void peerInserted(PeerAddress peerAddress, boolean verified) {
		updated.set(true);
	}

	@Override
	public void peerRemoved(PeerAddress peerAddress, PeerStatatistic storedPeerAddress) {
		updated.set(true);
	}

	@Override
	public void peerUpdated(PeerAddress peerAddress, PeerStatatistic storedPeerAddress) {
		updated.set(true);
	}

	private boolean bootstap() {
		// bootstrap to get updated peer map and then push it to the relay peers
		return bootstrapBuilder.start().awaitUninterruptibly(BOOTSTRAP_TIMEOUT_MS);
	}

	/**
	 * Bootstrap to get the latest peer map and then update all relays with the newest version
	 */
	private void updatePeerMap() {
		List<Map<Number160, PeerStatatistic>> peerMapVerified = relayRPC.peer().peerBean().peerMap().peerMapVerified();

		// send the map to all relay peers
		for (final BaseRelayConnection relay : distributedRelay.relays()) {
			sendPeerMap(relay, peerMapVerified);
		}
	}

	/**
	 * Send the peer map of an unreachable peer to a relay peer, so that the
	 * relay peer can reply to neighbor requests on behalf of the unreachable
	 * peer.
	 * 
	 * @param connection
	 *            The connection to the relay peer
	 * @param map
	 *            The unreachable peer's peer map.
	 */
	private void sendPeerMap(final BaseRelayConnection connection, List<Map<Number160, PeerStatatistic>> map) {
		LOG.debug("Sending current routing table to relay {}", connection.relayAddress());

		final Message message = relayRPC
				.createMessage(connection.relayAddress(), RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
		// TODO: neighbor size limit is 256, we might have more here
		message.neighborsSet(new NeighborSet(-1, RelayUtils.flatten(map)));

		final FutureResponse fr = connection.sendToRelay(message);
		fr.addListener(new BaseFutureAdapter<BaseFuture>() {
			public void operationComplete(BaseFuture future) throws Exception {
				if (future.isFailed()) {
					LOG.warn("Failed to update routing table on relay peer {}. Reason: {}", connection.relayAddress(),
							future.failedReason());
					connection.onMapUpdateFailed();
				} else {
					LOG.trace("Updated routing table on relay {}", connection.relayAddress());
					connection.onMapUpdateSuccess();
				}
			}
		});
	}
}