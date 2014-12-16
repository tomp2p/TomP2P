package net.tomp2p.relay;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.relay.android.AndroidRelayConnection;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PeerMapUpdateTask is responsible for periodically sending the unreachable
 * peer's PeerMap to its relays. This is important as the relay peers respond to
 * routing requests on behalf of the unreachable peers
 * 
 */
public class PeerMapUpdateTask extends TimerTask {

	private static final Logger LOG = LoggerFactory.getLogger(PeerMapUpdateTask.class);
	private static final long BOOTSTRAP_TIMEOUT_MS = 10000;

	private final RelayRPC relayRPC;
	private final BootstrapBuilder bootstrapBuilder;
	private final DistributedRelay distributedRelay;

	private Set<PeerAddress> gcmServersLast;

	/**
	 * Create a new peer map update task.
	 * 
	 * @param relayRPC
	 *            the RelayRPC of this peer
	 * @param bootstrapBuilder
	 *            bootstrap builder used to find neighbors of this peer
	 * @param distributedRelay
	 *            set of the relay addresses
	 * @param relayType
	 */
	public PeerMapUpdateTask(RelayRPC relayRPC, BootstrapBuilder bootstrapBuilder, DistributedRelay distributedRelay) {
		this.relayRPC = relayRPC;
		this.bootstrapBuilder = bootstrapBuilder;
		this.distributedRelay = distributedRelay;
		this.gcmServersLast = new HashSet<PeerAddress>(distributedRelay.relayConfig().gcmServers());
	}

	@Override
	public void run() {
		// don't cancel, as we can be relayed again in future, only cancel if this peer shuts down.
		if (relayRPC.peer().isShutdown()) {
			this.cancel();
			return;
		}

		// bootstrap to get updated peer map and then push it to the relay peers
		bootstrapBuilder.start().awaitUninterruptibly(BOOTSTRAP_TIMEOUT_MS);

		// send the peer map to the relays
		List<Map<Number160, PeerStatistic>> peerMapVerified = relayRPC.peer().peerBean().peerMap().peerMapVerified();
		boolean gcmServersChanged = distributedRelay.relayConfig().type() == RelayType.ANDROID && gcmServersChanged();
		for (final BaseRelayConnection relay : distributedRelay.relays()) {
			sendPeerMap(relay, peerMapVerified, gcmServersChanged);
		}
		// copy to compare with next iteration
		gcmServersLast = new HashSet<PeerAddress>(distributedRelay.relayConfig().gcmServers());

		// try to add more relays
		final FutureRelay futureRelay2 = new FutureRelay();
		distributedRelay.setupRelays(futureRelay2);
		relayRPC.peer().notifyAutomaticFutures(futureRelay2);
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
	private void sendPeerMap(final BaseRelayConnection connection, List<Map<Number160, PeerStatistic>> map,
			boolean gcmServersChanged) {
		LOG.debug("Sending current routing table to relay {}", connection.relayAddress());

		final Message message = relayRPC
				.createMessage(connection.relayAddress(), RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
		// TODO: neighbor size limit is 256, we might have more here
		message.neighborsSet(new NeighborSet(-1, RelayUtils.flatten(map)));

		if (gcmServersChanged) {
			LOG.debug("Sending updated GCM server list as well");
			message.neighborsSet(new NeighborSet(-1, distributedRelay.relayConfig().gcmServers()));
		}

		final FutureResponse fr = connection.sendToRelay(message);
		fr.addListener(new BaseFutureAdapter<FutureResponse>() {
			public void operationComplete(FutureResponse future) throws Exception {
				if (future.isFailed()) {
					LOG.warn("Failed to update routing table on relay peer {}. Reason: {}", connection.relayAddress(),
							future.failedReason());
					connection.onMapUpdateFailed();
				} else {
					LOG.trace("Updated routing table on relay {}", connection.relayAddress());
					connection.onMapUpdateSuccess();
					
					// process possible buffered messages (Android only)
					if(connection instanceof AndroidRelayConnection) {
						AndroidRelayConnection androidConnection = (AndroidRelayConnection) connection;
						androidConnection.onReceiveMessageBuffer(future.responseMessage(), new FutureDone<Void>());
					}
				}
			}
		});
	}

	/**
	 * Checks the GCM Server map in the last run with the current server map. If they differ,
	 * <code>true</code> is returned.
	 */
	private boolean gcmServersChanged() {
		Collection<PeerAddress> newServers = distributedRelay.relayConfig().gcmServers();
		if (newServers == null && gcmServersLast == null) {
			return false;
		} else if (newServers != null && gcmServersLast == null || newServers == null && gcmServersLast != null) {
			return true;
		} else {
			// compare content
			return !new HashSet<PeerAddress>(newServers).equals(gcmServersLast);
		}
	}
}
