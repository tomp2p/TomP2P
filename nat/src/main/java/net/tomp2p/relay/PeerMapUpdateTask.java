package net.tomp2p.relay;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatistic;

/**
 * The PeerMapUpdateTask is responsible for periodically sending the unreachable
 * peer's PeerMap to its relays. This is important as the relay peers respond to
 * routing requests on behalf of the unreachable peers
 * 
 */
public class PeerMapUpdateTask extends TimerTask {

	private static final Logger LOG = LoggerFactory.getLogger(PeerMapUpdateTask.class);

	private final RelayRPC relayRPC;
	private final BootstrapBuilder bootstrapBuilder;
	private final DistributedRelay distributedRelay;

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
	}

	@Override
	public void run() {
		// don't cancel, as we can be relayed again in future, only cancel if this peer shuts down.
		if (relayRPC.peer().isShutdown()) {
			this.cancel();
			return;
		}

		// bootstrap to get updated peer map and then push it to the relay peers
		bootstrapBuilder.start().addListener(new BaseFutureAdapter<FutureBootstrap>() {
			@Override
			public void operationComplete(FutureBootstrap future)
					throws Exception {
				// send the peer map to the relays
				List<Map<Number160, PeerStatistic>> peerMapVerified = relayRPC.peer().peerBean().peerMap().peerMapVerified();
				for (final Map.Entry<PeerAddress, PeerConnection> entry : distributedRelay.activeClients().entrySet()) {
					relayRPC.sendPeerMap(entry.getKey(), entry.getValue(), peerMapVerified);
					LOG.debug("send peermap to {}", entry.getKey());
				}
			}
		});
	}
}
