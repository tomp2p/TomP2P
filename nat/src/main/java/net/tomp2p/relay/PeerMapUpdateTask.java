package net.tomp2p.relay;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.relay.buffer.BufferedRelayClient;
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
		bootstrapBuilder.start().awaitUninterruptibly(BOOTSTRAP_TIMEOUT_MS);

		// send the peer map to the relays
		List<Map<Number160, PeerStatistic>> peerMapVerified = relayRPC.peer().peerBean().peerMap().peerMapVerified();
		for (final BaseRelayClient relay : distributedRelay.relays()) {
			sendPeerMap(relay, peerMapVerified);
		}

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
	private void sendPeerMap(final BaseRelayClient connection, List<Map<Number160, PeerStatistic>> map) {
		LOG.debug("Sending current routing table to relay {}", connection.relayAddress());

		final Message message = relayRPC
				.createMessage(connection.relayAddress(), RPC.Commands.RELAY.getNr(), Type.REQUEST_3);
		// TODO: neighbor size limit is 256, we might have more here
		message.neighborsSet(new NeighborSet(-1, RelayUtils.flatten(map)));
		
		// append relay-type specific data (if necessary)
		distributedRelay.relayConfig().prepareMapUpdateMessage(message);
		
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
					if(connection instanceof BufferedRelayClient) {
						BufferedRelayClient bufferedConn = (BufferedRelayClient) connection;
						bufferedConn.onReceiveMessageBuffer(future.responseMessage(), new FutureDone<Void>());
					}
				}
			}
		});
	}
}
