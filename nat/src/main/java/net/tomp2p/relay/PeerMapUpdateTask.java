package net.tomp2p.relay;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;

/**
 * The PeerMapUpdateTask is responsible for periodically sending the unreachable
 * peers PeerMap to its relays. This is important as the relay peers respond to
 * routing requests on behalf of the unreachable peers
 * 
 */
class PeerMapUpdateTask extends TimerTask {

    private RelayRPC relayRPC;
    private BootstrapBuilder bootstrapBuilder;
    private Set<PeerAddress> relayAddresses;

    /**
     * Create a new peer map update task.
     * 
     * @param relayRPC
     *            the RelayRPC of this peer
     * @param bootstrapBuilder
     *            bootstrap builder used to find neighbors of this peer
     * @param relayAddresses
     *            set of the relay addresses
     */
    public PeerMapUpdateTask(RelayRPC relayRPC, BootstrapBuilder bootstrapBuilder, Set<PeerAddress> relayAddresses) {
        this.relayRPC = relayRPC;
        this.bootstrapBuilder = bootstrapBuilder;
        this.relayAddresses = relayAddresses;

    }

    @Override
    public void run() {
        if (relayRPC.peer().isShutdown() || !relayRPC.peer().getPeerAddress().isRelayed()) {
            this.cancel();
            return;
        }

        // bootstrap to get updated peer map and then push it to the relay
        // peers
        FutureBootstrap fb = bootstrapBuilder.start();
        fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
            public void operationComplete(FutureBootstrap future) throws Exception {
                if (future.isSuccess()) {
                    List<Map<Number160, PeerStatatistic>> peerMapVerified = relayRPC.peer().getPeerBean().peerMap().peerMapVerified();
                    //TODO: synchronized?
                    for (final PeerAddress relay : relayAddresses) {
                        FutureChannelCreator fcc = relayRPC.peer().getConnectionBean().reservation().create(0, 1);
                        FutureResponse fr = relayRPC.sendPeerMap(relay, peerMapVerified, fcc);
                        fr.addListener(new BaseFutureAdapter<BaseFuture>() {
                            public void operationComplete(BaseFuture future) throws Exception {
                                if (future.isFailed()) {
                                    RelayManager.LOG.warn("failed to update peer map on relay peer {}: {}", relay, future.getFailedReason());
                                } else {
                                    RelayManager.LOG.trace("Updated peer map on relay {}", relay);
                                }
                            }
                        });
                    }
                }
            }
        });
    }
}