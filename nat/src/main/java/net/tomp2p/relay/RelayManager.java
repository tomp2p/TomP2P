package net.tomp2p.relay;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerStatatistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The relay manager is responsible for setting up and maintaining connections
 * to relay peers and contains all information about the relays.
 * 
 * @author Raphael Voellmy
 * 
 */
public class RelayManager {

    /**
     * The PeerMapUpdateTask is responsible for periodically sending the
     * unreachable peers PeerMap to its relays. This is important as the relay
     * peers respond to routing requests on behalf of the unreachable peers
     * 
     */
    private class PeerMapUpdateTask extends TimerTask {
        
        @Override
        public void run() {
            if (peer.isShutdown()) {
                this.cancel();
            }
            if (!peer.getPeerAddress().isRelayed()) {
                return;
            }

            // bootstrap to get updated peer map and then push it to the relay peers
            FutureBootstrap fb = bootstrapBuilder.start();
            fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
                public void operationComplete(FutureBootstrap future) throws Exception {
                    if (future.isSuccess()) {
                        List<Map<Number160, PeerStatatistic>> peerMapVerified = peer.getPeerBean().peerMap().peerMapVerified();
                        for (final PeerAddress relay : relayAddresses) {
                            FutureDirect fd = peer.sendDirect(relay).setObject(peerMapVerified).start();
                            fd.addListener(new BaseFutureAdapter<BaseFuture>() {
                                public void operationComplete(BaseFuture future) throws Exception {
                                    if (future.isFailed()) {
                                        logger.warn("failed to update peer map on relay peer {}: {}", relay, future.getFailedReason());
                                    } else {
                                        logger.trace("Updated peer map on relay {}", relay);
                                    }
                                }

                            });
                        }
                    }
                }
            });
        }
    }

    final private static Logger logger = LoggerFactory.getLogger(RelayManager.class);

    private final static long ROUTING_UPDATE_TIME = 10L * 1000;

    private final RelayManager self;
    private final int maxRelays;
    private final Peer peer;
    private final LinkedHashSet<PeerAddress> relayCandidates;
    private Semaphore relaySemaphore;
    private Set<PeerAddress> relayAddresses;
    private final BootstrapBuilder bootstrapBuilder;

    /**
     * 
     * @param peer
     *            the unreachable peer
     * @param bootstrapBuilder
     */
    public RelayManager(final Peer peer, BootstrapBuilder bootstrapBuilder) {
        this(peer, bootstrapBuilder, PeerAddress.MAX_RELAYS);
    }

    /**
     * @param peer
     *            the unreachable peer
     * @param bootstrapBuilder
     * @param maxRelays
     *            maximum relay peers to set up
     */
    public RelayManager(final Peer peer, BootstrapBuilder bootstrapBuilder, int maxRelays) {
        this.self = this;
        this.peer = peer;
        this.bootstrapBuilder = bootstrapBuilder;
        this.relayCandidates = new LinkedHashSet<PeerAddress>();

        if (maxRelays > PeerAddress.MAX_RELAYS || maxRelays < 0) {
            logger.warn("at most {} relays are allowed.", PeerAddress.MAX_RELAYS);
            maxRelays = PeerAddress.MAX_RELAYS;
        }

        this.maxRelays = maxRelays;
        
        this.relaySemaphore = new Semaphore(maxRelays);

        relayAddresses = new CopyOnWriteArraySet<PeerAddress>();
    }

    /**
     * Updates the peer's PeerAddress: Adds the relay addresses to the peer
     * address, updates the firewalled flags, and bootstraps
     */
    private void updatePeerAddress() {

        // add relay addresses to peer address
        boolean hasRelays = !relayAddresses.isEmpty();
        PeerSocketAddress[] socketAddresses = null;
        if (hasRelays) {
            socketAddresses = new PeerSocketAddress[relayAddresses.size()];
            int index = 0;
            for (PeerAddress pa : relayAddresses) {
                socketAddresses[index] = new PeerSocketAddress(pa.getInetAddress(), pa.tcpPort(), pa.udpPort());
                index++;
            }
        } else {
            socketAddresses = new PeerSocketAddress[0];
        }
        // update firewalled and isRelay flags
        PeerAddress pa = peer.getPeerAddress();

        PeerSocketAddress psa = new PeerSocketAddress(pa.getInetAddress(), pa.tcpPort(), pa.udpPort());
        PeerAddress newAddress = new PeerAddress(pa.getPeerId(), psa, !hasRelays, !hasRelays, hasRelays, socketAddresses);
        peer.getPeerBean().serverPeerAddress(newAddress);

    }

    private FutureDone<Void> getNeighbors(final ChannelCreator cc) {
        final FutureDone<Void> futureDone = new FutureDone<Void>();

        // bootstrap to get neighbor peers
        FutureBootstrap fb = bootstrapBuilder.start();
        fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
            public void operationComplete(FutureBootstrap future) throws Exception {
                if (future.isSuccess()) {
                    relayCandidates.addAll(peer.getDistributedRouting().peerMap().getAll());
                    logger.debug("Found {} peers that could act as relays", relayCandidates.size());
                    
                    if(relayCandidates.isEmpty()) {
                        futureDone.setFailed("No other peers were found");
                    }
                    
                    futureDone.setDone();
                } else {
                    logger.error("Bootstrapping failed: {}", future.getFailedReason());
                    futureDone.setFailed(future);
                }
            }
        });
        return futureDone;
    }

    /**
     * Returns addresses of current relay peers
     * 
     * @return Collection of PeerAddresses of the relay peers
     */
    public Collection<PeerAddress> getRelayAddresses() {
        return relayAddresses;
    }

    /**
     * Relay candidates are all peers that are neighbors of the unreachable peer
     * and are not unreachable themselves
     * 
     * @return collection of peer addresses of peers that are candidates of
     *         becoming relay peers.
     */
    public Collection<PeerAddress> getRelayCandidates() {
        return relayCandidates;
    }

    /**
     * @return maximum number of allowed relay peers
     */
    public int maxRelays() {
        return maxRelays;
    }

    /**
     * Sets up connections to relay peers recursively. If the maximum number of
     * relays is already reached, this method will do nothing.
     * 
     * @param futureRelayConnections
     * @param relayCandidates
     *            List of peers that could act as relays
     * @param cc
     * @param numberOfRelays
     *            The number of relays to establish.
     * @param futureDone
     * @return
     */
    private FutureDone<Void> relaySetupLoop(final RelayConnectionFuture[] futureRelayConnections, final LinkedHashSet<PeerAddress> relayCandidates, final ChannelCreator cc,
            final int numberOfRelays, final FutureDone<Void> futureDone) {

        try {
            relaySemaphore.acquire(numberOfRelays);
        } catch (InterruptedException e) {
            futureDone.setFailed(e);
            return futureDone;
        }

        if (numberOfRelays == 0) {
            futureDone.setDone();
            return futureDone;
        }

        int active = 0;
        for (int i = 0; i < numberOfRelays; i++) {
            if (futureRelayConnections[i] == null) {
                PeerAddress candidate = relayCandidates.iterator().next();
                relayCandidates.remove(candidate);
                futureRelayConnections[i] = new RelayRPC(peer).setupRelay(candidate, cc);
                if (futureRelayConnections[i] != null) {
                    active++;
                }
            } else if (futureRelayConnections[i] != null) {
                active++;
            }
        }
        if (active == 0) {
            updatePeerAddress();
            futureDone.setDone();
        }

        FutureForkJoin<RelayConnectionFuture> ffj = new FutureForkJoin<RelayConnectionFuture>(new AtomicReferenceArray<RelayConnectionFuture>(futureRelayConnections));

        ffj.addListener(new BaseFutureAdapter<FutureForkJoin<RelayConnectionFuture>>() {
            public void operationComplete(FutureForkJoin<RelayConnectionFuture> future) throws Exception {
                if (future.isSuccess()) {
                    List<RelayConnectionFuture> reponses = future.getCompleted();
                    for (final RelayConnectionFuture fr : reponses) {
                        PeerAddress relayAddress = fr.relayAddress();
                        if (fr.isSuccess()) {
                            logger.debug("Adding peer {} as a relay", relayAddress);
                            relayAddresses.add(relayAddress);
                            
                            //update peer map
                            

                            FutureDone<Void> closeFuture = fr.futurePeerConnection().getObject().closeFuture();
                            closeFuture.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
                                public void operationComplete(FutureDone<Void> future) throws Exception {
                                    if (!peer.isShutdown()) {
                                        // peer connection not open
                                        // anymore -> remove and open a
                                        // new relay connection
                                        logger.debug("Relay " + fr.relayAddress() + " failed, setting up a new relay peer");
                                        removeRelay(fr.relayAddress());
                                        setupRelays();
                                        futureDone.setDone();
                                    }
                                }
                            });
                        } else {
                            logger.debug("Peer {} denied relay request", relayAddress);
                        }
                    }
                    updatePeerAddress();
                    futureDone.setDone();
                } else {
                    relaySetupLoop(futureRelayConnections, relayCandidates, cc, numberOfRelays, futureDone);
                }
            }
        });
        return futureDone;
    }

    /**
     * This method is used to remove a relay peer from the unreachable peers
     * peer address. It will <strong>not</strong> cut the connection to an
     * existing peer, but only update the unreachable peer's PeerAddress if a
     * relay peer failed.
     * 
     * @param pa the PeerAddress of the relay that has been removed
     */
    private void removeRelay(PeerAddress pa) {
        relayAddresses.remove(pa);
        relaySemaphore.release();
    }

    /**
     * Sets up N peer connections to relay candidates, where N is (maxRelays -
     * current relay count)
     * 
     * @param cc
     * @return
     */
    private FutureDone<Void> setupPeerConnections(final ChannelCreator cc) {
        FutureDone<Void> fd = new FutureDone<Void>();

        int nrOfRelays = Math.min(relaySemaphore.availablePermits(), relayCandidates.size());

        if (nrOfRelays > 0) {
            RelayConnectionFuture[] relayConnectionFutures = new RelayConnectionFuture[nrOfRelays];
            relaySetupLoop(relayConnectionFutures, relayCandidates, cc, nrOfRelays, fd);
        } else {
            fd.setDone();
        }

        return fd;
    }

    /**
     * Sets up relay connections to other peers.
     * 
     * @return RelayFuture containing a {@link RelayManager} instance
     */
    public RelayFuture setupRelays() {

        final RelayFuture rf = new RelayFuture();
        
        rf.addListener(new BaseFutureAdapter<RelayFuture>() {
            public void operationComplete(RelayFuture future) throws Exception {
                if(future.isSuccess()) {
                    // Start routing table update thread
                    startPeerMapUpdateTask();
                }
            }
        });

        if (!peer.getPeerAddress().isRelayed()) {

            // set data object reply to answer incoming messages from the relay
            // peers
            peer.setRawDataReply(new RelayReply(peer.getConnectionBean().dispatcher()));

            // Set firewalled flag to avoid that other peers add this peer to
            // their routing tables
            peer.getPeerBean().serverPeerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true);

        }

        // create channel creator
        FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(0, maxRelays);
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    final ChannelCreator cc = future.getChannelCreator();
                    FutureDone<Void> fd = getNeighbors(future.getChannelCreator());
                    fd.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
                        public void operationComplete(FutureDone<Void> future) throws Exception {
                            if (future.isSuccess()) {
                                // establish connections to relay peers
                                FutureDone<Void> fd = setupPeerConnections(cc);
                                fd.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
                                    public void operationComplete(FutureDone<Void> future) throws Exception {
                                        if (future.isSuccess()) {
                                            // bootstrap with the updated peer
                                            // address
                                            FutureBootstrap fb = bootstrapBuilder.start();
                                            fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
                                                public void operationComplete(FutureBootstrap future) throws Exception {
                                                    if (future.isSuccess()) {
                                                        rf.relayManager(self);
                                                    } else {
                                                        future.setFailed(future);
                                                    }
                                                }
                                            });
                                        } else {
                                            rf.setFailed(future);
                                        }
                                    }
                                });
                            } else {
                                rf.setFailed(future);
                            }
                        }
                    });
                } else {
                    rf.setFailed(future);
                }
            }
        });

        return rf;
    }

    /**
     * Starts the {@link PeerMapUpdateTask} task.
     */
    private void startPeerMapUpdateTask() {
        //Update peer maps of relay peers as soon as all relays are set up
        TimerTask updateTask = new PeerMapUpdateTask();
        updateTask.run();
        new Timer().schedule(new PeerMapUpdateTask(), 0, ROUTING_UPDATE_TIME);
    }

	public BaseFuture publishNeighbors() {
	    return null;
	    
    }

}
