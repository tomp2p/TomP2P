package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

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

    final static Logger LOG = LoggerFactory.getLogger(RelayManager.class);

    private final Peer peer;
    private final RelayManager self;
    private final RelayRPC relayRPC;
    private final BootstrapBuilder bootstrapBuilder;

    // settings
    private final int maxRelays;
    private final int minRelays;
    private final int peerMapUpdateInterval;
    private final int relaySearchInterval;

    // maybe store PeerConnection
    final Set<PeerAddress> relayAddresses;
    private final Set<PeerAddress> failedRelays;

    private final SearchRelaysTask searchRelaysTask;
    private boolean searchRelaysTaskRunning = false;
    private final PeerMapUpdateTask peerMapUpdateTask;
    private boolean peerMapUpdateTaskRunning = false;

    /**
     * @param peer
     *            the unreachable peer
     * @param relayRPC
     *            the relay RPC
     * @param maxRelays
     *            maximum number of relay peers to set up
     */
    public RelayManager(final Peer peer, RelayRPC relayRPC, BootstrapBuilder bootstrapBuilder, int maxRelays, int minRelays, int peerMapUpdateInterval, int relaySearchInterval,
            int failedRelayWaitTime) {

        this.self = this;
        this.peer = peer;
        this.relayRPC = relayRPC;
        this.bootstrapBuilder = bootstrapBuilder;

        if (maxRelays > PeerAddress.MAX_RELAYS || maxRelays < 0) {
            LOG.warn("at most {} relays are allowed.", PeerAddress.MAX_RELAYS);
            maxRelays = PeerAddress.MAX_RELAYS;
        }

        this.maxRelays = maxRelays;
        this.minRelays = minRelays;
        this.peerMapUpdateInterval = peerMapUpdateInterval;
        this.relaySearchInterval = relaySearchInterval;

        relayAddresses = new CopyOnWriteArraySet<PeerAddress>();
        failedRelays = new ConcurrentCacheSet<PeerAddress>(failedRelayWaitTime);

        searchRelaysTask = new SearchRelaysTask(this);
        peerMapUpdateTask = new PeerMapUpdateTask(relayRPC, bootstrapBuilder, relayAddresses);
    }

    /**
     * Adds a close listener for an open peer connection, so that if the
     * connection to the relay peer drops, a new relay is found and a new relay
     * connection is established
     * 
     * @param peerConnection
     *            the peer connection on which to add a close listener
     * @param bootstrapBuilder
     *            bootstrap builder, used to find neighbors of this peer
     */
    private void addCloseListener(final PeerConnection peerConnection, final BootstrapBuilder bootstrapBuilder) {
        peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
            public void operationComplete(FutureDone<Void> future) throws Exception {
                if (!peer.isShutdown()) {
                    // peer connection not open anymore -> remove and open a new
                    // relay connection
                    PeerAddress failedRelay = peerConnection.remotePeer();
                    LOG.debug("Relay " + failedRelay + " failed, setting up a new relay peer");
                    removeRelay(failedRelay);
                    failedRelays.add(failedRelay);
                    setupRelays();
                }
            }
        });
    }

    /**
     * Get the neighbors of this peer that could possibly act as relays. Relay
     * candidates are neighboring peers that are not relayed themselves and have
     * not recently failed as relay or denied acting as relay.
     * 
     * @param bootstrapBuilder
     *            The bootstrap builder used to bootstrap
     * @return FutureDone containing a collection of relay candidates
     */
    private FutureDone<Set<PeerAddress>> getRelayCandidates(BootstrapBuilder bootstrapBuilder) {
        final FutureDone<Set<PeerAddress>> futureDone = new FutureDone<Set<PeerAddress>>();

        // bootstrap to get neighbor peers
        FutureBootstrap fb = bootstrapBuilder.start();
        fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
            public void operationComplete(FutureBootstrap future) throws Exception {
                if (future.isSuccess()) {
                    Set<PeerAddress> relayCandidates = new LinkedHashSet<PeerAddress>(peer.getDistributedRouting().peerMap().getAll());

                    /**
                     * remove recently failed relays, peers that are relayed
                     * themselves and peers that are already relays
                     */
                    for (PeerAddress pa : relayCandidates) {
                        if (pa.isRelayed()) {
                            relayCandidates.remove(pa);
                        }
                    }
                    relayCandidates.removeAll(relayAddresses);
                    relayCandidates.removeAll(failedRelays);

                    LOG.debug("Found {} peers that could act as relays", relayCandidates.size());

                    if (relayCandidates.isEmpty()) {
                        futureDone.setFailed("No peers that could act as relays were found");
                    }
                    futureDone.setDone(relayCandidates);
                } else {
                    LOG.error("Bootstrapping failed: {}", future.getFailedReason());
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
    private void relaySetupLoop(final FutureDone<PeerConnection>[] futures, final Set<PeerAddress> relayCandidates, final ChannelCreator cc, final int numberOfRelays,
            final FutureDone<Void> futureDone, final BootstrapBuilder bootstrapBuilder) {

        if (numberOfRelays == 0) {
            futureDone.setDone();
            return;
        }

        int active = 0;
        for (int i = 0; i < numberOfRelays; i++) {
            if (futures[i] == null) {

                PeerAddress candidate = null;
                synchronized (relayCandidates) {
                    if (!relayCandidates.isEmpty()) {
                        candidate = relayCandidates.iterator().next();
                        relayCandidates.remove(candidate);
                    }
                }

                final FuturePeerConnection fpc = peer.createPeerConnection(candidate);
                futures[i] = relayRPC.setupRelay(cc, fpc);
                if (futures[i] != null) {
                    active++;
                }
            } else if (futures[i] != null) {
                active++;
            }
        }
        if (active == 0) {
            updatePeerAddress();
            futureDone.setDone();
        }

        FutureForkJoin<FutureDone<PeerConnection>> ffj = new FutureForkJoin<FutureDone<PeerConnection>>(new AtomicReferenceArray<FutureDone<PeerConnection>>(futures));

        ffj.addListener(new BaseFutureAdapter<FutureForkJoin<FutureDone<PeerConnection>>>() {
            public void operationComplete(FutureForkJoin<FutureDone<PeerConnection>> future) throws Exception {
                if (future.isSuccess()) {
                    List<FutureDone<PeerConnection>> reponses = future.getCompleted();
                    for (final FutureDone<PeerConnection> fr : reponses) {
                        PeerConnection peerConnection = fr.getObject();
                        PeerAddress relayAddress = peerConnection.remotePeer();
                        if (fr.isSuccess()) {
                            LOG.debug("Adding peer {} as a relay", relayAddress);
                            relayAddresses.add(relayAddress);
                            addCloseListener(peerConnection, bootstrapBuilder);
                            updatePeerAddress();
                        } else {
                            LOG.debug("Peer {} denied relay request", relayAddress);
                            failedRelays.add(relayAddress);
                        }
                    }
                    updatePeerAddress();
                    futureDone.setDone();
                } else {
                    relaySetupLoop(futures, relayCandidates, cc, numberOfRelays, futureDone, bootstrapBuilder);
                }
            }
        });

    }

    /**
     * This method is used to remove a relay peer from the unreachable peers
     * peer address. It will <strong>not</strong> cut the connection to an
     * existing peer, but only update the unreachable peer's PeerAddress if a
     * relay peer failed. It will also cancel the {@link PeerMapUpdateTask} task
     * if the last relay is removed.
     * 
     * @param pa
     *            the PeerAddress of the relay that has been removed
     */
    private void removeRelay(PeerAddress pa) {
        relayAddresses.remove(pa);

        // no more relays -> cancel the update peer map task
        if (relayAddresses.isEmpty()) {
            peerMapUpdateTask.cancel();
            peerMapUpdateTaskRunning = false;
        }
        
        updatePeerAddress();
    }

    /**
     * Sets up N peer connections to relay candidates, where N is maxRelays
     * minus the current relay count
     * 
     * @param cc
     * @return FutureDone
     */
    private FutureDone<Void> setupPeerConnections(final ChannelCreator cc, BootstrapBuilder bootstrapBuilder, Set<PeerAddress> relayCandidates) {

        synchronized (this) {
            FutureDone<Void> fd = new FutureDone<Void>();

            // start actively searching for new relays if not enough relays are
            // available
            if (relayAddresses.size() + relayCandidates.size() < minRelays) {
                startRelaySearchTask();
            }

            int nrOfRelays = Math.min(maxRelays - relayAddresses.size(), relayCandidates.size());

            if (nrOfRelays > 0) {
                @SuppressWarnings("unchecked")
                FutureDone<PeerConnection>[] relayConnectionFutures = new FutureDone[nrOfRelays];
                relaySetupLoop(relayConnectionFutures, relayCandidates, cc, nrOfRelays, fd, bootstrapBuilder);
            } else {
                fd.setFailed("done");
            }
            return fd;
        }

    }

    /**
     * Sets up relay connections to other peers. The number of relays to set up
     * is determined by {@link PeerAddress#MAX_RELAYS} or passed to the
     * constructor of this class.
     * 
     * @return RelayFuture containing a {@link RelayManager} instance
     */
    public RelayFuture setupRelays() {

        final RelayFuture rf = new RelayFuture(minRelays);

        // Send peer map to relays as soon as they are set up
        rf.addListener(new BaseFutureAdapter<RelayFuture>() {
            public void operationComplete(RelayFuture future) throws Exception {
                if (future.isSuccess()) {
                    startPeerMapUpdateTask();
                    searchRelaysTask.cancel();
                } else {
                    startRelaySearchTask();
                }
            }
        });

        if (!peer.getPeerAddress().isRelayed()) {
            // Set firewalled flag to avoid that other peers add this peer to
            // their routing tables
            PeerAddress pa = peer.getPeerBean().serverPeerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true);
            peer.getPeerBean().serverPeerAddress(pa);

        }

        // create channel creator
        FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(0, maxRelays);
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    final ChannelCreator cc = future.getChannelCreator();
                    FutureDone<Set<PeerAddress>> fd = getRelayCandidates(bootstrapBuilder);
                    fd.addListener(new BaseFutureAdapter<FutureDone<Set<PeerAddress>>>() {
                        public void operationComplete(FutureDone<Set<PeerAddress>> future) throws Exception {
                            if (future.isSuccess()) {
                                // establish connections to relay peers
                                FutureDone<Void> fd = setupPeerConnections(cc, bootstrapBuilder, future.getObject());
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
                    // Utils.addReleaseListener(cc, rf);
                } else {
                    rf.setFailed(future);
                }
            }
        });

        return rf;
    }

    /**
     * Starts the {@link PeerMapUpdateTask} task if it is not already running
     */
    private void startPeerMapUpdateTask() {
        // Update peer maps of relay peers
        if (!peerMapUpdateTaskRunning) {
            peer.getConnectionBean().timer().scheduleAtFixedRate(peerMapUpdateTask, 0, peerMapUpdateInterval, TimeUnit.SECONDS);
            peerMapUpdateTaskRunning = true;
        }
    }

    /**
     * Starts the {@link SearchRelaysTask} task, if it is not already running
     * 
     * @param bootstrapBuilder
     */
    private void startRelaySearchTask() {
        //actively search for new relays
        if(!searchRelaysTaskRunning) {
            peer.getConnectionBean().timer().scheduleAtFixedRate(searchRelaysTask, relaySearchInterval, relaySearchInterval, TimeUnit.SECONDS);
        }
        searchRelaysTaskRunning = true;
    }

    /**
     * Updates the peer's PeerAddress: Adds the relay addresses to the peer
     * address, updates the firewalled flags, and bootstraps to announce its new
     * relay peers.
     */
    private void updatePeerAddress() {

        // add relay addresses to peer address
        boolean hasRelays = !relayAddresses.isEmpty();

        Collection<PeerSocketAddress> socketAddresses = new ArrayList<PeerSocketAddress>(relayAddresses.size());
        for (PeerAddress pa : relayAddresses) {
            socketAddresses.add(new PeerSocketAddress(pa.getInetAddress(), pa.tcpPort(), pa.udpPort()));
        }

        // update firewalled and isRelayed flags
        PeerAddress newAddress = peer.getPeerAddress().changeFirewalledTCP(!hasRelays).changeFirewalledUDP(!hasRelays).changeRelayed(hasRelays)
                .changePeerSocketAddresses(socketAddresses);
        peer.getPeerBean().serverPeerAddress(newAddress);

    }

}
