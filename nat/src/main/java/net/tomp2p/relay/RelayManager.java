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
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerStatatistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayManager {

    private class PeerMapUpdateTask extends TimerTask {
        @Override
        public void run() {
            if (peer.isShutdown()) {
                this.cancel();
            }
            if (!peer.getPeerAddress().isRelay()) {
                return;
            }

            // bootstrap to get updated peer map and then push it to the relay peers
            FutureBootstrap fb = peer.bootstrap().setPeerAddress(bootstrapAddress).start();

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

    private final int maxRelays;
    private final Peer peer;
    private PeerAddress bootstrapAddress;
    private final LinkedHashSet<PeerAddress> relayCandidates;
    private Semaphore relaySemaphore;

    private Set<PeerAddress> relayAddresses;

    public RelayManager(final Peer peer, PeerAddress bootstrapAddress) {
        this(peer, bootstrapAddress, PeerAddress.MAX_RELAYS);
    }

    public RelayManager(final Peer peer, PeerAddress bootstrapAddress, int maxRelays) {
        this.peer = peer;
        this.bootstrapAddress = bootstrapAddress;
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
        FutureBootstrap fb = peer.bootstrap().setPeerAddress(bootstrapAddress).start();
        fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
            public void operationComplete(FutureBootstrap future) throws Exception {
                if (future.isSuccess()) {
                    relayCandidates.addAll(peer.getDistributedRouting().peerMap().getAll());
                    logger.debug("Found {} peers that could act as relays", relayCandidates.size());
                } else {
                    logger.error("Bootstrapping failed: {}", future.getFailedReason());
                    futureDone.setFailed(future.getFailedReason());
                }
                futureDone.setDone();
            }
        });
        return futureDone;
    }

    public Collection<PeerAddress> getRelayAddresses() {
        return relayAddresses;
    }

    public Collection<PeerAddress> getRelayCandidates() {
        return relayCandidates;
    }

    public int maxRelays() {
        return maxRelays;
    }

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

    private void removeRelay(PeerAddress pa) {
        relayAddresses.remove(pa);
        relaySemaphore.release();
    }

    private FutureDone<Void> setupPeerConnections(final ChannelCreator cc) {
        FutureDone<Void> fd = new FutureDone<Void>();

        int nrOfRelays = relaySemaphore.availablePermits();

        if (nrOfRelays > 0) {
            RelayConnectionFuture[] relayConnectionFutures = new RelayConnectionFuture[nrOfRelays];
            relaySetupLoop(relayConnectionFutures, relayCandidates, cc, nrOfRelays, fd);
        } else {
            fd.setDone();
        }

        return fd;
    }

    public RelayFuture setupRelays() {

        final RelayFuture rf = new RelayFuture(this);

        if (!peer.getPeerAddress().isRelay()) {

            // set data object reply to answer incoming messages from the relay
            // peers
            peer.setRawDataReply(new RelayReply(peer.getConnectionBean().dispatcher()));

            // Set firewalled flag to avoid that other peers add this peer to
            // their routing tables
            peer.getPeerBean().serverPeerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true);

            // Start routing table update thread
            startPeerMapUpdateTask();
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
                                            FutureBootstrap fb = peer.bootstrap().setPeerAddress(bootstrapAddress).start();
                                            fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
                                                public void operationComplete(FutureBootstrap future) throws Exception {
                                                    if (future.isSuccess()) {
                                                        rf.done();
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

    private void startPeerMapUpdateTask() {
        new Timer().schedule(new PeerMapUpdateTask(), 0, ROUTING_UPDATE_TIME);
    }

}
