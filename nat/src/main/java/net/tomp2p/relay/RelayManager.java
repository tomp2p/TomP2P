package net.tomp2p.relay;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
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

            // bootstrap to get updated peer map and then push it to the relay
            // peers
            FutureBootstrap fb = peer.bootstrap().setPeerAddress(peerAddress).start();

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

    // settings
    private final int maxRelays;
    private final Peer peer;
    private PeerAddress peerAddress;
    private final Queue<PeerAddress> relayCandidates;

    private Set<PeerAddress> relayAddresses;

    public RelayManager(final Peer peer, PeerAddress peerAddress) {
        this(peer, peerAddress, PeerAddress.MAX_RELAYS);
    }

    public RelayManager(final Peer peer, PeerAddress peerAddress, int maxRelays) {
        this.peer = peer;
        this.peerAddress = peerAddress;
        this.relayCandidates = new ConcurrentLinkedQueue<PeerAddress>();

        if (maxRelays > PeerAddress.MAX_RELAYS || maxRelays < 0) {
            logger.warn("at most {} relays are allowed.", PeerAddress.MAX_RELAYS);
            maxRelays = PeerAddress.MAX_RELAYS;
        }

        this.maxRelays = maxRelays;

        relayAddresses = new CopyOnWriteArraySet<PeerAddress>();
    }

    /**
     * Adds the relay addresses to the peer address, updates the firewalled
     * flags, and bootstraps
     */
    private void updatePeerAddress() {

        Set<PeerAddress> relayAddressesCopy = new HashSet<PeerAddress>(relayAddresses);

        // add relay addresses to peer address
        boolean hasRelays = !relayAddressesCopy.isEmpty();
        PeerSocketAddress[] socketAddresses = null;
        if (hasRelays) {
            socketAddresses = new PeerSocketAddress[relayAddressesCopy.size()];
            int index = 0;
            for (PeerAddress pa : relayAddressesCopy) {
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
        FutureBootstrap fb = peer.bootstrap().setPeerAddress(peerAddress).start();
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

    public Queue<PeerAddress> getRelayCandidates() {
        return relayCandidates;
    }

    public int maxRelays() {
        return maxRelays;
    }

    private FutureDone<Void> relaySetupLoop(final RelayConnectionFuture[] futureRelayConnections, final Queue<PeerAddress> relayCandidates, final ChannelCreator cc,
            final int numberOfRelays, final FutureDone<Void> futureDone) {
        int active = 0;
        for (int i = 0; i < numberOfRelays; i++) {
            if (futureRelayConnections[i] == null) {
                futureRelayConnections[i] = new RelayRPC(peer).setupRelay(relayCandidates.poll(), cc);
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
                                        // peer connection not open anymore -> remove and open a new relay connection
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
        PeerSocketAddress[] socketAddresses = new PeerSocketAddress[relayAddresses.size()];
        int index = 0;
        for (PeerAddress relay : relayAddresses) {
            socketAddresses[index++] = new PeerSocketAddress(relay.getInetAddress(), relay.tcpPort(), relay.udpPort());
        }
    }

    private FutureDone<Void> setupPeerConnections(final ChannelCreator cc) {
        final FutureDone<Void> fd = new FutureDone<Void>();
        final int targetRelayCount = Math.min(maxRelays - relayAddresses.size(), relayCandidates.size());
        RelayConnectionFuture[] relayConnectionFutures = new RelayConnectionFuture[targetRelayCount];
        relaySetupLoop(relayConnectionFutures, relayCandidates, cc, targetRelayCount, fd);
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
        FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(1, maxRelays);
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
                                            FutureBootstrap fb = peer.bootstrap().setPeerAddress(peerAddress).start();
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
