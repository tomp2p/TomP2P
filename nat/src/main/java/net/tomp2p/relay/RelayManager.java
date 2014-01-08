package net.tomp2p.relay;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayManager {

	final private static Logger logger = LoggerFactory.getLogger(RelayManager.class);

	private final static long ROUTING_UPDATE_TIME = 60L * 1000;

	// settings
	private final int maxRelays;

	private final Peer peer;
	private PeerAddress peerAddress;
	private final Queue<PeerAddress> relayCandidates;
	private Set<PeerAddress> relayAddresses;

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

	public RelayManager(final Peer peer, PeerAddress peerAddress) {
		this(peer, peerAddress, PeerAddress.MAX_RELAYS);
	}

	public RelayFuture setupRelays() {

		final RelayFuture rf = new RelayFuture(this);

		if (!peer.getPeerAddress().isRelay()) {

			// set data object reply to answer incoming messages from the relay peers
			peer.setRawDataReply(new RelayReply(peer.getConnectionBean().dispatcher()));

			// Set firewalled flag to avoid that other peers add this peer to their routing tables
			peer.getPeerBean().serverPeerAddress().changeFirewalledTCP(true).changeFirewalledUDP(true);


		}
		// create channel creator
		FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(1, maxRelays);
		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {

			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					getNeighbors(rf, future.getChannelCreator());
				} else {
					rf.setFailed(future);
				}
			}
		});

		return rf;
	}

	private void getNeighbors(final RelayFuture rf, final ChannelCreator cc) {

		// bootstrap to get neighbor peers
		FutureBootstrap fb = peer.bootstrap().setPeerAddress(peerAddress).start();
		fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					relayCandidates.addAll(peer.getDistributedRouting().peerMap().getAll());
					logger.debug("Found {} peers that could act as relays", relayCandidates.size());
					setupPeerConnections(rf, cc);
				} else {
					logger.error("Bootstrapping failed: {}", future.getFailedReason());
					rf.setFailed(future.getFailedReason());
					rf.done();
				}
			}
		});
	}

	private void relaySetupLoop(final RelayConnectionFuture[] futureRelayConnections, final Queue<PeerAddress> relayCandidates, final ChannelCreator cc, final int numberOfRelays,
			final RelayFuture rf) {
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
			addRelays(rf);
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
										// peer connection not open anymore ->
										// remove and open a new relay
										// connection
										logger.debug("Relay " + fr.relayAddress() + " failed, setting up a new relay peer");
										removeRelay(fr.relayAddress());
										setupRelays();
									}
								}
							});
						} else {
							logger.debug("Peer {} denied relay request", relayAddress);
						}
					}
					addRelays(rf);
				} else {
					relaySetupLoop(futureRelayConnections, relayCandidates, cc, numberOfRelays, rf);
				}
			}
		});
	}

	/**
	 * Adds the relay addresses to the peer address, updates the firewalled flags, and bootstraps
	 */
	private void addRelays(final RelayFuture rf) {

		// add relay addresses to peer address
		PeerSocketAddress[] socketAddresses = new PeerSocketAddress[relayAddresses.size()];
		int index = 0;
		for (PeerAddress pa : relayAddresses) {
			socketAddresses[index] = new PeerSocketAddress(pa.getInetAddress(), pa.tcpPort(), pa.udpPort());
			index++;
		}

		// update firewalled and isRelay Flags
		PeerAddress pa = peer.getPeerAddress();
		PeerSocketAddress psa = new PeerSocketAddress(pa.getInetAddress(), pa.tcpPort(), pa.udpPort());
		PeerAddress newAddress = new PeerAddress(pa.getPeerId(), psa, false, false, true, socketAddresses);
		peer.getPeerBean().serverPeerAddress(newAddress);

		// bootstrap with the updated peer address
		bootstrap(rf);
	}

	private void removeRelay(PeerAddress pa) {
		relayAddresses.remove(pa);
		PeerSocketAddress[] socketAddresses = new PeerSocketAddress[relayAddresses.size()];
		int index = 0;
		for (PeerAddress relay : relayAddresses) {
			socketAddresses[index++] = new PeerSocketAddress(relay.getInetAddress(), relay.tcpPort(), relay.udpPort());
		}
	}

	private void bootstrap(final RelayFuture rf) {
		FutureBootstrap fb = peer.bootstrap().setPeerAddress(peerAddress).start();
		fb.addListener(new BaseFutureAdapter<FutureBootstrap>() {
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					rf.done();
				} else {
					rf.setFailed("Bootstrapping failed: " + future.getFailedReason());
				}
			}
		});
	}

	private void setupPeerConnections(final RelayFuture rf, final ChannelCreator cc) {
		final int targetRelayCount = Math.min(maxRelays - relayAddresses.size(), relayCandidates.size());
		RelayConnectionFuture[] relayConnectionFutures = new RelayConnectionFuture[targetRelayCount];
		relaySetupLoop(relayConnectionFutures, relayCandidates, cc, targetRelayCount, rf);
	}

	public Queue<PeerAddress> getRelayCandidates() {
		return relayCandidates;
	}

	public Collection<PeerAddress> getRelayAddresses() {
		return relayAddresses;
	}

	public int maxRelays() {
		return maxRelays;
	}

}
