package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The relay manager is responsible for setting up and maintaining connections
 * to relay peers and contains all information about the relays.
 * 
 * @author Raphael Voellmy
 * @author Thomas Bocek
 * 
 */
public class DistributedRelay {

	final static Logger LOG = LoggerFactory.getLogger(DistributedRelay.class);

	final private Peer peer;
	final private RelayRPC relayRPC;

	// maybe store PeerConnection
	final private Set<PeerConnection> relayAddresses;
	final private Set<PeerAddress> failedRelays;

	final private Collection<RelayListener> relayListeners = new ArrayList<RelayListener>(1);

	final private FutureChannelCreator futureChannelCreator;

	/**
	 * @param peer
	 *            the unreachable peer
	 * @param relayRPC
	 *            the relay RPC
	 * @param maxRelays
	 *            maximum number of relay peers to set up
	 */
	public DistributedRelay(final Peer peer, RelayRPC relayRPC, int failedRelayWaitTime) {

		this.peer = peer;
		this.relayRPC = relayRPC;

		relayAddresses = new CopyOnWriteArraySet<PeerConnection>();
		failedRelays = new ConcurrentCacheSet<PeerAddress>(failedRelayWaitTime);
		// this needs to be kept open, as we want the peerconnection to stay
		// alive
		futureChannelCreator = peer.connectionBean().reservation().create(0, PeerAddress.MAX_RELAYS);
	}

	/**
	 * Returns addresses of current relay peers
	 * 
	 * @return Collection of PeerAddresses of the relay peers
	 */
	public Collection<PeerConnection> relayAddresses() {
		return relayAddresses;
	}

	public DistributedRelay addRelayListener(RelayListener relayListener) {
		relayListeners.add(relayListener);
		return this;
	}

	public FutureForkJoin<FutureDone<Void>> shutdown() {
		@SuppressWarnings("unchecked")
		FutureDone<Void>[] futureDones = new FutureDone[relayAddresses.size() + 1];
		final AtomicReferenceArray<FutureDone<Void>> futureDones2 = new AtomicReferenceArray<FutureDone<Void>>(
		        futureDones);
		int i = 1;
		for (PeerConnection peerConnection : relayAddresses) {
			futureDones2.set(i++, peerConnection.close());
		}

		final FutureDone<Void> futureChannelShutdown = new FutureDone<Void>();
		futureDones2.set(0, futureChannelShutdown);
		synchronized (this) {
			relayListeners.clear();
		}
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				future.channelCreator().shutdown().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
					@Override
					public void operationComplete(FutureDone<Void> future) throws Exception {
						futureChannelShutdown.done();
					}
				});
			}
		});

		return new FutureForkJoin<FutureDone<Void>>(futureDones2);
	}

	/**
	 * Sets up relay connections to other peers. The number of relays to set up
	 * is determined by {@link PeerAddress#MAX_RELAYS} or passed to the
	 * constructor of this class. It is important that we call this after we
	 * bootstrapped and have recent information in our peer map.
	 * 
	 * @return RelayFuture containing a {@link DistributedRelay} instance
	 */
	public FutureRelay setupRelays(final FutureRelay futureRelay, final Collection<PeerAddress> relays,
	        final int successRelays, final int maxFail) {

		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					final ChannelCreator cc = future.channelCreator();
					final Collection<PeerAddress> relayCandidates;
					if (relays == null) {
						relayCandidates = relayCandidates();
					} else {
						relayCandidates = new ArrayList<PeerAddress>(relays);
						filter(relayCandidates);
					}
					setupPeerConnections(futureRelay, cc, relayCandidates, successRelays, maxFail);
				} else {
					futureRelay.failed(future);
				}
			}
		});
		return futureRelay;
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
	private Set<PeerAddress> relayCandidates() {
		Set<PeerAddress> relayCandidates = new LinkedHashSet<PeerAddress>(peer.distributedRouting().peerMap()
		        .all());

		filter(relayCandidates);
		return relayCandidates;
	}

	/**
	 * remove recently failed relays, peers that are relayed themselves and
	 * peers that are already relays
	 */
	private void filter(Collection<PeerAddress> relayCandidates) {
		for (Iterator<PeerAddress> iterator = relayCandidates.iterator(); iterator.hasNext();) {
			PeerAddress pa = iterator.next();
			if (pa.isRelayed()) {
				iterator.remove();
				continue;
			}
			for (PeerConnection pc : relayAddresses) {
				if (pc.remotePeer().equals(pa)) {
					iterator.remove();
					break;
				}
			}
		}
		relayCandidates.removeAll(failedRelays);
		LOG.debug("Found {} peers that could act as relays", relayCandidates.size());
	}

	/**
	 * Sets up N peer connections to relay candidates, where N is maxRelays
	 * minus the current relay count.
	 * 
	 * @param cc
	 * @return FutureDone
	 */
	private void setupPeerConnections(final FutureRelay futureRelay, final ChannelCreator cc,
	        Collection<PeerAddress> relayCandidates, final int relaySuccess, final int maxFail) {
		int nrOfRelays = Math.min(PeerAddress.MAX_RELAYS - relayAddresses.size(), relayCandidates.size());
		nrOfRelays = Math.min(nrOfRelays, futureRelay.nrRelays());
		LOG.debug("setting up {} relays", nrOfRelays);
		if (nrOfRelays > 0) {
			@SuppressWarnings("unchecked")
			FutureDone<PeerConnection>[] futureDones = new FutureDone[nrOfRelays];
			AtomicReferenceArray<FutureDone<PeerConnection>> relayConnectionFutures = new AtomicReferenceArray<FutureDone<PeerConnection>>(
			        futureDones);
			setupPeerConnectionsRecursive(relayConnectionFutures, relayCandidates, cc, nrOfRelays, futureRelay,
			        relaySuccess, 0, maxFail);
		} else {
			futureRelay.failed("done");
		}
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
	 * @throws InterruptedException
	 */
	private void setupPeerConnectionsRecursive(final AtomicReferenceArray<FutureDone<PeerConnection>> futures,
	        final Collection<PeerAddress> relayCandidates, final ChannelCreator cc, final int numberOfRelays,
	        final FutureRelay futureRelay, final int relaySuccess, final int fail, final int maxFail) {
		int active = 0;
		for (int i = 0; i < numberOfRelays; i++) {
			if (futures.get(i) == null) {
				PeerAddress candidate = null;
				synchronized (relayCandidates) {
					if (!relayCandidates.isEmpty()) {
						candidate = relayCandidates.iterator().next();
						relayCandidates.remove(candidate);
					}
				}
				if(candidate !=null) {
					final FuturePeerConnection fpc = peer.createPeerConnection(candidate);
					FutureDone<PeerConnection> futureDone = relayRPC.setupRelay(cc, fpc);
					setupAddRealys(futureDone);
					futures.set(i, futureDone);
					active++;
				}
			} else {
				active++;
			}
		}
		if (active == 0) {
			updatePeerAddress();
			futureRelay.done(new ArrayList<PeerConnection>(relayAddresses));
			return;
		}
		if (fail > maxFail) {
			updatePeerAddress();
			futureRelay.failed("maxfail");
			return;
		}

		FutureForkJoin<FutureDone<PeerConnection>> ffj = new FutureForkJoin<FutureDone<PeerConnection>>(Math.min(
		        relaySuccess, active), false, futures);

		ffj.addListener(new BaseFutureAdapter<FutureForkJoin<FutureDone<PeerConnection>>>() {
			public void operationComplete(FutureForkJoin<FutureDone<PeerConnection>> futureForkJoin) throws Exception {
				if (futureForkJoin.isSuccess()) {
					updatePeerAddress();
					futureRelay.done(new ArrayList<PeerConnection>(relayAddresses));
				} else if (!peer.isShutdown()){
					setupPeerConnectionsRecursive(futures, relayCandidates, cc, numberOfRelays, futureRelay,
					        relaySuccess, fail + 1, maxFail);
				} else {
					futureRelay.failed("shutting down");
				}
			}
		});
	}

	private void setupAddRealys(final FutureDone<PeerConnection> futureDone) {
		futureDone.addListener(new BaseFutureAdapter<FutureDone<PeerConnection>>() {
			@Override
			public void operationComplete(FutureDone<PeerConnection> future) throws Exception {
				if (future.isSuccess()) {
					PeerConnection peerConnection = future.object();
					PeerAddress relayAddress = peerConnection.remotePeer();
					if (future.isSuccess()) {
						LOG.debug("Adding peer {} as a relay", relayAddress);
						relayAddresses.add(peerConnection);
						addCloseListener(peerConnection);
					} else {
						LOG.debug("Peer {} denied relay request", relayAddress);
						failedRelays.add(relayAddress);
					}
				} else {
					futureDone.failed(future);
				}
			}
		});
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
	private void addCloseListener(final PeerConnection peerConnection) {
		peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			public void operationComplete(FutureDone<Void> future) throws Exception {
				if (!peer.isShutdown()) {
					// peer connection not open anymore -> remove and open a new
					// relay connection
					PeerAddress failedRelay = peerConnection.remotePeer();
					LOG.debug("Relay " + failedRelay + " failed, setting up a new relay peer");

					// used to remove a relay peer from the unreachable peers
					// peer address. It will <strong>not</strong> cut the
					// connection to an
					// existing peer, but only update the unreachable peer's
					// PeerAddress if a
					// relay peer failed. It will also cancel the {@link
					// PeerMapUpdateTask} task
					// if the last relay is removed.
					relayAddresses.remove(peerConnection);
					failedRelays.add(failedRelay);
					updatePeerAddress();
					synchronized (this) {
						for (RelayListener relayListener : relayListeners) {
							relayListener.relayFailed(DistributedRelay.this, peerConnection);
						}
					}
				}
			}
		});
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
		for (PeerConnection pc : relayAddresses) {
			PeerAddress pa = pc.remotePeer();
			socketAddresses.add(new PeerSocketAddress(pa.inetAddress(), pa.tcpPort(), pa.udpPort()));
		}

		// update firewalled and isRelayed flags
		PeerAddress newAddress = peer.peerAddress().changeFirewalledTCP(!hasRelays).changeFirewalledUDP(!hasRelays)
		        .changeRelayed(hasRelays).changePeerSocketAddresses(socketAddresses);
		peer.peerBean().serverPeerAddress(newAddress);
		LOG.debug("update peer address {}, isrelay = {}", newAddress, hasRelays);
	}
}
