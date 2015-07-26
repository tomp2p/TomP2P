package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.utils.ConcurrentCacheSet;

/**
 * The relay manager is responsible for setting up and maintaining connections
 * to relay peers and contains all information about the relays.
 * 
 * @author Raphael Voellmy
 * @author Thomas Bocek
 * @author Nico Rutishauser
 * 
 */
public class DistributedRelay /*implements BufferRequestListener*/ {

	private final static Logger LOG = LoggerFactory.getLogger(DistributedRelay.class);

	private final Peer peer;
	private final RelayRPC relayRPC;

	//private final List<BaseRelayClient> relayClients;
	private final Set<PeerAddress> failedRelays;
	private final Map<PeerAddress, PeerConnection> activeClients;

	//private final Collection<RelayListener> relayListeners;
	private final RelayClientConfig relayConfig;
	
	private FutureDone<Void> shutdownFuture = new FutureDone<Void>();
	
	private volatile boolean shutdown = false;
		
	final private ExecutorService executorService;

	/**
	 * @param peer
	 *            the unreachable peer
	 * @param relayRPC
	 *            the relay RPC
	 * @param maxFail 
	 * @param relayConfig 
	 * @param maxRelays
	 *            maximum number of relay peers to set up
	 * @param relayType
	 *            the kind of the relay connection
	 */
	public DistributedRelay(final Peer peer, RelayRPC relayRPC, RelayClientConfig relayConfig, ExecutorService executorService) {
		this.peer = peer;
		this.relayRPC = relayRPC;
		this.relayConfig = relayConfig;
		this.executorService = executorService;

		activeClients = Collections.synchronizedMap(new HashMap<PeerAddress, PeerConnection>());
		failedRelays = new ConcurrentCacheSet<PeerAddress>(relayConfig.failedRelayWaitTime());
		//relayListeners = Collections.synchronizedList(new ArrayList<RelayListener>(1));
	}

	public RelayClientConfig relayConfig() {
		return relayConfig;
	}

	/**
	 * Returns connections to current relay peers
	 * 
	 * @return List of PeerAddresses of the relay peers (copy)
	 */
	public Map<PeerAddress, PeerConnection> activeClients() {
		synchronized (activeClients) {
			// make a copy
			return Collections.unmodifiableMap(new HashMap<PeerAddress, PeerConnection>(activeClients));
		}
	}

	/*public void addRelayListener(RelayListener relayListener) {
		synchronized (relayListeners) {
			relayListeners.add(relayListener);
		}
	}*/

	public FutureDone<Void> shutdown() {
		shutdown = true;
		synchronized (activeClients) {
			for (Map.Entry<PeerAddress, PeerConnection> entry: activeClients.entrySet()) {
				entry.getValue().close();
			}
		}
		executorService.shutdown();
		synchronized (peer) {
			peer.notify();
		}
		return shutdownFuture;
	}

	/**
	 * Sets up relay connections to other peers. The number of relays to set up
	 * is determined by {@link PeerAddress#MAX_RELAYS} or passed to the
	 * constructor of this class. It is important that we call this after we
	 * bootstrapped and have recent information in our peer map.
	 * 
	 * @return RelayFuture containing a {@link DistributedRelay} instance
	 */
	public DistributedRelay setupRelays(final RelayCallback relayCallback) {
		executorService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					startConnectionsLoop(relayCallback);
				} catch (Exception e) {
					relayCallback.onFailure(e);
				}
			}
		});
		return this;
	}
	
	private List<PeerAddress> relayCandidates() {
		final List<PeerAddress> relayCandidates;
		if (relayConfig.manualRelays().isEmpty()) {
			// Get the neighbors of this peer that could possibly act as relays. Relay
			// candidates are neighboring peers that are not relayed themselves and have
			// not recently failed as relay or denied acting as relay.
			relayCandidates = peer.distributedRouting().peerMap().all();
			// remove those who we know have failed
			relayCandidates.removeAll(failedRelays);
		} else {
			// if the user sets manual relays, the failed relays are not removed, as this has to be done by
			// the user
			relayCandidates = new ArrayList<PeerAddress>(relayConfig.manualRelays());
		}

		//filterRelayCandidates
		for (Iterator<PeerAddress> iterator = relayCandidates.iterator(); iterator.hasNext();) {
			PeerAddress candidate = iterator.next();

			// filter peers that are relayed themselves
			if (candidate.isRelayed()) {
				iterator.remove();
				continue;
			}

			//  Remove recently failed relays, peers that are relayed themselves and
			// peers that are already relays
			if (activeClients.containsKey(candidate)) {
				iterator.remove();
			}
		}
		LOG.trace("Found {} addtional relay candidates: {}", relayCandidates.size(), relayCandidates);
		
		return relayCandidates;
	}
	
	/**
	 * The relay setup is called sequentially until the number of max relays is reached. If a peerconnection goes down, it will search for other relays
	 * @param relayCallback 
	 * @throws InterruptedException 
	 */
	
	
	private void startConnectionsLoop(final RelayCallback relayCallback) throws InterruptedException {

		if(shutdown && activeClients.isEmpty()) {
			shutdownFuture.done();
			LOG.debug("shutting down, don't restart relays");
			return;
		}
		
		if(shutdown) {
			return;
		}
		
		if(activeClients.size() >= relayConfig.type().maxRelayCount()) {
			LOG.debug("we have enough relays");
			//wait at most x seconds for a restart of the loop
			executorService.submit(new Runnable() {
				@Override
				public void run() { 
					try {
						synchronized (peer) {
							peer.wait(60 * 1000);
						}
						startConnectionsLoop(relayCallback);
					} catch (Exception e) {
						relayCallback.onFailure(e);
					}
				}
			});
			return;
		}
		
		//get candidates
		final List<PeerAddress> relayCandidates = relayCandidates();
		if(relayCandidates.isEmpty()) {
			LOG.debug("no more relays");
			executorService.submit(new Runnable() {
				@Override
				public void run() { 
					try {
						synchronized (peer) {
							peer.wait(60 * 1000);
						}
						startConnectionsLoop(relayCallback);
					} catch (Exception e) {
						relayCallback.onFailure(e);
					}
				}
			});
			return;
		}
		
		final PeerAddress candidate = relayCandidates.get(0);
		final FutureDone<PeerConnection> futureDone = relayRPC.sendSetupMessage(candidate, relayConfig);
		futureDone.addListener(new BaseFutureAdapter<FutureDone<PeerConnection>>() {
			@Override
			public void operationComplete(final FutureDone<PeerConnection> future)
					throws Exception {
				
				if(future.isSuccess()) {
					LOG.debug("found relay: {}", candidate);
					activeClients.put(candidate, future.object());
					updatePeerAddress();
					relayCallback.onRelayAdded(candidate, future.object());
					
					future.object().closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
						@Override
						public void operationComplete(final FutureDone<Void> futureClose)
								throws Exception {
							LOG.debug("lost/offline relay: {}", candidate);
							failedRelays.add(future.object().remotePeer());
							
							activeClients.remove(candidate, future.object());
							updatePeerAddress();
							relayCallback.onRelayRemoved(candidate, future.object());
							
							//notify to loop now - this may not do anything if we are shutting down
							synchronized (peer) {
								peer.notify();
							}
							
							if(shutdown && activeClients.isEmpty()) {
								shutdownFuture.done();
							}
						}
					});
				} else {
					LOG.debug("bad relay: {}", candidate);
					activeClients.remove(candidate, future.object());
					updatePeerAddress();
					relayCallback.onRelayRemoved(candidate, future.object());
				}
				//loop again
				startConnectionsLoop(relayCallback);
			}
		});		
	}

	/**
	 * Updates the peer's PeerAddress: Adds the relay addresses to the peer
	 * address, updates the firewalled flags, and bootstraps to announce its new
	 * relay peers.
	 */
	private void updatePeerAddress() {
		final boolean hasRelays;
		final Collection<PeerSocketAddress> socketAddresses;
		synchronized (activeClients) {
			// add relay addresses to peer address
			hasRelays = !activeClients.isEmpty();
			socketAddresses = new ArrayList<PeerSocketAddress>(activeClients.size());
		
			//we can have more than the max relay count in our active client list.
			int max = relayConfig.type().maxRelayCount();
			int i = 0;
			for (PeerAddress relay : activeClients.keySet()) {
				socketAddresses.add(new PeerSocketAddress(relay.inetAddress(), relay.tcpPort(), relay.udpPort()));
				if(i++ >= max) {
					break;
				}
			}
		}

		// update firewalled and isRelayed flags
		PeerAddress newAddress = peer.peerAddress().changeFirewalledTCP(!hasRelays).changeFirewalledUDP(!hasRelays)
				.changeRelayed(hasRelays).changePeerSocketAddresses(socketAddresses).changeSlow(hasRelays && relayConfig.type().isSlow());
		peer.peerBean().serverPeerAddress(newAddress);
		LOG.debug("Updated peer address {}, isrelay = {}", newAddress, hasRelays);
	}


	/*@Override
	public FutureDone<Void> sendBufferRequest(String relayPeerId) {
		for (BaseRelayClient relayConnection : relayClients()) {
			String peerId = relayConnection.relayAddress().peerId().toString();
			if (peerId.equals(relayPeerId) && relayConnection instanceof BufferedRelayClient) {
				return ((BufferedRelayClient) relayConnection).sendBufferRequest();
			}
		}

		LOG.warn("No connection to relay {} found. Ignoring the message.", relayPeerId);
		return new FutureDone<Void>().failed("No connection to relay " + relayPeerId + " found");
	}*/
}
