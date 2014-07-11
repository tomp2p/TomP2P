package net.tomp2p.nat;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.DiscoverBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.DistributedRelay;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.RelayListener;
import net.tomp2p.relay.RelayRPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerNAT {

	private static final Logger LOG = LoggerFactory.getLogger(PeerNAT.class);

	final private Peer peer;
	final private NATUtils natUtils;
	final private RelayRPC relayRPC;

	private BootstrapBuilder bootstrapBuilder;
	private int peerMapUpdateInterval = 5;
	private int failedRelayWaitTime = 60;
	private int minRelays = 2;
	private int maxFail = 2;
	private Collection<PeerAddress> relays;

	public PeerNAT(Peer peer) {
		this.peer = peer;
		this.natUtils = new NATUtils();
		this.relayRPC = new RelayRPC(peer);

		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().done();
			}
		});

	}

	public RelayRPC relayRPC() {
		return relayRPC;
	}

	/**
	 * Setup UPNP or NATPMP port forwarding.
	 * 
	 * @param futureDiscover
	 *            The result of the discovery process. This information from the
	 *            discovery process is important to setup UPNP or NATPMP. If
	 *            this fails, then this future will also fail, and other means
	 *            to connect to the network needs to be found.
	 * @return The future object that tells you if you are reachable (success),
	 *         if UPNP or NATPMP could be setup and then you are reachable
	 *         (success), or if it failed.
	 */
	public FutureNAT startSetupPortforwarding(final FutureDiscover futureDiscover) {
		final FutureNAT futureNAT = new FutureNAT();
		futureDiscover.addListener(new BaseFutureAdapter<FutureDiscover>() {

			@Override
			public void operationComplete(FutureDiscover future) throws Exception {
				
				//set the peer that we contacted
				if(future.reporter()!=null) {
					futureNAT.discoverPeer(future.reporter());
				} else if(future.discoverPeer() != null) {
					futureNAT.discoverPeer(future.discoverPeer());
				}
				
				if (future.isFailed() && future.isNat()) {
					Ports externalPorts = setupPortforwarding(future.internalAddress().getHostAddress());
					if (externalPorts != null) {
						PeerAddress serverAddress = peer.peerBean().serverPeerAddress();
						serverAddress = serverAddress.changePorts(externalPorts.tcpPort(),
						        externalPorts.udpPort());
						serverAddress = serverAddress.changeAddress(future.externalAddress());
						peer.peerBean().serverPeerAddress(serverAddress);
						// test with discover again
						DiscoverBuilder builder = new DiscoverBuilder(peer).peerAddress(futureNAT.discoverPeer());
						builder.start().addListener(new BaseFutureAdapter<FutureDiscover>() {
							@Override
							public void operationComplete(FutureDiscover future) throws Exception {
								if (future.isSuccess()) {
									futureNAT.done(future.peerAddress(), future.reporter());
								} else {
									// indicate relay
									PeerAddress pa = peer.peerBean().serverPeerAddress().changeFirewalledTCP(true)
									        .changeFirewalledUDP(true);
									peer.peerBean().serverPeerAddress(pa);
									futureNAT.failed(future);
								}
							}
						});
					} else {
						// indicate relay
						PeerAddress pa = peer.peerBean().serverPeerAddress().changeFirewalledTCP(true)
						        .changeFirewalledUDP(true);
						peer.peerBean().serverPeerAddress(pa);
						futureNAT.failed("could not setup NAT");
					}
				} else {
					LOG.info("nothing to do, you are reachable from outside");
					futureNAT.done(futureDiscover.peerAddress(), futureDiscover.reporter());
				}
			}
		});
		return futureNAT;
	}

	/**
	 * The Dynamic and/or Private Ports are those from 49152 through 65535
	 * (http://www.iana.org/assignments/port-numbers).
	 * 
	 * @param internalHost
	 *            The IP of the internal host
	 * @return The new external ports if port forwarding seemed to be
	 *         successful, otherwise null
	 */
	public Ports setupPortforwarding(final String internalHost) {
		// new random ports
		Ports ports = new Ports();
		boolean success;

		try {
			success = natUtils.mapUPNP(internalHost, peer.peerAddress().tcpPort(), peer.peerAddress().udpPort(),
			        ports.udpPort(), ports.tcpPort());
		} catch (Exception e) {
			success = false;
		}

		if (!success) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("cannot find UPNP devices");
			}
			try {
				success = natUtils.mapPMP(peer.peerAddress().tcpPort(), peer.peerAddress().udpPort(),
				        ports.udpPort(), ports.tcpPort());
				if (!success) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("cannot find NAT-PMP devices");
					}
				}
			} catch (NatPmpException e1) {
				if (LOG.isWarnEnabled()) {
					LOG.warn("cannot find NAT-PMP devices ", e1);
				}
			}
		}
		if (success) {
			return ports;
		}
		return null;
	}

	public FutureRelay startSetupRelay(FutureNAT futureNAT) {
		final FutureRelay futureRelay = new FutureRelay();
		if(futureNAT == null) {
			startSetupRelay(futureRelay);
			return futureRelay;
		}
		futureNAT.addListener(new BaseFutureAdapter<FutureNAT>() {

			@Override
			public void operationComplete(FutureNAT future) throws Exception {
				if (future.isSuccess()) {
					futureRelay.nothingTodo();
				} else {
					startSetupRelay(futureRelay);
				}
			}
		});
		return futureRelay;
	}

	public FutureRelay startSetupRelay() {
		final FutureRelay futureRelay = new FutureRelay();
		startSetupRelay(futureRelay);
		return futureRelay;
	}

	private void startSetupRelay(final FutureRelay futureRelay) {
		final DistributedRelay distributedRelay = new DistributedRelay(peer, relayRPC, failedRelayWaitTime());
		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				return distributedRelay.shutdown();
			}
		});
		distributedRelay.addRelayListener(new RelayListener() {
			@Override
			public void relayFailed(final DistributedRelay distributedRelay, final PeerConnection peerConnection) {
				// one failed, add one
				final FutureRelay futureRelay2 = new FutureRelay(1);
				futureRelay2.distributedRelay(distributedRelay);
				distributedRelay.setupRelays(futureRelay2, relays, minRelays, maxFail);
				peer.notifyAutomaticFutures(futureRelay2);
			}
		});
		distributedRelay.setupRelays(futureRelay, relays, minRelays, maxFail);
		futureRelay.distributedRelay(distributedRelay);
	}

	public Shutdown startRelayMaintenance(final FutureRelay futureRelay) {
		if (bootstrapBuilder() == null) {
			throw new IllegalArgumentException(
			        "you need to set bootstrap builder first with PeerNAT.bootstrapBuilder()");
		}
		final PeerMapUpdateTask peerMapUpdateTask = new PeerMapUpdateTask(relayRPC, bootstrapBuilder(),
		        futureRelay.distributedRelay());
		peer.connectionBean().timer()
		        .scheduleAtFixedRate(peerMapUpdateTask, 0, peerMapUpdateInterval(), TimeUnit.SECONDS);

		final Shutdown shutdown = new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				peerMapUpdateTask.cancel();
				return new FutureDone<Void>().done();
			}
		};
		peer.addShutdownListener(shutdown);

		return new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				peerMapUpdateTask.cancel();
				peer.removeShutdownListener(shutdown);
				return new FutureDone<Void>().done();
			}
		};
	}
	
	public FutureRelayNAT startRelay() {
		return startRelay(null);
	}

	public FutureRelayNAT startRelay(final FutureNAT futureNAT) {
		if (bootstrapBuilder() == null) {
			if(futureNAT.reporter()!=null) {
				bootstrapBuilder(peer.bootstrap().peerAddress(futureNAT.reporter()));
			} else if(futureNAT.discoverPeer() != null) {
				bootstrapBuilder(peer.bootstrap().peerAddress(futureNAT.discoverPeer()));
			} else {
				throw new IllegalArgumentException(
			        "you need to set bootstrap builder first with PeerNAT.bootstrapBuilder()");
			}
		}
		final FutureRelayNAT futureBootstrapNAT = new FutureRelayNAT();
		
		PeerAddress upa = peer.peerBean().serverPeerAddress();
		upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		peer.peerBean().serverPeerAddress(upa);
		// find neighbors

		FutureBootstrap futureBootstrap = bootstrapBuilder().start();
		futureBootstrapNAT.futureBootstrap0(futureBootstrap);

		futureBootstrap.addListener(new BaseFutureAdapter<FutureBootstrap>() {
			@Override
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					// setup relay
					final FutureRelay futureRelay = startSetupRelay(futureNAT);
					futureBootstrapNAT.futureRelay(futureRelay);
					futureRelay.addListener(new BaseFutureAdapter<FutureRelay>() {

						@Override
						public void operationComplete(FutureRelay future) throws Exception {
							// find neighbors again
							if (future.isSuccess()) {
								FutureBootstrap futureBootstrap = bootstrapBuilder().start();
								futureBootstrapNAT.futureBootstrap1(futureBootstrap);
								futureBootstrap.addListener(new BaseFutureAdapter<FutureBootstrap>() {
									@Override
									public void operationComplete(FutureBootstrap future) throws Exception {
										if (future.isSuccess()) {
											Shutdown shutdown = startRelayMaintenance(futureRelay);
											futureBootstrapNAT.done(shutdown);
										} else {
											futureBootstrapNAT.failed(future);
										}
									}
								});
							} else {
								futureBootstrapNAT.failed(future);
							}
						}
					});
				} else {
					futureBootstrapNAT.failed(future);
				}
			}
		});
		return futureBootstrapNAT;
	}

	/**
	 * Defines how many seconds to wait at least until asking a relay that
	 * denied a relay request or a relay that failed to act as a relay again
	 * 
	 * @param failedRelayWaitTime
	 *            wait time in seconds
	 * @return this instance
	 */
	public PeerNAT failedRelayWaitTime(int failedRelayWaitTime) {
		this.failedRelayWaitTime = failedRelayWaitTime;
		return this;
	}

	/**
	 * @return How many seconds to wait at least until asking a relay that
	 *         denied a relay request or a relay that failed to act as a relay
	 *         again
	 */
	public int failedRelayWaitTime() {
		return failedRelayWaitTime;
	}

	/**
	 * Defines how many relays have to be set up. If less than minRelays relay
	 * peers could be set up, it is considered a fail.
	 * 
	 * @param minRelays
	 *            minimum amount of relays
	 * @return this instance
	 */
	public PeerNAT minRelays(int minRelays) {
		this.minRelays = minRelays;
		return this;
	}

	/**
	 * @return How many relays have to be set up. If less than minRelays relay
	 *         peers could be set up, it is considered a fail.
	 */
	public int minRelays() {
		return minRelays;
	}
	
	public PeerNAT maxFail(int maxFail) {
		this.maxFail = maxFail;
		return this;
	}

	public int maxFail() {
		return maxFail;
	}

	/**
	 * Defines the time interval of sending the peer map of the unreachable peer
	 * to its relays. The routing requests are not relayed to the unreachable
	 * peer but handled by the relay peers. Therefore, the relay peers should
	 * always have an up-to-date peer map of the relayed peer
	 * 
	 * @param peerMapUpdateInterval
	 *            interval of updates in seconds
	 * @return this instance
	 */
	public PeerNAT peerMapUpdateInterval(int peerMapUpdateInterval) {
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		return this;
	}

	/**
	 * @return the peer map update interval in seconds
	 */
	public int peerMapUpdateInterval() {
		return peerMapUpdateInterval;
	}

	/**
	 * Specify a bootstrap builder that will be used to bootstrap during the
	 * process of setting up relay peers and after that.
	 * 
	 * @param bootstrapBuilder
	 *            The bootstrap builder
	 * @return this instance
	 */
	public PeerNAT bootstrapBuilder(BootstrapBuilder bootstrapBuilder) {
		this.bootstrapBuilder = bootstrapBuilder;
		return this;
	}
	
	public PeerNAT bootstrapBuilder(final PeerAddress peerAddress) {
		this.bootstrapBuilder = peer.bootstrap().peerAddress(peerAddress);
		return this;
	}

	/**
	 * @return Get a bootstrap builder that will be used to bootstrap during the
	 *         process of setting up relay peers and after that.
	 */
	public BootstrapBuilder bootstrapBuilder() {
		return bootstrapBuilder;
	}

	public Collection<PeerAddress> relays() {
		return relays;
	}

	public PeerNAT relays(Collection<PeerAddress> relays) {
		this.relays = relays;
		return this;
	}
}
