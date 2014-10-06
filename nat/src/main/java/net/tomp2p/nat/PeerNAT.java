package net.tomp2p.nat;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.DiscoverBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.DistributedRelay;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.PeerMapUpdateTask;
import net.tomp2p.relay.RelayListener;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.relay.android.GCMServerCredentials;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerNAT {

	private static final Logger LOG = LoggerFactory.getLogger(PeerNAT.class);

	private final Peer peer;
	private final NATUtils natUtils;
	private final RelayRPC relayRPC;
	private final int peerMapUpdateInterval;
	private final int failedRelayWaitTime;
	private final int maxFail;
	private final boolean manualPorts;
	private final Collection<PeerAddress> manualRelays;
	private final ConnectionConfiguration config;
	
	// relaying
	private final DistributedRelay distributedRelay;
	
	private static final int MESSAGE_VERSION = 1;

	public PeerNAT(Peer peer, NATUtils natUtils, RelayRPC relayRPC, Collection<PeerAddress> manualRelays,
			int failedRelayWaitTime, int maxFail, int peerMapUpdateInterval, boolean manualPorts, RelayType relayType,
			GCMServerCredentials gcmServerCredentials, ConnectionConfiguration config) {
		this.peer = peer;
		this.natUtils = natUtils;
		this.relayRPC = relayRPC;
		this.manualRelays = manualRelays;
		this.failedRelayWaitTime = failedRelayWaitTime;
		this.maxFail = maxFail;
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		this.manualPorts = manualPorts;
		this.config = config;
		this.distributedRelay = new DistributedRelay(peer, relayRPC, failedRelayWaitTime, relayType, gcmServerCredentials, config);
	}

	public Peer peer() {
		return peer;
	}

	public NATUtils natUtils() {
		return natUtils;
	}

	public RelayRPC relayRPC() {
		return relayRPC;
	}

	/**
	 * @return the peer map update interval in seconds
	 */
	public int peerMapUpdateInterval() {
		return peerMapUpdateInterval;
	}

	/**
	 * @return How many seconds to wait at least until asking a relay that
	 *         denied a relay request or a relay that failed to act as a relay
	 *         again
	 */
	public int failedRelayWaitTime() {
		return failedRelayWaitTime;
	}

	public int maxFail() {
		return maxFail;
	}

	public void addRelay(PeerAddress relay) {
		synchronized (manualRelays) {
			manualRelays.add(relay);
		}
	}

	public Collection<PeerAddress> manualRelays() {
		return manualRelays;
	}

	public boolean isManualPorts() {
		return manualPorts;
	}
	
	public Collection<BaseRelayConnection> currentRelays() {
		return distributedRelay.relays();
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

				// set the peer that we contacted
				if (future.reporter() != null) {
					futureNAT.reporter(future.reporter());
				}

				if (future.isFailed() && future.isNat() && !isManualPorts()) {
					Ports externalPorts = setupPortforwarding(future.internalAddress().getHostAddress(), peer
							.connectionBean().channelServer().channelServerConfiguration().portsForwarding());
					if (externalPorts != null) {
						final PeerAddress serverAddress = peer.peerBean().serverPeerAddress()
								.changePorts(externalPorts.tcpPort(), externalPorts.udpPort())
								.changeAddress(future.externalAddress());

						// test with discover again
						DiscoverBuilder builder = new DiscoverBuilder(peer).peerAddress(futureNAT.reporter()).senderAddress(serverAddress);
						builder.start().addListener(new BaseFutureAdapter<FutureDiscover>() {
							@Override
							public void operationComplete(FutureDiscover future) throws Exception {
								if (future.isSuccess()) {
									// change the server address to reflect the ports from the NAT device
									peer.peerBean().serverPeerAddress(serverAddress);
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
				} else if (future.isSuccess()) {
					LOG.info("nothing to do, you are reachable from outside");
					futureNAT.done(futureDiscover.peerAddress(), futureDiscover.reporter());
				} else {
					LOG.info("not reachable, maybe your setup is wrong");
					futureNAT.failed("could discover anything", future);
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
	public Ports setupPortforwarding(final String internalHost, Ports ports) {
		// new random ports
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
				success = natUtils.mapPMP(peer.peerAddress().tcpPort(), peer.peerAddress().udpPort(), ports.udpPort(),
						ports.tcpPort());
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

	private DistributedRelay startSetupRelay(FutureRelay futureRelay) {
		// close the relay connection when the peer is shutdown
		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				return distributedRelay.shutdown();
			}
		});

		// open new relay when one failed
		distributedRelay.addRelayListener(new RelayListener() {
			@Override
			public void relayFailed(PeerAddress relayAddress) {
				// one failed, add new one
				final FutureRelay futureRelay2 = new FutureRelay();
				distributedRelay.setupRelays(futureRelay2, manualRelays, maxFail);
				peer.notifyAutomaticFutures(futureRelay2);
			}
		});

		distributedRelay.setupRelays(futureRelay, manualRelays, maxFail);
		return distributedRelay;
	}

	private Shutdown startRelayMaintenance(final FutureRelay futureRelay, BootstrapBuilder bootstrapBuilder,
			DistributedRelay distributedRelay) {
		final PeerMapUpdateTask peerMapUpdateTask = new PeerMapUpdateTask(relayRPC, bootstrapBuilder, distributedRelay,
				manualRelays, maxFail);
		peer.connectionBean().timer().scheduleAtFixedRate(peerMapUpdateTask, 0, peerMapUpdateInterval(), TimeUnit.SECONDS);

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

	public FutureRelayNAT startRelay(final PeerAddress peerAddress) {
		final BootstrapBuilder bootstrapBuilder = peer.bootstrap().peerAddress(peerAddress);
		return startRelay(bootstrapBuilder);
	}

	public FutureRelayNAT startRelay(BootstrapBuilder bootstrapBuilder) {
		final FutureRelayNAT futureBootstrapNAT = new FutureRelayNAT();
		return startRelay(futureBootstrapNAT, bootstrapBuilder);
	}

	public FutureRelayNAT startRelay(final FutureDiscover futureDiscover) {
		return startRelay(futureDiscover, null);
	}

	public FutureRelayNAT startRelay(final FutureDiscover futureDiscover, final FutureNAT futureNAT) {
		final FutureRelayNAT futureRelayNAT = new FutureRelayNAT();
		futureDiscover.addListener(new BaseFutureAdapter<FutureDiscover>() {
			@Override
			public void operationComplete(FutureDiscover future) throws Exception {
				if (future.isFailed()) {
					if (futureNAT != null) {
						handleFutureNat(futureDiscover.reporter(), futureNAT, futureRelayNAT);
					} else {
						BootstrapBuilder bootstrapBuilder = peer.bootstrap().peerAddress(futureDiscover.reporter());
						startRelay(futureRelayNAT, bootstrapBuilder);
					}
				} else {
					futureRelayNAT.done();
				}
			}
		});
		return futureRelayNAT;
	}

	private void handleFutureNat(final PeerAddress peerAddress, final FutureNAT futureNAT,
			final FutureRelayNAT futureRelayNAT) {
		futureNAT.addListener(new BaseFutureAdapter<FutureNAT>() {
			@Override
			public void operationComplete(FutureNAT future) throws Exception {
				if (future.isSuccess()) {
					// setup UPNP or NATPMP worked
					futureRelayNAT.done();
				} else {
					BootstrapBuilder bootstrapBuilder = peer.bootstrap().peerAddress(peerAddress);
					startRelay(futureRelayNAT, bootstrapBuilder);
				}
			}
		});
	}

	private FutureRelayNAT startRelay(final FutureRelayNAT futureBootstrapNAT, final BootstrapBuilder bootstrapBuilder) {
		PeerAddress upa = peer.peerBean().serverPeerAddress();
		upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true);
		peer.peerBean().serverPeerAddress(upa);
		// find neighbors

		FutureBootstrap futureBootstrap = bootstrapBuilder.start();
		futureBootstrapNAT.futureBootstrap0(futureBootstrap);

		futureBootstrap.addListener(new BaseFutureAdapter<FutureBootstrap>() {
			@Override
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					// setup relay
					LOG.debug("bootstrap completed");
					final FutureRelay futureRelay = new FutureRelay();
					final DistributedRelay distributedRelay = startSetupRelay(futureRelay);
					futureBootstrapNAT.futureRelay(futureRelay);
					futureRelay.addListener(new BaseFutureAdapter<FutureRelay>() {

						@Override
						public void operationComplete(FutureRelay future) throws Exception {
							// find neighbors again
							if (future.isSuccess()) {
								FutureBootstrap futureBootstrap = bootstrapBuilder.start();
								futureBootstrapNAT.futureBootstrap1(futureBootstrap);
								futureBootstrap.addListener(new BaseFutureAdapter<FutureBootstrap>() {
									@Override
									public void operationComplete(FutureBootstrap future) throws Exception {
										if (future.isSuccess()) {
											Shutdown shutdown = startRelayMaintenance(futureRelay, bootstrapBuilder, distributedRelay);
											futureBootstrapNAT.done(shutdown);
										} else {
											futureBootstrapNAT.failed("2nd FutureBootstrap failed", future);
										}
									}
								});
							} else {
								futureBootstrapNAT.failed("FutureRelay failed", future);
							}
						}
					});
				} else {
					futureBootstrapNAT.failed("FutureBootstrap failed", future);
				}
			}
		});
		return futureBootstrapNAT;
	}

	/**
	 * This Method creates a {@link PeerConnection} to an unreachable (behind a NAT) peer using an active
	 * relay of the unreachable peer. The connection will be kept open until close() is called.
	 * 
	 * @param relayPeerAddress
	 * @param unreachablePeerAddress
	 * @param timeoutSeconds
	 * @return {@link FutureDone}
	 * @throws TimeoutException
	 */
	public FutureDone<PeerConnection> startSetupRcon(final PeerAddress relayPeerAddress, final PeerAddress unreachablePeerAddress) {
		checkRconPreconditions(relayPeerAddress, unreachablePeerAddress);

		final FutureDone<PeerConnection> futureDone = new FutureDone<PeerConnection>();
		final FuturePeerConnection fpc = peer.createPeerConnection(relayPeerAddress);
		fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
			// wait for the connection to the relay Peer
			@Override
			public void operationComplete(FuturePeerConnection future) throws Exception {
				if (fpc.isSuccess()) {
					final PeerConnection peerConnection = fpc.peerConnection();
					if (peerConnection != null) {
						// create the necessary messages
						final Message setUpMessage = createSetupMessage(relayPeerAddress, unreachablePeerAddress);

						// send the message to the relay so it forwards it to the unreachable peer
						FutureResponse futureResponse = RelayUtils.send(peerConnection, peer.peerBean(),
								peer.connectionBean(), config, setUpMessage);

						// wait for the unreachable peer to answer
						futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
							@Override
							public void operationComplete(FutureResponse future) throws Exception {
								// get the PeerConnection which is cached in the PeerBean object
								final PeerConnection openPeerConnection = peer.peerBean().peerConnection(unreachablePeerAddress.peerId());
								if (openPeerConnection != null && openPeerConnection.isOpen()) {
									futureDone.done(openPeerConnection);
								} else {
									LOG.error("The reverse connection to the unreachable peer failed.");
									handleFail(futureDone, "No reverse connection could be established");
								}

								// can close the connection to the relay peer
								peerConnection.close();
							}
						});
					} else {
						handleFail(futureDone, "The PeerConnection was null!");
					}
				} else {
					handleFail(futureDone, "no channel could be established");
				}
			}

			private void handleFail(final FutureDone<PeerConnection> futureDone, final String failMessage) {
				LOG.error(failMessage);
				futureDone.failed(failMessage);
			}

			// this message is sent to the relay peer to initiate the rcon setup
			private Message createSetupMessage(final PeerAddress relayPeerAddress, final PeerAddress unreachablePeerAddress) {
				Message setUpMessage = new Message();
				setUpMessage.version(MESSAGE_VERSION);
				setUpMessage.sender(peer.peerAddress());
				setUpMessage.recipient(relayPeerAddress.changePeerId(unreachablePeerAddress.peerId()));
				setUpMessage.command(RPC.Commands.RCON.getNr());
				setUpMessage.type(Type.REQUEST_1);
				// setUpMessage.keepAlive(true);
				return setUpMessage;
			}
		});
		return futureDone;
	}

	/**
	 * This method checks if a reverse connection setup with startSetupRcon() is
	 * possible.
	 * 
	 * @param relayPeerAddress
	 * @param unreachablePeerAddress
	 * @param timeoutSeconds
	 */
	private void checkRconPreconditions(final PeerAddress relayPeerAddress, final PeerAddress unreachablePeerAddress) {
		// If we are already a relay of the unreachable peer, we shouldn't use a
		// reverse connection setup. It just doesn't make sense!
		if (peer.peerAddress().peerId().equals(relayPeerAddress.peerId())) {
			throw new IllegalStateException(
					"We are alredy a relay for the target peer. We shouldn't use a reverse connection to connect to the targeted peer!");
		}

		if (relayPeerAddress == null || unreachablePeerAddress == null) {
			throw new IllegalArgumentException("either the relay PeerAddress or the unreachablePeerAddress or both was/were null!");
		}
	}
}
