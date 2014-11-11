package net.tomp2p.nat;

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
import net.tomp2p.relay.DistributedRelay;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.PeerMapUpdateTask;
import net.tomp2p.relay.RelayConfig;
import net.tomp2p.relay.RelayListener;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerNAT {

	private static final Logger LOG = LoggerFactory.getLogger(PeerNAT.class);

	private final Peer peer;
	private final NATUtils natUtils;
	private final RelayRPC relayRPC;
	private final int failedRelayWaitTime;
	private final int maxFail;
	private final boolean manualPorts;
	private final ConnectionConfiguration config;
	
	public PeerNAT(Peer peer, NATUtils natUtils, RelayRPC relayRPC, int failedRelayWaitTime, int maxFail,
			boolean manualPorts, ConnectionConfiguration config) {
		this.peer = peer;
		this.natUtils = natUtils;
		this.relayRPC = relayRPC;
		this.failedRelayWaitTime = failedRelayWaitTime;
		this.maxFail = maxFail;
		this.manualPorts = manualPorts;
		this.config = config;
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

	public boolean isManualPorts() {
		return manualPorts;
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

				if (future.isFailed() && future.isNat() && !manualPorts) {
					Ports externalPorts = setupPortforwarding(future.internalAddress().getHostAddress(), peer
							.connectionBean().channelServer().channelServerConfiguration().portsForwarding());
					if (externalPorts != null) {
						final PeerAddress serverAddressOrig = peer.peerBean().serverPeerAddress();
						final PeerAddress serverAddress = serverAddressOrig
								.changePorts(externalPorts.tcpPort(), externalPorts.udpPort())
								.changeAddress(future.externalAddress());
						
						//set the new address regardless wheter it will succeed or not. 
						// The discover will eventually check wheter the announced ip matches the one that it sees. 
						peer.peerBean().serverPeerAddress(serverAddress);
						
						// test with discover again
						DiscoverBuilder builder = new DiscoverBuilder(peer).peerAddress(futureNAT.reporter());
						builder.start().addListener(new BaseFutureAdapter<FutureDiscover>() {
							@Override
							public void operationComplete(FutureDiscover future) throws Exception {
								if (future.isSuccess()) {
									//UPNP or NAT-PMP was successful, set flag
									peer.peerBean().serverPeerAddress(serverAddress.changePortForwarding(true));
									peer.peerBean().serverPeerAddress().internalPeerSocketAddress(serverAddressOrig.peerSocketAddress());
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

	private DistributedRelay startSetupRelay(FutureRelay futureRelay, final RelayConfig relayConfig) {
		final DistributedRelay distributedRelay = new DistributedRelay(peer, relayRPC, failedRelayWaitTime, maxFail, config, relayConfig);
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
				distributedRelay.setupRelays(futureRelay2);
				peer.notifyAutomaticFutures(futureRelay2);
			}
		});

		distributedRelay.setupRelays(futureRelay);
		return distributedRelay;
	}
	
	private Shutdown startRelayMaintenance(final FutureRelay futureRelay, BootstrapBuilder bootstrapBuilder,
			DistributedRelay distributedRelay, int peerMapUpdateInterval) {
		final PeerMapUpdateTask peerMapUpdateTask = new PeerMapUpdateTask(relayRPC, bootstrapBuilder, distributedRelay);
		peer.connectionBean().timer().scheduleAtFixedRate(peerMapUpdateTask, 0, peerMapUpdateInterval, TimeUnit.SECONDS);

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
	
	public FutureRelayNAT startRelay(final RelayConfig relayConfig, final PeerAddress peerAddress) {
		final BootstrapBuilder bootstrapBuilder = peer.bootstrap().peerAddress(peerAddress);
		return startRelay(relayConfig, bootstrapBuilder);
	}

	public FutureRelayNAT startRelay(final RelayConfig relayConfig, BootstrapBuilder bootstrapBuilder) {
		final FutureRelayNAT futureBootstrapNAT = new FutureRelayNAT();
		return startRelay(relayConfig, futureBootstrapNAT, bootstrapBuilder);
	}

	public FutureRelayNAT startRelay(final RelayConfig relayConfig, final FutureDiscover futureDiscover) {
		return startRelay(relayConfig, futureDiscover, null);
	}

	public FutureRelayNAT startRelay(final RelayConfig relayConfig, final FutureDiscover futureDiscover, final FutureNAT futureNAT) {
		final FutureRelayNAT futureRelayNAT = new FutureRelayNAT();
		futureDiscover.addListener(new BaseFutureAdapter<FutureDiscover>() {
			@Override
			public void operationComplete(FutureDiscover future) throws Exception {
				if (future.isFailed()) {
					if (futureNAT != null) {
						handleFutureNat(relayConfig, futureDiscover.reporter(), futureNAT, futureRelayNAT);
					} else {
						BootstrapBuilder bootstrapBuilder = peer.bootstrap().peerAddress(futureDiscover.reporter());
						startRelay(relayConfig, futureRelayNAT, bootstrapBuilder);
					}
				} else {
					futureRelayNAT.done();
				}
			}
		});
		return futureRelayNAT;
	}

	private void handleFutureNat(final RelayConfig relayConfig, final PeerAddress peerAddress, final FutureNAT futureNAT,
			final FutureRelayNAT futureRelayNAT) {
		futureNAT.addListener(new BaseFutureAdapter<FutureNAT>() {
			@Override
			public void operationComplete(FutureNAT future) throws Exception {
				if (future.isSuccess()) {
					// setup UPNP or NATPMP worked
					futureRelayNAT.done();
				} else {
					BootstrapBuilder bootstrapBuilder = peer.bootstrap().peerAddress(peerAddress);
					startRelay(relayConfig, futureRelayNAT, bootstrapBuilder);
				}
			}
		});
	}

	private FutureRelayNAT startRelay(final RelayConfig relayConfig, final FutureRelayNAT futureRelayNAT, final BootstrapBuilder bootstrapBuilder) {
		PeerAddress upa = peer.peerBean().serverPeerAddress();
		upa = upa.changeFirewalledTCP(true).changeFirewalledUDP(true).changeSlow(relayConfig.type().isSlow());
		peer.peerBean().serverPeerAddress(upa);

		// find neighbors
		FutureBootstrap futureBootstrap = bootstrapBuilder.start();
		futureBootstrap.addListener(new BaseFutureAdapter<FutureBootstrap>() {
			@Override
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					// setup relay
					LOG.debug("bootstrap completed");
					final FutureRelay futureRelay = new FutureRelay();
					final DistributedRelay distributedRelay = startSetupRelay(futureRelay, relayConfig);
					futureRelayNAT.gcmMessageHandler(distributedRelay);
					futureRelayNAT.futureRelay(futureRelay);
					futureRelay.addListener(new BaseFutureAdapter<FutureRelay>() {

						@Override
						public void operationComplete(FutureRelay future) throws Exception {
							if (future.isSuccess()) {
								// find neighbors again
								FutureBootstrap futureBootstrap = bootstrapBuilder.start();
								futureBootstrap.addListener(new BaseFutureAdapter<FutureBootstrap>() {
									@Override
									public void operationComplete(FutureBootstrap future) throws Exception {
										if (future.isSuccess()) {
											Shutdown shutdown = startRelayMaintenance(futureRelay, bootstrapBuilder, distributedRelay, relayConfig.peerMapUpdateInterval());
											futureRelayNAT.done(shutdown);
										} else {
											futureRelayNAT.failed("2nd FutureBootstrap failed", future);
										}
									}
								});
							} else {
								futureRelayNAT.failed("FutureRelay failed", future);
							}
						}
					});
				} else {
					futureRelayNAT.failed("FutureBootstrap failed", future);
				}
			}
		});
		return futureRelayNAT;
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
				setUpMessage.version(peer.connectionBean().p2pId());
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
		if (relayPeerAddress == null || unreachablePeerAddress == null) {
			throw new IllegalArgumentException("either the relay PeerAddress or the unreachablePeerAddress or both was/were null!");
		}
		
		// If we are already a relay of the unreachable peer, we shouldn't use a
		// reverse connection setup. It just doesn't make sense!
		if (peer.peerAddress().peerId().equals(relayPeerAddress.peerId())) {
			throw new IllegalStateException(
					"We are alredy a relay for the target peer. We shouldn't use a reverse connection to connect to the targeted peer!");
		}
	}
}
