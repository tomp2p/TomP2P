package net.tomp2p.nat;


import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
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

	public PeerNAT(Peer peer) {
		this.peer = peer;
		this.natUtils = new NATUtils();
		this.relayRPC = new RelayRPC(peer);
		
		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().setDone();
			}
		});

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
	public FutureNAT setupPortforwarding(final FutureDiscover futureDiscover) {
		final FutureNAT futureNAT = new FutureNAT();
		futureDiscover.addListener(new BaseFutureAdapter<FutureDiscover>() {

			@Override
			public void operationComplete(FutureDiscover future) throws Exception {
				if (future.isFailed() && future.isNat()) {
					Ports externalPorts = setupPortforwarding(future.internalAddress().getHostAddress());
					if (externalPorts != null) {
						PeerAddress serverAddress = peer.getPeerBean().serverPeerAddress();
						serverAddress = serverAddress.changePorts(externalPorts.externalTCPPort(),
						        externalPorts.externalUDPPort());
						serverAddress = serverAddress.changeAddress(future.externalAddress());
						peer.getPeerBean().serverPeerAddress(serverAddress);
						// test with discover again
						DiscoverBuilder builder = new DiscoverBuilder(peer);
						builder.start().addListener(new BaseFutureAdapter<FutureDiscover>() {
							@Override
							public void operationComplete(FutureDiscover future) throws Exception {
								if (future.isSuccess()) {
									futureNAT.done(future.getPeerAddress(), future.getReporter());
								} else {
									// indicate relay
									PeerAddress pa = peer.getPeerBean().serverPeerAddress().changeFirewalledTCP(true)
									        .changeFirewalledUDP(true);
									peer.getPeerBean().serverPeerAddress(pa);
									futureNAT.setFailed(future);
								}
							}
						});
					} else {
						// indicate relay
						PeerAddress pa = peer.getPeerBean().serverPeerAddress().changeFirewalledTCP(true)
						        .changeFirewalledUDP(true);
						peer.getPeerBean().serverPeerAddress(pa);
						futureNAT.setFailed("could not setup NAT");
					}
				} else {
					LOG.info("nothing to do, you are reachable from outside");
					futureNAT.done(futureDiscover.getPeerAddress(), futureDiscover.getReporter());
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
			success = natUtils.mapUPNP(internalHost, peer.getPeerAddress().tcpPort(), peer.getPeerAddress().udpPort(),
			        ports.externalUDPPort(), ports.externalTCPPort());
		} catch (IOException e) {
			success = false;
		}

		if (!success) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("cannot find UPNP devices");
			}
			try {
				success = natUtils.mapPMP(peer.getPeerAddress().tcpPort(), peer.getPeerAddress().udpPort(),
				        ports.externalUDPPort(), ports.externalTCPPort());
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

	public FutureRelay setupRelay(FutureNAT futureNAT, final RelayConf relayConf) {
		final FutureRelay futureRelay = new FutureRelay(relayConf.minRelays());
		futureNAT.addListener(new BaseFutureAdapter<FutureNAT>() {

			@Override
			public void operationComplete(FutureNAT future) throws Exception {
				if (future.isSuccess()) {
					futureRelay.nothingTodo();
				} else {
					setupRelay(relayConf, futureRelay);
				}
			}
		});
		return futureRelay;
	}
	
	public FutureRelay setupRelay(final RelayConf relayConf) {
		final FutureRelay futureRelay = new FutureRelay(relayConf.minRelays());
		setupRelay(relayConf, futureRelay);
		return futureRelay;
	}

	private void setupRelay(final RelayConf relayBuilder, final FutureRelay relayConf) {
		final DistributedRelay distributedRelay = new DistributedRelay(peer, relayRPC,
		        relayBuilder.failedRelayWaitTime());
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
				distributedRelay.setupRelays(futureRelay2);
				peer.notifyAutomaticFutures(futureRelay2);
			}
		});
		distributedRelay.setupRelays(relayConf);
		relayConf.distributedRelay(distributedRelay);
	}

	public Shutdown relayMaintenance(final FutureRelay futureRelay, final RelayConf relayBuilder) {
		final PeerMapUpdateTask peerMapUpdateTask = new PeerMapUpdateTask(relayRPC, relayBuilder.bootstrapBuilder(),
		        futureRelay.distributedRelay());
		peer.getConnectionBean().timer()
		        .scheduleAtFixedRate(peerMapUpdateTask, 0, relayBuilder.peerMapUpdateInterval(), TimeUnit.SECONDS);
		
		final Shutdown shutdown = new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				peerMapUpdateTask.cancel();
				return new FutureDone<Void>().setDone();
			}
		};
		peer.addShutdownListener(shutdown);
		
		return new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				peerMapUpdateTask.cancel();
				peer.removeShutdownListener(shutdown);
				return new FutureDone<Void>().setDone();
			}
		};		
	}
}
