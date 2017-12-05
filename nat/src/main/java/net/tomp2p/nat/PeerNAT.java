package net.tomp2p.nat;

import java.net.Inet4Address;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.connection.ChannelClient;
import net.tomp2p.connection.Ports;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.DiscoverBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Triple;
import net.tomp2p.utils.Utils;

public class PeerNAT {

	private static final Logger LOG = LoggerFactory.getLogger(PeerNAT.class);

	private final Peer peer;
	private final NATUtils natUtils;
	private final RelayRPC relayRPC;
	private final boolean manualPorts;
	private boolean relayMaintenance;
	private BootstrapBuilder bootstrapBuilder;
	private int peerMapUpdateIntervalSeconds;

	PeerNAT(Peer peer, NATUtils natUtils, RelayRPC relayRPC, boolean manualPorts, boolean relayMaintenance,
			BootstrapBuilder bootstrapBuilder, int peerMapUpdateIntervalSeconds) {
		this.peer = peer;
		this.natUtils = natUtils;
		this.relayRPC = relayRPC;
		this.manualPorts = manualPorts;
		this.relayMaintenance = relayMaintenance;
		this.bootstrapBuilder = bootstrapBuilder;
		this.peerMapUpdateIntervalSeconds = peerMapUpdateIntervalSeconds;
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

	public boolean isManualPorts() {
		return manualPorts;
	}

	public FutureNAT portForwarding(final FutureDiscover futureDiscover) {
		DiscoverBuilder builder = new DiscoverBuilder(peer);
		return portForwarding(futureDiscover, builder);
	}

	/**
	 * Setup UPNP or NATPMP port forwarding.
	 * 
	 * @param futureDiscover
	 *            The result of the discovery process. This information from the
	 *            discovery process is important to setup UPNP or NATPMP. If this
	 *            fails, then this future will also fail, and other means to connect
	 *            to the network needs to be found.
	 * @return The future object that tells you if you are reachable (success), if
	 *         UPNP or NATPMP could be setup and then you are reachable (success),
	 *         or if it failed.
	 */
	public FutureNAT portForwarding(final FutureDiscover futureDiscover, final DiscoverBuilder builder) {
		final FutureNAT futureNAT = new FutureNAT();
		futureDiscover.addListener(new BaseFutureAdapter<FutureDiscover>() {

			@Override
			public void operationComplete(FutureDiscover future) throws Exception {

				// set the peer that we contacted
				if (future.reporter() != null) {
					futureNAT.reporter(future.reporter());
				}

				if (future.isFailed() && future.isNat() && !manualPorts) {
					Ports externalPorts = setupPortforwarding(
							future.internalAddress().ipv4().toInetAddress().getHostAddress(),
							peer.connectionBean().channelServer().channelServerConfiguration().portsForwarding());
					if (externalPorts != null) {
						final PeerAddress serverAddressOrig = peer.peerBean().serverPeerAddress();
						final PeerAddress serverAddress = serverAddressOrig.withIpv4Socket(PeerSocket4Address.create(
								(Inet4Address) future.externalAddress().ipv4().toInetAddress(),
								externalPorts.udpPort()));

						// set the new address regardless wheter it will succeed
						// or not.
						// The discover will eventually check wheter the
						// announced ip matches the one that it sees.
						peer.peerBean().serverPeerAddress(serverAddress);

						// test with discover again
						builder.peerAddress(futureNAT.reporter()).start()
								.addListener(new BaseFutureAdapter<FutureDiscover>() {
									@Override
									public void operationComplete(FutureDiscover future) throws Exception {
										if (future.isSuccess()) {
											// UPNP or NAT-PMP was
											// successful, set flag
											PeerAddress newServerAddress = serverAddress.withReachable4UDP(true);
											peer.peerBean().serverPeerAddress(newServerAddress);
											futureNAT.done(future.peerAddress(), future.reporter());
										} else {
											// indicate relay
											PeerAddress pa = serverAddressOrig.withReachable4UDP(false);
											peer.peerBean().serverPeerAddress(pa);
											futureNAT.failed(future);
										}
									}
								});
					} else {
						// indicate relay
						PeerAddress pa = peer.peerBean().serverPeerAddress().withReachable4UDP(false);
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
	 * @return The new external ports if port forwarding seemed to be successful,
	 *         otherwise null
	 */
	public Ports setupPortforwarding(final String internalHost, Ports ports) {
		LOG.debug("setup UPNP for host {} and ports {}", internalHost, ports);
		// new random ports
		boolean success;

		try {
			success = natUtils.mapUPNP(internalHost, peer.peerAddress().ipv4Socket().udpPort(), ports.udpPort());
		} catch (Exception e) {
			LOG.error("cannot map UPNP", e);
			success = false;
		}

		if (!success) {
			LOG.warn("cannot find UPNP devices");
			try {
				success = natUtils.mapPMP(peer.peerAddress().ipv4Socket().udpPort(), ports.udpPort());
				if (!success) {
					LOG.warn("cannot find NAT-PMP devices");
				}
			} catch (NatPmpException e1) {
				LOG.warn("cannot find NAT-PMP devices ", e1);
			}
		}
		if (success) {
			return ports;
		}
		return null;
	}

	public FutureDone<ChannelClient> startParticularRelay(PeerAddress relayPeerAddress) {
		FutureDone<ChannelClient> futureDone = new FutureDone<>();
		Message message = new Message().command(Commands.RELAY.getNr()).type(Type.REQUEST_1).sender(peer.peerAddress())
				.recipient(relayPeerAddress).version(1).sctp(true);
		
		FutureChannelCreator fcc = peer.connectionBean().reservation().create(1)
				.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						if (future.isSuccess()) {
							LOG.error("Fire UDP to {}.", message.recipient());
							ChannelClient cc = future.channelCreator();

							// setup relaying
							Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> p = cc
									.sendUDP(message);
							Utils.addReleaseListener(future.channelCreator());

							//TODO jwa: clean up this
							p.first.addListener(new BaseFutureListener<FutureDone<Message>>() {

								@Override
								public void operationComplete(FutureDone<Message> future) throws Exception {
									if (future.isSuccess()) {
										Message response = future.object();
										LOG.error("response message was {}", response.type().toString());
									}
								}

								@Override
								public void exceptionCaught(Throwable t) throws Exception {
									LOG.error("no response message");
									System.exit(1);
								}
							});

						} else {
							Utils.addReleaseListener(future.channelCreator());
							LOG.warn("handleResponse for REQUEST_1 failed (UDP) {}", future.failedReason());
						}
					}
				});
		return futureDone;
	}
}
