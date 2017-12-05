package net.tomp2p.holep.example;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFutureListener;
import net.sctp4nat.connection.SctpDefaultStreamConfig;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.SctpDataCallback;
import net.tomp2p.connection.ChannelClient;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Triple;
import net.tomp2p.utils.Utils;

public class UnreachablePeer extends AbstractPeer {

	private static final Logger LOG = LoggerFactory.getLogger(UnreachablePeer.class);

	protected Peer peer;
	protected PeerAddress masterPeerAddress;

	public UnreachablePeer(InetSocketAddress local, boolean isUnreachable1, InetSocketAddress master)
			throws IOException {
		super(local);

		createPeer(isUnreachable1);
		PeerAddress masterPeerAddress = PeerAddress.create(masterPeerId);

		LOG.debug("now starting bootstrap to masterPeer with peerAddress:" + masterPeerAddress.toString() + "...");
		natBootstrap(master);

	}

	private void createPeer(boolean isUnreachable1) throws IOException {
		LOG.debug("start creating unreachablePeer...");
		if (isUnreachable1 == true) {
			peer = new PeerBuilder(unreachablePeerId1).start();
		} else {
			peer = new PeerBuilder(unreachablePeerId2).start();
		}
		new PeerBuilderNAT(peer).start();
		LOG.debug("masterpeer created! with peerAddress:" + peer.peerAddress().toString());
	}

	private void natBootstrap(InetSocketAddress remoteAddress) throws UnknownHostException {
		PeerSocket4Address address = new PeerSocket4Address(IPv4.fromInet4Address(remoteAddress.getAddress()),
				remoteAddress.getPort());

		PeerAddress bootstrapPeerAddress = PeerAddress.builder().peerId(masterPeerId).ipv4Socket(address)
				.reachable4UDP(false).build();
		this.masterPeerAddress = bootstrapPeerAddress;

		// Set the isFirewalledUDP and isFirewalledTCP flags
		// PeerAddress upa = peer.peerBean().serverPeerAddress();
		// peer.peerBean().serverPeerAddress(upa);
		// peer.peerBean().serverPeerAddress(peer.peerAddress());

		peer.peerBean().serverPeerAddress(peer.peerAddress());

		// find neighbors
		FutureBootstrap futureBootstrap = peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
		futureBootstrap.awaitUninterruptibly();

		Message message = new Message().command(Commands.RELAY.getNr()).type(Type.REQUEST_1).sender(peer.peerAddress())
				.recipient(masterPeerAddress).version(1).sctp(true);

		// setup relay
		PeerNAT uNat = new PeerBuilderNAT(peer).start();
		FutureChannelCreator cc = peer.connectionBean().reservation().create(1)
				.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
					@Override
					public void operationComplete(final FutureChannelCreator future) throws Exception {
						if (future.isSuccess()) {
							LOG.error("Fire UDP to {}.", message.recipient());
							Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> p = future
									.channelCreator().sendUDP(message);
							Utils.addReleaseListener(future.channelCreator());

							p.first.addListener(new BaseFutureListener<FutureDone<Message>>() {

								@Override
								public void operationComplete(FutureDone<Message> future) throws Exception {
									if (future.isSuccess()) {
										Message response = future.object();
										LOG.debug("response message was {}", response.type().toString());
									}
								}

								@Override
								public void exceptionCaught(Throwable t) throws Exception {
									LOG.error("no response message");
									System.exit(1);
								}
							});

							p.second.addListener(new BaseFutureListener<FutureDone<SctpChannelFacade>>() {

								@Override
								public void operationComplete(FutureDone<SctpChannelFacade> future) throws Exception {
									if (future.isSuccess()) {
										LOG.debug(future.toString());
									}
								}

								@Override
								public void exceptionCaught(Throwable t) throws Exception {
									LOG.error("no sctp channel");
								}

							});
						} else {
							Utils.addReleaseListener(future.channelCreator());
							LOG.warn("handleResponse for REQUEST_1 failed (UDP) {}", future.failedReason());
						}
					}
				});

		cc.awaitUninterruptibly();
		if (!cc.isSuccess()) {
			LOG.error("Could not setup Relay!");
			System.exit(1);
		}

		// set up 3 relays
		// uNat.
		// FutureRelayNAT frn = uNat.startRelay(bootstrapPeerAddress);
		// frn.awaitUninterruptibly();

		// find neighbors again
		// FutureBootstrap fb =
		// peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
		// fb.awaitUninterruptibly();

		// do maintenance
		// uNat.bootstrapBuilder(peer.bootstrap().peerAddress(bootstrapPeerAddress));
		// uNat.startRelayMaintenance(futureRelay);
	}

}
