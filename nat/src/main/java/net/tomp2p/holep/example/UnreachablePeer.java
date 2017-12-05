package net.tomp2p.holep.example;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.connection.SctpDefaultStreamConfig;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.SctpDataCallback;
import net.tomp2p.connection.ChannelClient;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.message.Message;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC.Commands;

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
		FutureBootstrap future = peer.bootstrap().inetAddress(local.getAddress()).ports(9899)
				.bootstrapTo(Arrays.asList(masterPeerAddress)).start();
		future.addListener(new BaseFutureListener<FutureBootstrap>() {

			@Override
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					LOG.debug("Bootstrap success!");

					// TODO jwa: add object data reply
					peer.sctpDataCallback(new SctpDataCallback() {

						@Override
						public void onSctpPacket(byte[] data, int sid, int ssn, int tsn, long ppid, int context,
								int flags, SctpChannelFacade facade) {
							System.out.println("I WAS HERE");
							System.out.println("got data: " + new String(data, StandardCharsets.UTF_8));
							facade.send(data, new SctpDefaultStreamConfig());
						}
					});

					natBootstrap(master);

					// PeerAddress u2 = PeerAddress.create(unreachablePeerId2);
					//
					// Message holePInitMessage = new Message();
					// holePInitMessage.command(Commands.HOLEP.getNr());
					// holePInitMessage.sender(peer.peerAddress());
					// holePInitMessage.recipient(u2);
					//
					// peer.sendDirect(masterPeerAddress).object(HELLO_WORLD);
					//
					// Message message = new Message();
					// message.recipient(masterPeerAddress);

				} else {
					LOG.error("Could not bootstrap masterpeer. Now closing program!");
					System.exit(1);
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				LOG.error("Could not bootstrap masterpeer. Now closing program!");
				System.exit(1);
			}
		});
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
		PeerAddress upa = peer.peerBean().serverPeerAddress();
		peer.peerBean().serverPeerAddress(upa);

		// find neighbors
		FutureBootstrap futureBootstrap = peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
		futureBootstrap.awaitUninterruptibly();

		// setup relay
		PeerNAT uNat = new PeerBuilderNAT(peer).start();
		// set up 3 relays
//		uNat.
//		FutureRelayNAT frn = uNat.startRelay(bootstrapPeerAddress);
//		frn.awaitUninterruptibly();

		// find neighbors again
//		FutureBootstrap fb = peer.bootstrap().peerAddress(bootstrapPeerAddress).start();
//		fb.awaitUninterruptibly();

		// do maintenance
		// uNat.bootstrapBuilder(peer.bootstrap().peerAddress(bootstrapPeerAddress));
		// uNat.startRelayMaintenance(futureRelay);
	}

}
