package net.tomp2p.holep.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.connection.ChannelClient;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
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
		natBootstrap(master);

	}

	public UnreachablePeer(InetSocketAddress local, InetSocketAddress remote, boolean connected,
			Thread manualPunch, boolean isUnreachable) throws IOException {
		super(local);
		
		createPeer(isUnreachable);
		manualPunch.start();
		
		System.out.println("1 = connect");
		while (!connected) {
			int input = System.in.read();
			
			if (input == 1) {
				PeerSocketAddress peerSocketAddress = PeerSocketAddress.create(remote.getAddress(), remote.getPort(), 0, 0);
				PeerAddress recipientAddress = PeerAddress.create(unreachablePeerId2).withIPSocket(peerSocketAddress);
				FutureDirect direct = peer.sendDirect(recipientAddress).object(HELLO_WORLD).start();
				direct.awaitUninterruptibly();
				connected = true;
			}
		}
		
		
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

		// setup relay
		PeerNAT uNat = new PeerBuilderNAT(peer).start();
		FutureDone<ChannelClient> futureRelay = uNat.startParticularRelay(masterPeerAddress);

		futureRelay.awaitUninterruptibly();
		if (!futureRelay.isSuccess()) {
			LOG.error("Could not setup Relay!");
			System.exit(1);
		} else {
			ChannelClient cc = futureRelay.object();

			PeerAddress u2 = PeerAddress.builder().peerId(unreachablePeerId2).build();
				
			
			FutureDone<SctpChannelFacade> futureSctpChannel = new FutureDone<>();
			
			
			Message message = new Message().command(Commands.HOLEP.getNr()).type(Type.REQUEST_1)
					.sender(peer.peerAddress()).recipient(masterPeerAddress);

			cc.sendUDP(message);
		}
	}

}
