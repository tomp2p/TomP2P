package net.tomp2p.p2p;

import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

//import org.junit.Test;

/**
 * This class is not suitable for automated integration testing, since it
 * requires a setup with a NAT in between, which has to be set up manually.
 * 
 * @author draft
 * 
 */
public class TestNAT {
	//@Test
	public void startServer() throws Exception {
		Random r = new Random(42L);
		Peer peer = new Peer(new Number160(r));
		peer.getP2PConfiguration().setBehindFirewall(true);
		peer.listen(4000, 4000);
		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			for (PeerAddress pa : peer.getPeerBean().getPeerMap().getAll()) {
				ChannelCreator cc=peer.getConnectionBean().getReservation().reserve(1);
				FutureResponse fr1 = peer.getHandshakeRPC().pingTCP(pa, cc);
				fr1.awaitUninterruptibly();
				if (fr1.isSuccess())
					System.out.println("peer online T:" + pa);
				else
					System.out.println("offline " + pa);
				FutureResponse fr2 = peer.getHandshakeRPC().pingUDP(pa, cc);
				fr2.awaitUninterruptibly();
				cc.release();
				if (fr2.isSuccess())
					System.out.println("peer online U:" + pa);
				else
					System.out.println("offline " + pa);

			}
			Thread.sleep(1500);
		}

		peer.shutdown();
	}

	//@Test
	public void startClient() throws Exception {
		Random r = new Random(43L);
		Peer peer = new Peer(new Number160(r));
		peer.getP2PConfiguration().setBehindFirewall(true);
		peer.listen(4000, 4000);
		PeerAddress pa = new PeerAddress(Number160.ZERO,
				InetAddress.getByName("130.60.156.187"), 4000, 4000);
		FutureDiscover fd = peer.discover(pa);
		fd.awaitUninterruptibly();
		if (fd.isSuccess()) {
			System.out.println("found that my outside address is "
					+ fd.getPeerAddress());
		} else {
			System.out.println("failed " + fd.getFailedReason());
		}
		Thread.sleep(Integer.MAX_VALUE);
		peer.shutdown();
	}
}