package net.tomp2p.p2p;

import java.net.InetSocketAddress;
import java.util.Random;

//import org.junit.Test;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.Bindings.Protocol;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

/**
 * This class is not suitable for automated integration testing, since it
 * requires a setup with a IPv6, which has to be set up manually.
 * 
 * @author draft
 * 
 */
public class TestIPv6
{
	//@Test
	public void startServer() throws Exception
	{
		Random r = new Random(42L);
		Peer peer = new Peer(new Number160(r));
		Bindings b=new Bindings(Protocol.IPv6);
		peer.listen(4000, 4000, b);
		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			for (PeerAddress pa : peer.getPeerBean().getPeerMap().getAll()) {
				FutureChannelCreator fcc=peer.getConnectionBean().getConnectionReservation().reserve(1);
				fcc.awaitUninterruptibly();
				ChannelCreator cc = fcc.getChannelCreator();
				FutureResponse fr1 = peer.getHandshakeRPC().pingTCP(pa, cc);
				fr1.awaitUninterruptibly();
				
				if (fr1.isSuccess())
					System.out.println("peer online TCP:" + pa);
				else
					System.out.println("offline " + pa);
				FutureResponse fr2 = peer.getHandshakeRPC().pingUDP(pa, cc);
				fr2.awaitUninterruptibly();
				peer.getConnectionBean().getConnectionReservation().release(cc);
				if (fr2.isSuccess())
					System.out.println("peer online UDP:" + pa);
				else
					System.out.println("offline " + pa);

			}
			Thread.sleep(1500);
		}
	}
	
	//@Test
	public void startClient() throws Exception 
	{
		Random r = new Random(43L);
		Peer peer = new Peer(new Number160(r));
		Bindings b=new Bindings(Protocol.IPv6);
		peer.listen(4000, 4000, b);
		FutureBootstrap fb=peer.bootstrap(new InetSocketAddress("2001:620:10:10c1:201:6cff:feca:426d", 4000));
		fb.awaitUninterruptibly();
		System.out.println("Got it: "+fb.isSuccess());
		Thread.sleep(10000);
	}
}
