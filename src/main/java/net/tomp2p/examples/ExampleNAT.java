package net.tomp2p.examples;

import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class ExampleNAT 
{
	public void startServer() throws Exception 
	{
		Peer peer = null;
		try 
		{
			Random r = new Random(42L);
			peer = new Peer(new Number160(r));
			//peer.getP2PConfiguration().setBehindFirewall(true);
			Bindings b = new Bindings();
			b.addInterface("eth0");
			peer.listen(4000, 4000, b);
			System.out.println("peer started.");
			for (;;) 
			{
				for (PeerAddress pa : peer.getPeerBean().getPeerMap().getAll()) 
				{
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
		}
		finally 
		{
			peer.shutdown();
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		if(args.length>0)
		{
			startClientNAT(args[0]);
		}
		else
		{
			ExampleNAT t = new ExampleNAT();
			t.startServer();
		}
	}
	
	public static void startClientNAT(String ip) throws Exception 
	{
		Random r = new Random(43L);
		Peer peer = new Peer(new Number160(r));
		peer.getP2PConfiguration().setBehindFirewall(true);
		peer.listen(4000, 4000);
		PeerAddress pa = new PeerAddress(Number160.ZERO,
				InetAddress.getByName(ip), 4000, 4000);
		FutureDiscover fd = peer.discover(pa);
		fd.awaitUninterruptibly();
		if (fd.isSuccess()) 
		{
			System.out.println("found that my outside address is "
					+ fd.getPeerAddress());
		} 
		else 
		{
			System.out.println("failed " + fd.getFailedReason());
		}
		peer.shutdown();
	}
}
