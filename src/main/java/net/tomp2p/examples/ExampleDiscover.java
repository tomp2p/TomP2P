package net.tomp2p.examples;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.Bindings.Protocol;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;


public class ExampleDiscover {
	
	
	
	public static void main(String[] args) throws Exception
	{
		//basicExample();
		if(args.length>0)
			startClient(args[0]);			
		else
			startServer();
		
	}	
	
	public static void startServer()
		throws Exception
	{
		Random rnd = new Random(43L);
		Bindings b = new Bindings(Protocol.IPv4, Inet4Address.getByName("127.0.0.1"), 4000, 4000);
		//b.addInterface("eth0");
		Peer master = new PeerMaker(new Number160(rnd)).setPorts(4000).setBindings(b).buildAndListen();
		System.out.println("Server started Listening to: " + DiscoverNetworks.discoverInterfaces(b));
		System.out.println("address visible to outside is " + master.getPeerAddress());
		while(true)
		{
			for (PeerAddress pa : master.getPeerBean().getPeerMap().getAll()) 
			{
				FutureChannelCreator fcc=master.getConnectionBean().getConnectionReservation().reserve(1);
				fcc.awaitUninterruptibly();
				
				ChannelCreator cc = fcc.getChannelCreator();
				
				//FutureResponse fr1 = master.getHandshakeRPC().pingTCP(pa, cc);
				//fr1.awaitUninterruptibly();
				
				//if (fr1.isSuccess())
				//	System.out.println("peer online T:" + pa);
				//else
				//	System.out.println("offline " + pa);
				
				//FutureResponse fr2 = master.getHandshakeRPC().pingUDP(pa, cc);
				//fr2.awaitUninterruptibly();
				
				master.getConnectionBean().getConnectionReservation().release(cc);
				
				//if (fr2.isSuccess())
				//	System.out.println("peer online U:" + pa);
				//else
				//	System.out.println("offline " + pa);
			}
			Thread.sleep(1500);
		}
	}
	
	public static void startClient(String ipAddress) throws Exception
	{
		Random rnd = new Random(42L);
		Bindings b = new Bindings(Protocol.IPv4, Inet4Address.getByName("127.0.0.1"), 4001, 4001);
		//b.addInterface("eth0");
		Peer client = new PeerMaker(new Number160(rnd)).setPorts(4001).setBindings(b).buildAndListen();
		System.out.println("Client started and Listening to: " + DiscoverNetworks.discoverInterfaces(b));
		System.out.println("address visible to outside is " + client.getPeerAddress());
		
		
		InetAddress address = Inet4Address.getByName(ipAddress);
		int masterPort = 4000;
		PeerAddress pa = new PeerAddress(Number160.ZERO, address, masterPort, masterPort);
		
		//Creates a connetion before we discover
		//client.createPeerConnection(pa, 10000);		
		
		
		//Future Discover
		FutureDiscover futureDiscover = client.discover().setInetAddress(address).setPorts(masterPort).build();
		futureDiscover.awaitUninterruptibly();
		
		//Future Bootstrap - slave
		FutureBootstrap futureBootstrap = client.bootstrap().setInetAddress(address).setPorts(masterPort).build();
		futureBootstrap.awaitUninterruptibly();
		
		Collection<PeerAddress> addressList =  client.getPeerBean().getPeerMap().getAll();
		System.out.println(addressList.size());
		
		
		if (futureDiscover.isSuccess()) 
		{
			System.out.println("found that my outside address is "
					+ futureDiscover.getPeerAddress());
		}
		else 
		{
			System.out.println("failed " + futureDiscover.getFailedReason());
		}
		client.shutdown();

		//Future Bootstrap - master		
//		futureBootstrap = master.bootstrap(masterPA);
//		futureBootstrap.awaitUninterruptibly();	
		
		
		
	}

}