package net.tomp2p.examples;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class ExampleDiscover {

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			startClient(args[0]);
		} else {
			startServer();
		}
	}

	public static void startServer() throws Exception {
		Random rnd = new Random(43L);
		Bindings b = new Bindings().listenAny();
		Peer master = new PeerBuilder(new Number160(rnd)).ports(4000).bindings(b).start();
		System.out.println("Server started Listening to: " + DiscoverNetworks.discoverInterfaces(b));
		System.out.println("address visible to outside is " + master.peerAddress());
		while (true) {
			for (PeerAddress pa : master.peerBean().peerMap().all()) {
				System.out.println("PeerAddress: " + pa);
				FutureChannelCreator fcc = master.connectionBean().reservation().create(1, 1);
				fcc.awaitUninterruptibly();

				ChannelCreator cc = fcc.channelCreator();

				FutureResponse fr1 = master.pingRPC().pingTCP(pa, cc, new DefaultConnectionConfiguration());
				fr1.awaitUninterruptibly();

				if (fr1.isSuccess()) {
					System.out.println("peer online T:" + pa);
				} else {
					System.out.println("offline " + pa);
				}

				FutureResponse fr2 = master.pingRPC().pingUDP(pa, cc, new DefaultConnectionConfiguration());
				fr2.awaitUninterruptibly();

				cc.shutdown();

				if (fr2.isSuccess()) {
					System.out.println("peer online U:" + pa);
				} else {
					System.out.println("offline " + pa);
				}
			}
			Thread.sleep(1500);
		}
	}

	public static void startClient(String ipAddress) throws Exception {
		Random rnd = new Random(42L);
		Bindings b = new Bindings().listenAny();
		Peer client = new PeerBuilder(new Number160(rnd)).ports(4001).bindings(b).start();
		System.out.println("Client started and Listening to: " + DiscoverNetworks.discoverInterfaces(b));
		System.out.println("address visible to outside is " + client.peerAddress());

		InetAddress address = Inet4Address.getByName(ipAddress);
		int masterPort = 4000;
		PeerAddress pa = PeerAddress.create(Number160.ZERO, address, masterPort, masterPort, masterPort +1 );

		System.out.println("PeerAddress: " + pa);
		
		// Future Discover
		FutureDiscover futureDiscover = client.discover().expectManualForwarding().inetAddress(address).ports(masterPort).start();
		futureDiscover.awaitUninterruptibly();

		// Future Bootstrap - slave
		FutureBootstrap futureBootstrap = client.bootstrap().inetAddress(address).ports(masterPort).start();
		futureBootstrap.awaitUninterruptibly();

		Collection<PeerAddress> addressList = client.peerBean().peerMap().all();
		System.out.println(addressList.size());

		if (futureDiscover.isSuccess()) {
			System.out.println("found that my outside address is " + futureDiscover.peerAddress());
		} else {
			System.out.println("failed " + futureDiscover.failedReason());
		}
		client.shutdown();
	}

}