package net.tomp2p.examples;

import java.util.Date;

import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class ExampleBuffer {
	public static void main(String[] args) throws Exception {
		final Number160 idP1 = Number160.createHash("p1");
		final Number160 idP2 = Number160.createHash("p2");
		final Number160 idP3 = Number160.createHash("p3");
		PeerDHT relay = new PeerBuilderDHT(new PeerBuilder(idP1).ports(1234).start()).start();
		PeerDHT requester = new PeerBuilderDHT(new PeerBuilder(idP2).ports(1235).start()).start();
		ChannelServerConfiguration csc = PeerBuilder.createDefaultChannelServerConfiguration();
		csc.idleTCPSlowMillis(35 * 1000);
		PeerDHT slow = new PeerBuilderDHT(new PeerBuilder(idP3).channelServerConfiguration(csc).ports(1236).start()).start();
		
		PeerNAT pn1 = new PeerBuilderNAT(relay.peer()).bufferTimeoutSeconds(30).start();
		PeerNAT pn2 = new PeerBuilderNAT(requester.peer()).bufferTimeoutSeconds(30).start();
		
		PeerAddress pa = slow.peerBean().serverPeerAddress();
		pa = pa.changeFirewalledTCP(true).changeFirewalledUDP(true).changeSlow(true);
		slow.peerBean().serverPeerAddress(pa);
		
		// find neighbors
		FutureBootstrap futureBootstrap = slow.peer().bootstrap().peerAddress(relay.peerAddress()).start();
		futureBootstrap.awaitUninterruptibly();
				
		// setup relay
		PeerNAT pn3 = new PeerBuilderNAT(slow.peer()).peerMapUpdateIntervalSeconds(30).heartBeatMillis(30 * 1000).idleTCP(35 * 1000).start();
		pn3.startRelay(relay.peerAddress());
		
		slow.peer().objectDataReply(new ObjectDataReply() {

			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				System.err.println("received: " + (String) request);
				return "REPLY";
			}
		});
		
		System.err.println("RPC to ("+new Date()+") "+slow.peerAddress());
		Thread.sleep(3000);
		FutureDirect fd = requester.peer().sendDirect(slow.peerAddress()).object("REQUEST").start();
		fd.awaitUninterruptibly();
		System.err.println("GOT ("+new Date()+"): "+fd.object());
		
		relay.shutdown();
		requester.shutdown();
		slow.shutdown();
	}
}
