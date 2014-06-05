package net.tomp2p.examples;

import net.tomp2p.dht.FutureSend;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class ExampleSend {
	public static void main(String[] args) throws Exception {
		PeerDHT master = null;
		try {
			PeerDHT[] peers = ExampleUtils.createAndAttachPeersDHT(100, 4001);
			ExampleUtils.bootstrap(peers);
			master = peers[0];
			setupReplyHandler(peers);
			System.err.println(" ---- query 3 - ~6 close peers -----");
			exampleSendRedundant(peers[34]);
			System.err.println(" ---- now we want to query only the closest one -----");
			exampleSendOne(peers[14]);
			Thread.sleep(60000);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			master.shutdown();
		}
	}

	private static void exampleSendOne(PeerDHT peer) {
		RequestP2PConfiguration requestP2PConfiguration = new RequestP2PConfiguration(1, 10, 0);
		FutureSend futureSend = peer.send(Number160.createHash("key")).object("hello")
		        .requestP2PConfiguration(requestP2PConfiguration).start();
		futureSend.awaitUninterruptibly();
		for (Object object : futureSend.rawDirectData2().values()) {
			System.err.println("got:" + object);
		}
	}

	private static void exampleSendRedundant(PeerDHT peer) {
		FutureSend futureSend = peer.send(Number160.createHash("key")).object("hello").start();
		futureSend.awaitUninterruptibly();
		for (Object object : futureSend.rawDirectData2().values()) {
			System.err.println("got:" + object);
		}

	}

	private static void setupReplyHandler(PeerDHT[] peers) {
		for (final PeerDHT peer : peers) {
			peer.peer().objectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					System.err.println("I'm " + peer.peerID() + " and I just got the message [" + request
					        + "] from " + sender.peerId());
					return "world";
				}
			});
		}
	}
}
