package net.tomp2p.dht;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class TestSinglePeer {
	public static void main(String[] args) throws IOException {
		Random rnd = new Random(1);
		Peer peer = new PeerBuilder( new Number160( rnd ) ).ports( 4000 ).start();
		PeerDHT peerDHT = new PeerBuilderDHT(peer).start();
		for(int i=0;true;i++) {
			peerDHT.put(Number160.ONE).data(new Data("test")).start().awaitUninterruptibly();
		}
        //System.out.println("peer up and running : " + peer.peerAddress());
	}
}
