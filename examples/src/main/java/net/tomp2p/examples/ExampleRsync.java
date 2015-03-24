package net.tomp2p.examples;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.DataMap;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.synchronization.PeerSync;
import net.tomp2p.synchronization.SyncStat;

public class ExampleRsync {
	private static final Random RND = new Random(42L);

	public static void main(String[] args) throws IOException {

		PeerDHT master = null;
		final int nrPeers = 100;
		final int port = 4001;

		try {
			PeerDHT[] peers = ExampleUtils.createAndAttachPeersDHT(nrPeers,
					port);
			ExampleUtils.bootstrap(peers);
			master = peers[0];
			Number160 nr = new Number160(RND);
			exampleRsync(peers, nr);
		} finally {
			if (master != null) {
				master.shutdown();
			}
		}
	}

	private static void exampleRsync(PeerDHT[] peers, Number160 nr) {
		//100KB
		byte test[] = new byte[100000];
		final NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
		dataMap.put(new Number640(peers[1].peerID(), Number160.ZERO, Number160.ZERO, Number160.ZERO), new Data(test)); 
		
		//now it will be stored on peer1 and peer2
		FuturePut fp = peers[3].put(peers[1].peerID()).dataMap(dataMap).start().awaitUninterruptibly();
		System.out.println("stored on: " + fp.rawResult());
		
		PeerSync ps1 = new PeerSync(peers[1]);
		//need to create reply handlers for sync request
		new PeerSync(peers[2]);
		
		//change second byte
		test[1]=1;
		
		FutureDone<SyncStat> fd = ps1.synchronize(peers[2].peerAddress()).dataMap(new DataMap(dataMap)).start().awaitUninterruptibly();
		System.out.println(fd.object().toString());
		
		
	}
}
