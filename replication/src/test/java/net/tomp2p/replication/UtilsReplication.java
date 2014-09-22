package net.tomp2p.replication;

import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;

public class UtilsReplication {
	
	public static PeerDHT[] createNodes(int nrOfPeers, Random rnd, int port, AutomaticFuture automaticFuture) throws Exception {
        if (nrOfPeers < 1) {
            throw new IllegalArgumentException("Cannot create less than 1 peer");
        }
        Bindings bindings = new Bindings().addInterface("lo");
        PeerDHT[] peers  = new PeerDHT[nrOfPeers];
        final Peer master;
        if (automaticFuture != null) {
        	Number160 peerId = new Number160(rnd);
        	PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
        	master = new PeerBuilder(peerId)
                    .ports(port)
                    .externalBindings(bindings).peerMap(peerMap).start().addAutomaticFuture(automaticFuture);
        	peers[0] = new PeerBuilderDHT(master).start(); 
            
        } else {
        	Number160 peerId = new Number160(rnd);
        	PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
        	master = new PeerBuilder(peerId).externalBindings(bindings)
                    .peerMap(peerMap).ports(port).start();
        	peers[0] = new PeerBuilderDHT(master).start(); 
        }
        
        IndirectReplication i1 = new IndirectReplication(peers[0])
		.intervalMillis(1000)
		.nRoot()
		.replicationFactor(6)
		.keepData(false).start();

        for (int i = 1; i < nrOfPeers; i++) {
            if (automaticFuture != null) {
            	Number160 peerId = new Number160(rnd);
            	PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId));
                Peer peer = new PeerBuilder(peerId)
                        .masterPeer(master)
                        .peerMap(peerMap).externalBindings(bindings).start().addAutomaticFuture(automaticFuture);
                peers[i] = new PeerBuilderDHT(peer).start(); 
            } else {
            	Number160 peerId = new Number160(rnd);
            	PeerMap peerMap = new PeerMap(new PeerMapConfiguration(peerId).peerNoVerification());
            	Peer peer = new PeerBuilder(peerId)
                        .externalBindings(bindings).peerMap(peerMap).masterPeer(master)
                        .start();
                peers[i] = new PeerBuilderDHT(peer).start(); 
            }
            
            IndirectReplication i2 = new IndirectReplication(peers[i])
    		.intervalMillis(1000)
    		.nRoot()
    		.replicationFactor(6)
    		.keepData(false).start();
        }
        System.err.println("peers created.");
        return peers;
    }

	public static void perfectRouting(PeerDHT... peers) {
        for (int i = 0; i < peers.length; i++) {
            for (int j = 0; j < peers.length; j++)
                peers[i].peer().peerBean().peerMap().peerFound(peers[j].peer().peerAddress(), null, null);
        }
        System.err.println("perfect routing done.");
    }
}
