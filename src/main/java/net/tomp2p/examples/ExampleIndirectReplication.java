package net.tomp2p.examples;

import java.io.IOException;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class ExampleIndirectReplication
{
	public static void main(String[] args) throws Exception
	{
		exmpleIndirectReplication();
	}

	private static void exmpleIndirectReplication() throws IOException, InterruptedException
	{
		Peer peer1 = new PeerMaker(new Number160(1)).setPorts(4001).setEnableIndirectReplication(true).buildAndListen();
		Peer peer2 = new PeerMaker(new Number160(2)).setPorts(4002).setEnableIndirectReplication(true).buildAndListen();
		Peer peer3 = new PeerMaker(new Number160(4)).setPorts(4003).setEnableIndirectReplication(true).buildAndListen();
		Peer[] peers = new Peer[]{peer1, peer2, peer3};
		//
		FutureDHT futureDHT = peer1.put(new Number160(4)).setData(new Data("store on peer1")).build();
		futureDHT.awaitUninterruptibly();
		futureDHT = peer1.get(new Number160(4)).setDigest().build();
		futureDHT.awaitUninterruptibly();
		System.out.println("we found the data on "+futureDHT.getRawDigest().size()+" peers");
		// now peer1 gets to know peer2, transfer the data
		peer1.bootstrap().setPeerAddress(peer2.getPeerAddress()).build();
		peer1.bootstrap().setPeerAddress(peer3.getPeerAddress()).build();
		Thread.sleep(1000);
		futureDHT = peer1.get(new Number160(4)).setDigest().build();
		futureDHT.awaitUninterruptibly();
		System.out.println("we found the data on "+futureDHT.getRawDigest().size()+" peers");
		shutdown(peers);
	}
	
	private static void shutdown(Peer[] peers)
	{
		for(Peer peer:peers)
		{
			peer.shutdown();
		}
	}
}
