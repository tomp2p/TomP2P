package net.tomp2p.stat;
import java.net.UnknownHostException;
import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMapKadImpl;
import net.tomp2p.utils.Utils;

import org.junit.Test;

public class TestStat
{
	@Test
	public void testNodesEstimation1() throws UnknownHostException
	{
		Number160 id1 = new Number160("1");
		Number160 id2 = new Number160("3");
		Number160 id3 = new Number160("5");
		Number160 id4 = new Number160("7");
		Number160 id5 = new Number160("9");
		Number160 id6 = new Number160("11");
		Number160 id7 = new Number160("13");
		Number160 id8 = new Number160("15");
		PeerMapKadImpl kadRouting = new PeerMapKadImpl(id4, 20, 0, 0, 0, new int[0]);
		PeerAddress remoteNode1 = Utils2.createAddress(id1);
		kadRouting.peerOnline(remoteNode1, null); // 
		PeerAddress remoteNode2 = Utils2.createAddress(id2);
		kadRouting.peerOnline(remoteNode2, null);
		PeerAddress remoteNode3 = Utils2.createAddress(id3);
		kadRouting.peerOnline(remoteNode3, null); // //
		PeerAddress remoteNode4 = Utils2.createAddress(id4); //
		kadRouting.peerOnline(remoteNode4, null);
		PeerAddress remoteNode5 = Utils2.createAddress(id5);
		kadRouting.peerOnline(remoteNode5, null);
		PeerAddress remoteNode6 = Utils2.createAddress(id6);
		kadRouting.peerOnline(remoteNode6, null);
		PeerAddress remoteNode7 = Utils2.createAddress(id7);
		kadRouting.peerOnline(remoteNode7, null);
		PeerAddress remoteNode8 = Utils2.createAddress(id8);
		kadRouting.peerOnline(remoteNode8, null);
		System.err.println("We have 8 nodes, we esimate " + kadRouting.expectedNumberOfNodes());
	}

	@Test
	public void testNodesEstimation2() throws UnknownHostException
	{
		int maxNr = 1000;
		Random rnd = new Random(42L);
		Number160 id = new Number160(rnd);
		PeerMapKadImpl kadRouting = new PeerMapKadImpl(id, 20, 0, 0, 0, new int[0]);
		for (int i = 0; i < maxNr; i++)
		{
			Number160 id1 = new Number160(rnd);
			PeerAddress remoteNode1 = Utils2.createAddress(id1);
			kadRouting.peerOnline(remoteNode1, null);
		}
		System.err.println("We have " + maxNr + " nodes, we esimate "
				+ kadRouting.expectedNumberOfNodes());
	}
}
