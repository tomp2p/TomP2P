package net.tomp2p.stat;
import java.net.UnknownHostException;
import java.util.Random;

import junit.framework.Assert;
import net.tomp2p.Utils2;
import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMapKadImpl;

import org.junit.Test;

public class TestStat
{
	@Test
	public void testNodesEstimation1() throws UnknownHostException
	{
		Number160 id1 = new Number160("0x1");
		Number160 id2 = new Number160("0x3");
		Number160 id3 = new Number160("0x5");
		Number160 id4 = new Number160("0x7");
		Number160 id5 = new Number160("0x9");
		Number160 id6 = new Number160("0x11");
		Number160 id7 = new Number160("0x13");
		Number160 id8 = new Number160("0x15");
		Statistics statistics = new Statistics();
		PeerMapKadImpl kadRouting = new PeerMapKadImpl(id4, 20, 0, 0, 0, new int[0], statistics);
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
		System.err.println("We have 8 nodes, we esimate " + statistics.getEstimatedNumberOfNodes());
		Assert.assertEquals(8d, statistics.getEstimatedNumberOfNodes());
	}

	@Test
	public void testNodesEstimation2() throws UnknownHostException
	{
		for (int j = 1; j < 200; j++)
		{
			int maxNr = 1000 * j;
			Random rnd = new Random(42L);
			Number160 id = new Number160(rnd);
			Statistics statistics = new Statistics();
			PeerMapKadImpl kadRouting = new PeerMapKadImpl(id, 20, 0, 0, 0, new int[0], statistics);
			for (int i = 0; i < maxNr; i++)
			{
				Number160 id1 = new Number160(rnd);
				PeerAddress remoteNode1 = Utils2.createAddress(id1);
				kadRouting.peerOnline(remoteNode1, null);
			}
			double diff = (maxNr + 1) / statistics.getEstimatedNumberOfNodes();
			System.err.println("We have " + (maxNr + 1) + " nodes, we esimate "
					+ statistics.getEstimatedNumberOfNodes() + ", diff:" + diff);
			Assert.assertEquals(true, diff < 1.5 && diff > 0.5);
		}
	}
}
