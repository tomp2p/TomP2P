package net.tomp2p.p2p;

import java.util.Collection;
import java.util.Random;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Timings;

import org.junit.Assert;
import org.junit.Test;

public class TestMaintenance
{
	final private static Random rnd = new Random(42L);

	@Test
	public void testMaintenance1() throws Exception
	{
		Peer master = null;
		try
		{
			PeerMaker p = new PeerMaker(new Number160(rnd));
			master = p.setStartMaintenance(false).setPorts(4001).buildAndListen();
			Peer[] nodes = createNodes(master, 500);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				master.getPeerBean().getPeerMap().peerFound(nodes[i].getPeerAddress(), master.getPeerAddress());
				nodes[i].getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), master.getPeerAddress());
				for (int j = 0; j < nodes.length; j++)
				{
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), master.getPeerAddress());
				}
			}
			//
			Collection<PeerAddress> pas = master.getPeerBean().getPeerMap().peersForMaintenance();
			Assert.assertEquals(160 * p.getBagSize(), pas.size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testMaintenance2() throws Exception
	{
		Peer master = null;
		try
		{
			PeerMaker peerMaker = new PeerMaker(new Number160(rnd)).setPorts(4001);
			master = setTime(peerMaker, 0, 3).buildAndListen();
			Peer[] nodes = createNodes(master, 500, rnd, 0, 3);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				master.getPeerBean().getPeerMap().peerFound(nodes[i].getPeerAddress(), null);
				nodes[i].getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
				for (int j = 0; j < nodes.length; j++)
				{
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
				}
			}
			System.err.println("routing done "+Timings.currentTimeMillis());
			Collection<PeerAddress> pas = master.getPeerBean().getPeerMap().peersForMaintenance();
			//this needs to be empty because its within the 4 seconds, where we do our second maintenance loop.
			Assert.assertEquals(0, pas.size());
			Timings.sleep(3000);
			pas = master.getPeerBean().getPeerMap().peersForMaintenance();
			//after 4 seconds we get all the peers back. 160 * bagsize is the maximum capacity
			Assert.assertEquals(160 * peerMaker.getBagSize(), pas.size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testMaintenance3() throws Exception
	{
		Peer master = null;
		try
		{
			master = setTime(new PeerMaker(new Number160(rnd)).setPorts(4001)).buildAndListen();
			Peer[] nodes = createNodes(master, 4, rnd, 1, 1, 1, 1, 1, 1);
			
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				master.getPeerBean().getPeerMap().peerFound(nodes[i].getPeerAddress(), null);
				nodes[i].getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
				for (int j = 0; j < nodes.length; j++)
				{
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
				}
			}
			//
			Timings.sleep(3000);
			master.startMaintainance();
			nodes[0].startMaintainance();
			nodes[1].startMaintainance();
			nodes[2].startMaintainance();
			nodes[3].startMaintainance();
			Timings.sleep(10000);
			PeerAddress node3 = nodes[3].getPeerAddress();
			nodes[3].shutdown();
			System.err.println("node 3 shutdown");
			Timings.sleep(15000);

			Assert.assertEquals(false, master.getPeerBean().getPeerMap().contains(node3));
			Assert.assertEquals(false, nodes[0].getPeerBean().getPeerMap().contains(node3));
			Assert.assertEquals(false, nodes[1].getPeerBean().getPeerMap().contains(node3));
			Assert.assertEquals(false, nodes[2].getPeerBean().getPeerMap().contains(node3));
		}
		finally
		{
			System.err.println("Shutdown!!");
			master.shutdown();
		}
	}

	/*private void setTime(Peer peer, int... times)
	{
		peer.getConfiguration().setStartMaintenance(false);
		for(int i=0;i<times.length;i++)
		{
			peer.getConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[i] = times[i];
		}
	}*/
	
	private PeerMaker setTime(PeerMaker maker, int... times)
	{
		maker.setStartMaintenance(false);
		maker.setWaitingTimeBetweenNodeMaintenenceSeconds(new int[times.length]);
		for(int i=0;i<times.length;i++)
		{
			maker.getWaitingTimeBetweenNodeMaintenenceSeconds()[i] = times[i];
		}
		return maker;
	}
	
	private Peer[] createNodes(Peer master, int nr) throws Exception
	{
		return createNodes(master, nr, rnd);
	}
	
	private Peer[] createNodes(Peer master, int nr, Random rnd, int... times) throws Exception
	{
		Peer[] peers = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			peers[i] = setTime(new PeerMaker(new Number160(rnd)).setStartMaintenance(false).setMasterPeer(master), times).buildAndListen();
		}
		return peers;
	}

	private Peer[] createNodes(Peer master, int nr, Random rnd) throws Exception
	{
		Peer[] peers = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			peers[i] = new PeerMaker(new Number160(rnd)).setStartMaintenance(false).setMasterPeer(master).buildAndListen();
		}
		return peers;
	}
}
