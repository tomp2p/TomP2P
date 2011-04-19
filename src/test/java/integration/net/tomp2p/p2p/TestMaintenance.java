package net.tomp2p.p2p;
import java.util.Collection;
import java.util.Random;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

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
			master = new Peer(new Number160(rnd));
			master.getP2PConfiguration().setStartMaintenance(false);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 500);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				master.getPeerBean().getPeerMap().peerFound(nodes[i].getPeerAddress(),
						master.getPeerAddress());
				nodes[i].getPeerBean().getPeerMap().peerFound(master.getPeerAddress(),
						master.getPeerAddress());
				for (int j = 0; j < nodes.length; j++)
				{
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(),
							master.getPeerAddress());
				}
			}
			//
			Collection<PeerAddress> pas = master.getPeerBean().getPeerMap().peersForMaintenance();
			Assert.assertEquals(160 * master.getP2PConfiguration().getBagSize(), pas.size());
			// master.startMaintainance(master.getPeerInfo().getPeerMap(),
			// master.getHandshakeRPC());
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
			master = new Peer(new Number160(rnd));
			master.getP2PConfiguration().setStartMaintenance(false);
			master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[0] = 0;
			master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[1] = 3;
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 500);
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
			Collection<PeerAddress> pas = master.getPeerBean().getPeerMap().peersForMaintenance();
			Assert.assertEquals(0, pas.size());
			Utils.sleep(3000);
			pas = master.getPeerBean().getPeerMap().peersForMaintenance();
			Assert.assertEquals(160 * master.getP2PConfiguration().getBagSize(), pas.size());
			// master.startMaintainance(master.getPeerInfo().getPeerMap(),
			// master.getHandshakeRPC());
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
			master = new Peer(new Number160(rnd));
			master.getP2PConfiguration().setStartMaintenance(false);
			master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[0] = 1;
			master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[1] = 3;
			master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[2] = 3;
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 500);
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
			Utils.sleep(3000);
			master.startMaintainance();
			Utils.sleep(10000);
		}
		finally
		{
			System.err.println("Shutdown!!");
			master.shutdown();
		}
	}

	@Test
	public void testMaintenance4() throws Exception
	{
		Peer master = null;
		try
		{
			master = new Peer(new Number160(rnd));
			setTime(master);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 4);
			setTime(nodes[0]);
			setTime(nodes[1]);
			setTime(nodes[2]);
			setTime(nodes[3]);
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
			Utils.sleep(3000);
			master.startMaintainance();
			nodes[0].startMaintainance();
			nodes[1].startMaintainance();
			nodes[2].startMaintainance();
			nodes[3].startMaintainance();
			Utils.sleep(10000);
			nodes[3].shutdown();
			Utils.sleep(15000);
			Assert.assertEquals(false, master.getPeerBean().getPeerMap().contains(
					nodes[3].getPeerAddress()));
			Assert.assertEquals(false, nodes[0].getPeerBean().getPeerMap().contains(
					nodes[3].getPeerAddress()));
			Assert.assertEquals(false, nodes[1].getPeerBean().getPeerMap().contains(
					nodes[3].getPeerAddress()));
			Assert.assertEquals(false, nodes[2].getPeerBean().getPeerMap().contains(
					nodes[3].getPeerAddress()));
		}
		finally
		{
			System.err.println("Shutdown!!");
			master.shutdown();
		}
	}

	private void setTime(Peer master)
	{
		master.getP2PConfiguration().setStartMaintenance(false);
		master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[0] = 1;
		master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[1] = 1;
		master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[2] = 1;
		master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[3] = 1;
		master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[4] = 1;
		master.getP2PConfiguration().getWaitingTimeBetweenNodeMaintenenceSeconds()[5] = 1;
	}

	private Peer[] createNodes(Peer master, int nr) throws Exception
	{
		Peer[] nodes = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			nodes[i] = new Peer(new Number160(rnd));
			nodes[i].getP2PConfiguration().setStartMaintenance(false);
			nodes[i].listen(master);
		}
		return nodes;
	}
}
