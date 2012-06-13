package net.tomp2p.p2p;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.tomp2p.futures.FutureTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.ShortString;
import net.tomp2p.storage.TrackerData;

import org.junit.Assert;
import org.junit.Test;

public class TestTracker
{

	final private static ConnectionConfiguration CONFIGURATION = new ConnectionConfiguration();
	//static
	//{
		//CONFIGURATION.setIdleTCPMillis(3000000);
		//CONFIGURATION.setIdleUDPMillis(3000000);
	//}

	@Test
	public void testTracker() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			Peer[] nodes = createNodes(master, 500, rnd);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(0, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 1, 0);
			Number160 trackerID = new Number160(rnd);
			System.err.println("about to store " + trackerID);
			// FutureTracker ft = nodes[300].addToTracker(trackerID, "test",
			// null, rc, tc);
			FutureTracker ft = nodes[300].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			tc = new TrackerConfiguration(1, 1, 0, 1);
			ft = nodes[301].getTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setEvaluatingScheme(new VotingSchemeTracker()).build();
			ft.awaitUninterruptibly();
			System.err.println(ft.getFailedReason());
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(1, ft.getRawPeersOnTracker().size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker2() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			Peer[] nodes = createNodes(master, 500, rnd);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0, 1000, 2);
			Number160 trackerID = new Number160(rnd);
			System.err.println("about to store " + trackerID);
			FutureTracker ft = nodes[300].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 1, 3, 1000, 2);
			ft = nodes[301].getTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setEvaluatingScheme(new VotingSchemeTracker()).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(1, ft.getRawPeersOnTracker().size());
			Assert.assertEquals(1, ft.getPotentialTrackers().size());
			Assert.assertEquals(1, ft.getDirectTrackers().size());

		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker2_5() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			Peer[] nodes = createNodes(master, 500, rnd);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0, 1000, 2);
			Number160 trackerID = new Number160(rnd);
			System.err.println("about to store " + trackerID);
			FutureTracker ft = nodes[300].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 2, 3, 1000, 2);
			ft = nodes[301].getTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setEvaluatingScheme(new VotingSchemeTracker()).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(1, ft.getRawPeersOnTracker().size());
			Assert.assertEquals(0, ft.getPotentialTrackers().size());
			Assert.assertEquals(2, ft.getDirectTrackers().size());

		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker3() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			Peer[] nodes = createNodes(master, 500, rnd);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
			Number160 trackerID = new Number160(rnd);
			System.err.println("about to store " + trackerID);
			FutureTracker ft = nodes[300].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
			System.err.println("add the peer to the tracker: " + nodes[300].getPeerAddress());
			ft.awaitUninterruptibly();
			ft = nodes[301].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
			System.err.println("add the peer to the tracker: " + nodes[301].getPeerAddress());
			ft.awaitUninterruptibly();
			ft = nodes[302].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
			System.err.println("add the peer to the tracker: " + nodes[302].getPeerAddress());
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 2, 2);
			ft = nodes[299].getTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setEvaluatingScheme(new VotingSchemeTracker()).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			// we return there 1 because we use bloomfilters to not return
			// already known peers
			Assert.assertEquals(1, ft.getRawPeersOnTracker().size());
			// but here we expect 3 peers, since 3 peers are on the tracker
			Assert.assertEquals(3, ft.getRawPeersOnTracker().values().iterator().next().size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker4() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			Peer[] nodes = createNodes(master, 1000, rnd);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				// nodes[i].getPeerBean().getTrackerStorage()
				// .setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			// 3 is good!
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 0, 20, 2);
			Number160 trackerID = new Number160(rnd);
			Set<Number160> tmp = new HashSet<Number160>();
			for (int i = 0; i <= 300; i++)
			{
				FutureTracker ft = nodes[300 + i].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
				ft.awaitUninterruptibly();
				System.err
						.println("added " + nodes[300 + i].getPeerAddress().getID() + " on " + ft.getDirectTrackers());
				tmp.add(nodes[300 + i].getPeerAddress().getID());
				Assert.assertEquals(true, ft.isSuccess());
				// Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
				// || ft.getDirectTrackers().size() == 3);
			}
			for (int i = 0; i <= 300; i++)
			{
				FutureTracker ft = nodes[600 - i].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).build();
				ft.awaitUninterruptibly();
				System.err
						.println("added " + nodes[300 + i].getPeerAddress().getID() + " on " + ft.getDirectTrackers());
				tmp.add(nodes[600 - i].getPeerAddress().getID());
				Assert.assertEquals(true, ft.isSuccess());
				// Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
				// || ft.getDirectTrackers().size() == 3);
			}
			for (int i = 0; i < nodes.length; i++)
			{
				boolean secondary = nodes[i].getPeerBean().getTrackerStorage()
						.isSecondaryTracker(trackerID, new ShortString("test").toNumber160());
				Map<Number160, TrackerData> tdr = nodes[i].getPeerBean().getTrackerStorage()
						.meshPeers(trackerID, new ShortString("test").toNumber160());
				Map<Number160, TrackerData> tdr2= nodes[i].getPeerBean().getTrackerStorage()
					.secondaryPeers(trackerID, new ShortString("test").toNumber160());

				if (tdr != null)
					System.err.println("size[" + i + "] (" + secondary + "): " + tdr.size()+"/"+tdr2.size());
				else
					System.err.println("size[" + i + "] (" + secondary + "): " + 0);
			}
			System.err.println("SEARCH>>");
			tc = new TrackerConfiguration(1, 1, 30, 301, 0, 20);
			FutureTracker ft1 = nodes[299].getTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).
					setTrackerConfiguration(tc).setEvaluatingScheme(new VotingSchemeTracker()).build();
			ft1.awaitUninterruptibly();
			Assert.assertEquals(true, ft1.isSuccess());
			for (TrackerData pa : ft1.getTrackers())
			{
				System.err.println("found on DHT1: " + pa.getPeerAddress().getID());
				tmp.remove(pa.getPeerAddress().getID());
			}
			//ctg.setUseSecondaryTrackers(true);
			FutureTracker ft2 = nodes[299].getTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).
					setTrackerConfiguration(tc).setEvaluatingScheme(new VotingSchemeTracker()).setKnownPeers(ft1.getKnownPeers()).build();
			ft2.awaitUninterruptibly();
			System.err.println("Reason: " + ft2.getFailedReason());
			Assert.assertEquals(true, ft2.isSuccess());
			for (TrackerData pa : ft2.getTrackers())
			{
				if (tmp.remove(pa.getPeerAddress().getID()))
					System.err.println("found on DHT2: " + pa.getPeerAddress().getID());
			}
			/*
			 * for (Number480 n480 :
			 * nodes[299].getPeerBean().getTrackerStorage().getKeys(new
			 * Number320(trackerID, ctg.getDomain()))) {
			 * System.err.println("found locally: " + n480);
			 * tmp.remove(n480.getContentKey()); }
			 */
			for (Number160 number160 : tmp)
			{
				System.err.println("not found: " + number160+ " out of 301");
			}
			System.err.println("not found: " + tmp.size());
			Assert.assertEquals(true, tmp.size() < 160);

		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker5() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			Peer[] nodes = createNodes(master, 500, rnd);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
			Number160 trackerID = new Number160(rnd);
			FutureTracker ft = nodes[300].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setAttachement(new String(",.peoueuaoeue").getBytes()).build();
			ft.awaitUninterruptibly();
			ft = nodes[301].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setAttachement(new String(",.peoueuaoeue").getBytes()).build();
			ft.awaitUninterruptibly();
			ft = nodes[302].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setAttachement(new String(",.peoueuaoeue").getBytes()).build();
			ft.awaitUninterruptibly();
			ft = nodes[303].addTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setAttachement(new String(",.peoueuaoeue").getBytes()).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 0, 1);
			ft = nodes[199].getTracker(trackerID).setDomainKey(new ShortString("test").toNumber160()).setRoutingConfiguration(rc).setTrackerConfiguration(tc).setExpectAttachement().setEvaluatingScheme(new VotingSchemeTracker()).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(1, ft.getRawPeersOnTracker().size());
			Assert.assertArrayEquals(",.peoueuaoeue".getBytes(), ft.getRawPeersOnTracker().values().iterator().next()
					.iterator().next().getAttachement());
			Assert.assertArrayEquals(",.peoueuaoeue".getBytes(), ft.getTrackers().iterator().next().getAttachement());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker6() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			Number160 key = new Number160(44);
			FutureTracker future = master.addTracker(key).setDomainKey(new ShortString("pub").toNumber160()).build();
			future.awaitUninterruptibly();
			Assert.assertTrue(future.isSuccess());
			//
			FutureTracker future2 = master.getTracker(key).setDomainKey(new ShortString("pub").toNumber160()).build();
			future2.awaitUninterruptibly();
			System.err.println(future2.getFailedReason());
			Assert.assertTrue(future2.isSuccess());
			Assert.assertEquals(future2.getPeersOnTracker().size(), 1);
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTrackerResponsibility() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			Number160 trackerID = new Number160(rnd);
			master = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setPorts(4001).buildAndListen();
			
			Peer[] nodes = createNodes(master, 500, rnd);
			FutureTracker futureTracker = nodes[0].addTracker(trackerID).build();
			futureTracker.awaitUninterruptibly();
			
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
				{
					if(i!=j) nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
				}
			}
			FutureTracker ft = nodes[30].getTracker(trackerID).build();
			ft.awaitUninterruptibly();
			Assert.assertEquals(1, ft.getTrackers().size());
		}
		finally
		{
			master.shutdown();
		}
	}

	private Peer[] createNodes(Peer master, int nr, Random rnd) throws Exception
	{

		Peer[] nodes = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			nodes[i] = new PeerMaker(new Number160(rnd)).setP2PId(1).setConfiguration(CONFIGURATION).setMasterPeer(master).buildAndListen();
		}
		return nodes;
	}
}