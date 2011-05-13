package net.tomp2p.p2p;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.config.ConfigurationTrackerGet;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.ShortString;
import net.tomp2p.rpc.TrackerDataResult;
import net.tomp2p.storage.TrackerData;

import org.junit.Assert;
import org.junit.Test;

public class TestTracker
{

	final private static ConnectionConfiguration CONFIGURATION = new ConnectionConfiguration();
	// final private static ConnectionConfiguration CONFIGURATION_DEBUG = new
	// ConnectionConfiguration(3000000, 1000000,
	// 5000, 5, false);
	static
	{
		CONFIGURATION.setIdleTCPMillis(3000000);
		CONFIGURATION.setIdleUDPMillis(3000000);
	}

	@Test
	public void testTracker() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 500);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(0, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 1, 0);
			Number160 trackerID = new Number160(rnd);
			// FutureTracker ft = nodes[300].addToTracker(trackerID, "test",
			// null, rc, tc);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			FutureTracker ft = nodes[300].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			tc = new TrackerConfiguration(1, 1, 0, 1);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			ft = nodes[301].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
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
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 500);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0, 1000, 2);
			Number160 trackerID = new Number160(rnd);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			FutureTracker ft = nodes[300].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 0, 3, 1000 ,2);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			ft = nodes[301].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(1, ft.getRawPeersOnTracker().size());
			Assert.assertEquals(2, ft.getPotentialTrackers().size());
			Assert.assertEquals(1, ft.getDirectTrackers().size());
			
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
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 500);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
			Number160 trackerID = new Number160(rnd);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			FutureTracker ft = nodes[300].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			ft = nodes[301].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			ft = nodes[302].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 0, 1);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			ft = nodes[299].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getRawPeersOnTracker().size());
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
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 1000);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				//nodes[i].getPeerBean().getTrackerStorage()
				//		.setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			//3 is good!
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 13, 0, 20, 2);
			Number160 trackerID = new Number160(rnd);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			Set<Number160> tmp = new HashSet<Number160>();
			for (int i = 0; i <= 300; i++)
			{
				FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
				ft.awaitUninterruptibly();
				System.err.println("added " + nodes[300 + i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
				tmp.add(nodes[300 + i].getPeerAddress().getID());
				Assert.assertEquals(true, ft.isSuccess());
				// Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
				// || ft.getDirectTrackers().size() == 3);
			}
			for (int i = 0; i <= 300; i++)
			{
				FutureTracker ft = nodes[600 - i].addToTracker(trackerID, cts);
				ft.awaitUninterruptibly();
				System.err.println("added " + nodes[300 + i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
				tmp.add(nodes[600 - i].getPeerAddress().getID());
				Assert.assertEquals(true, ft.isSuccess());
				// Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
				// || ft.getDirectTrackers().size() == 3);
			}
			for (int i = 0; i < nodes.length; i++)
			{
				boolean secondary=nodes[i].getPeerBean().getTrackerStorage().isSecondaryTracker(trackerID, cts.getDomain());
				
				TrackerDataResult tdr=nodes[i].getPeerBean().getTrackerStorage().get(new Number320(trackerID, cts.getDomain()));
				if(tdr!=null)
					System.err.println("size11 ("+secondary+"): "+tdr.size());
				else
					System.err.println("size11 ("+secondary+"): "+0);
			}
			System.err.println("SEARCH>>"); 
			tc = new TrackerConfiguration(1, 1, 20, 301, 0, 20);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			FutureTracker ft1 = nodes[299].getFromTracker(trackerID, ctg);
			ft1.awaitUninterruptibly();
			Assert.assertEquals(true, ft1.isSuccess());
			for (TrackerData pa : ft1.getTrackers())
			{
				System.err.println("found on DHT1: " + pa.getPeerAddress().getID());
				tmp.remove(pa.getPeerAddress().getID());
			}
			ctg.setUseSecondaryTrackers(true);
			FutureTracker ft2 = nodes[299].getFromTracker(trackerID, ctg, ft1.getKnownPeers());
			ft2.awaitUninterruptibly();
			System.err.println("Reason: "+ft2.getFailedReason());
			Assert.assertEquals(true, ft2.isSuccess());
			for (TrackerData pa : ft2.getTrackers())
			{
				if(tmp.remove(pa.getPeerAddress().getID()))
						System.err.println("found on DHT2: " + pa.getPeerAddress().getID());
			}
			/*for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage().getKeys(new Number320(trackerID, ctg.getDomain())))
			{
				System.err.println("found locally: " + n480);
				tmp.remove(n480.getContentKey());
			}*/
			for (Number160 number160 : tmp)
			{
				System.err.println("not found: " + number160);
			}
			System.err.println("not found: "+tmp.size());
			Assert.assertEquals(true, tmp.size()< 100);

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
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 500);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
			Number160 trackerID = new Number160(rnd);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			cts.setAttachement(new String(",.peoueuaoeue").getBytes());
			FutureTracker ft = nodes[300].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			ft = nodes[301].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			ft = nodes[302].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			ft = nodes[303].addToTracker(trackerID, cts);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 0, 1);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			ctg.setExpectAttachement(true);
			ft = nodes[199].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(4, ft.getRawPeersOnTracker().size());
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
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Number160 key = new Number160(44);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("pub").toNumber160());
			FutureTracker future = master.addToTracker(key, cts);
			future.awaitUninterruptibly();
			Assert.assertTrue(future.isSuccess());
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("pub").toNumber160());
			//
			FutureTracker future2 = master.getFromTracker(key, ctg);
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

	private Peer[] createNodes(Peer master, int nr) throws Exception
	{
		final Random rnd = new Random(42L);
		Peer[] nodes = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			nodes[i] = new Peer(1, new Number160(rnd), CONFIGURATION);
			nodes[i].listen(master);
			
		}
		return nodes;
	}
}
