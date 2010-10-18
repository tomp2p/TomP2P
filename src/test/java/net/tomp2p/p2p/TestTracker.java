package net.tomp2p.p2p;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Random;
import java.util.logging.LogManager;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.config.ConfigurationTrackerGet;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.ShortString;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;


public class TestTracker
{
	final private static Random rnd = new Random(42L);
	final private static ConnectionConfiguration CONFIGURATION = new ConnectionConfiguration();
	// final private static ConnectionConfiguration CONFIGURATION_DEBUG = new
	// ConnectionConfiguration(3000000, 1000000,
	// 5000, 5, false);
	static
	{
		CONFIGURATION.setIdleTCPMillis(3000000);
		CONFIGURATION.setIdleUDPMillis(3000000);
		try
		{
			LogManager.getLogManager().readConfiguration(
					new FileInputStream(
							"/home/draft/Private/workspace/TomP2P_v3/src/conf/tomp2plog.properties"));
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testTracker() throws Exception
	{
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
					nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
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
					nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
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
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
			tc = new TrackerConfiguration(1, 1, 0, 2);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			ft = nodes[301].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(0, ft.getPotentialTrackers().size());
			Assert.assertEquals(3, ft.getDirectTrackers().size());
			Assert.assertEquals(1, ft.getRawPeersOnTracker().size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker3() throws Exception
	{
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
					nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
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
			Assert.assertEquals(3, ft.getRawPeersOnTracker().size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker4() throws Exception
	{
		Peer master = null;
		try
		{
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 600);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				nodes[i].getPeerBean().getTrackerStorage().setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
			    for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
			Number160 trackerID = new Number160(rnd);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			for (int i = 0; i <= 200; i++)
			{
				FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
				ft.awaitUninterruptibly();
				Assert.assertEquals(true, ft.isSuccess());
				// Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
				// || ft.getDirectTrackers().size() == 3);
			}
			tc = new TrackerConfiguration(1, 1, 5, 201);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			FutureTracker ft = nodes[299].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(201, ft.getRawPeersOnTracker().size());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker5() throws Exception
	{
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
					nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 2, 0);
			Number160 trackerID = new Number160(rnd);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			cts.setAttachement(new Data(new String(",.peoueuaoeue")));
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
			Assert.assertEquals(",.peoueuaoeue", ft.getRawPeersOnTracker().values().iterator()
					.next().values().iterator().next().getObject());
			Assert.assertEquals(",.peoueuaoeue", ft.getTrackers().values().iterator().next()
					.getObject());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testTracker6() throws Exception
	{
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
	
	@Test
	public void testTrackerReplication() throws Exception
	{
		Peer master = null;
		try
		{
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			master.setDefaultTrackerReplication();
			ConfigurationTrackerStore cts=Configurations.defaultTrackerStoreConfiguration();
			Number160 locationKey=new Number160(rnd);
			master.addToTracker(locationKey, cts);
			Peer[] nodes = createNodes(master, 500, true);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				master.getPeerBean().getPeerMap().peerOnline(nodes[i].getPeerAddress(), null);
			}
			for (int i = 0; i < nodes.length; i++)
			{
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
			}
			// WAIT HERE
			for(Map.Entry<BaseFuture, Long> entry:master.getPendingFutures().entrySet())
			{
				entry.getKey().awaitUninterruptibly();
				Assert.assertEquals(true, entry.getKey().isSuccess());
			}
			for (int i = 0; i < nodes.length; i++)
			{
				for(Map.Entry<BaseFuture, Long> entry:nodes[i].getPendingFutures().entrySet())
				{
					entry.getKey().awaitUninterruptibly();
					Assert.assertEquals(true, entry.getKey().isSuccess());
				}
			}
			
			
			Number160 tmp=Number160.MAX_VALUE;
			Number160 id=Number160.MAX_VALUE;
			for (int i = 0; i < nodes.length; i++)
			{
				Number160 test=locationKey.xor(nodes[i].getPeerID());
				if(test.compareTo(tmp)<0)
				{
					tmp=test;
					id=nodes[i].getPeerID();
				}
			}
			ConfigurationTrackerGet ctg=Configurations.defaultTrackerGetConfiguration();
			FutureTracker ft=nodes[300].getFromTracker(locationKey, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			boolean set=false;
			for(PeerAddress pa:ft.getDirectTrackers())
			{
				if(pa.getID().equals(id))
					set=true;
			}
			Assert.assertEquals(true, set);
		}
		finally
		{
			master.shutdown();
		}
	}
	private Peer[] createNodes(Peer master, int nr ) throws Exception
	{
		return createNodes(master, nr, false);
	}

	private Peer[] createNodes(Peer master, int nr, boolean replication) throws Exception
	{
		Peer[] nodes = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			nodes[i] = new Peer(1, new Number160(rnd), CONFIGURATION);
			nodes[i].listen(master);
			if(replication)
				nodes[i].setDefaultTrackerReplication();
		}
		return nodes;
	}
}
