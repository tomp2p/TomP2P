package net.tomp2p.p2p;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
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
			tc = new TrackerConfiguration(1, 1, 0, 3);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			ft = nodes[301].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			Assert.assertEquals(0, ft.getPotentialTrackers().size());
			Assert.assertEquals(2, ft.getDirectTrackers().size());
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
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			Peer[] nodes = createNodes(master, 600);
			// perfect routing
			for (int i = 0; i < nodes.length; i++)
			{
				nodes[i].getPeerBean().getTrackerStorage()
						.setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
				for (int j = 0; j < nodes.length; j++)
					nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
			}
			RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
			//3 is good!
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 3, 0);
			Number160 trackerID = new Number160(rnd);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(new ShortString("test").toNumber160());
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			Set<Number160> tmp = new HashSet<Number160>();
			for (int i = 0; i <= 200; i++)
			{
				FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
				ft.awaitUninterruptibly();
				System.err.println("added " + nodes[300 + i].getPeerAddress().getID());
				tmp.add(nodes[300 + i].getPeerAddress().getID());
				Assert.assertEquals(true, ft.isSuccess());
				// Assert.assertEquals(true, ft.getDirectTrackers().size() == 2
				// || ft.getDirectTrackers().size() == 3);
			}
			tc = new TrackerConfiguration(1, 1, 10, 201);
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			ctg.setDomain(new ShortString("test").toNumber160());
			ctg.setRoutingConfiguration(rc);
			ctg.setTrackerConfiguration(tc);
			ctg.setEvaluationScheme(new VotingSchemeTracker());
			FutureTracker ft = nodes[299].getFromTracker(trackerID, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			for (PeerAddress pa : ft.getRawPeersOnTracker().keySet())
			{
				System.err.println("found: " + pa.getID());
				tmp.remove(pa.getID());
			}
			for (Number160 number160 : tmp)
			{
				System.err.println("not found: " + number160);
			}
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
			Assert.assertEquals(",.peoueuaoeue", ft.getRawPeersOnTracker().values().iterator().next().values()
					.iterator().next().getObject());
			Assert.assertEquals(",.peoueuaoeue", ft.getTrackers().values().iterator().next().getObject());
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

	@Test
	public void testTrackerReplication() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			master = new Peer(1, new Number160(rnd), CONFIGURATION);
			master.listen(4001, 4001);
			master.setDefaultTrackerReplication();
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			Number160 locationKey = new Number160(rnd);
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
			for (Map.Entry<BaseFuture, Long> entry : master.getPendingFutures().entrySet())
			{
				entry.getKey().awaitUninterruptibly();
				Assert.assertEquals(true, entry.getKey().isSuccess());
			}
			for (int i = 0; i < nodes.length; i++)
			{
				for (Map.Entry<BaseFuture, Long> entry : nodes[i].getPendingFutures().entrySet())
				{
					entry.getKey().awaitUninterruptibly();
					Assert.assertEquals(true, entry.getKey().isSuccess());
				}
			}

			Number160 tmp = Number160.MAX_VALUE;
			Number160 id = Number160.MAX_VALUE;
			for (int i = 0; i < nodes.length; i++)
			{
				Number160 test = locationKey.xor(nodes[i].getPeerID());
				if (test.compareTo(tmp) < 0)
				{
					tmp = test;
					id = nodes[i].getPeerID();
				}
			}
			ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
			FutureTracker ft = nodes[300].getFromTracker(locationKey, ctg);
			ft.awaitUninterruptibly();
			Assert.assertEquals(true, ft.isSuccess());
			boolean set = false;
			for (PeerAddress pa : ft.getDirectTrackers())
			{
				if (pa.getID().equals(id)) set = true;
			}
			Assert.assertEquals(true, set);
		}
		finally
		{
			master.shutdown();
		}
	}

	private Peer[] createNodes(Peer master, int nr) throws Exception
	{
		return createNodes(master, nr, false);
	}

	private Peer[] createNodes(Peer master, int nr, boolean replication) throws Exception
	{
		final Random rnd = new Random(42L);
		Peer[] nodes = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			nodes[i] = new Peer(1, new Number160(rnd), CONFIGURATION);
			nodes[i].listen(master);
			if (replication) nodes[i].setDefaultTrackerReplication();
		}
		return nodes;
	}

	//measurements
	public void simulation() throws Exception
	{
		int counter = 0;
		for (int askTrackers = 5; askTrackers <= 15; askTrackers++)
		{
			for (int uploadTo = 1; uploadTo <= 5; uploadTo++)
			{
				for (int repetition = 1; repetition <= 3; repetition++)
				{
					counter++;
					final Random rnd = new Random(42L);
					Peer master = null;
					try
					{
						master = new Peer(1, new Number160(rnd), CONFIGURATION);
						master.listen(4001, 4001);
						Peer[] nodes = createNodes(master, 600);
						// perfect routing
						for (int i = 0; i < nodes.length; i++)
						{
							nodes[i].getPeerBean().getTrackerStorage()
									.setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
							for (int j = 0; j < nodes.length; j++)
								nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
						}
						RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
						TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0);
						Number160 trackerID = new Number160(rnd);
						ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
						cts.setDomain(new ShortString("test").toNumber160());
						cts.setRoutingConfiguration(rc);
						cts.setTrackerConfiguration(tc);
						Set<Number160> tmp = new HashSet<Number160>();
						for (int i = 0; i <= 200; i++)
						{
							FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
							ft.awaitUninterruptibly();
							tmp.add(nodes[300 + i].getPeerAddress().getID());
							Assert.assertEquals(true, ft.isSuccess());
							// Assert.assertEquals(true,
							// ft.getDirectTrackers().size()
							// == 2
							// || ft.getDirectTrackers().size() == 3);
						}
						tc = new TrackerConfiguration(1, 1, askTrackers, 201);
						ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
						ctg.setDomain(new ShortString("test").toNumber160());
						ctg.setRoutingConfiguration(rc);
						ctg.setTrackerConfiguration(tc);
						ctg.setEvaluationScheme(new VotingSchemeTracker());
						for (int i = 0; i < repetition; i++)
						{
							FutureTracker ft = nodes[299].getFromTracker(trackerID, ctg);
							ft.awaitUninterruptibly();
							Assert.assertEquals(true, ft.isSuccess());
							for (PeerAddress pa : ft.getRawPeersOnTracker().keySet())
							{
								// System.err.println("found: " + pa.getID());
								tmp.remove(pa.getID());
							}
						}
						// System.err.println("not found: " + tmp.size()+"");
						System.err.println(">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition
								+ ";" + tmp.size());
						// Assert.assertEquals(201,
						// ft.getRawPeersOnTracker().size());

					}
					finally
					{
						master.shutdown();
					}
				}
			}
		}
	}

	//measurements
	public void simulationPrepare() throws IOException
	{
		// Open the file that is the first
		// command line parameter
		FileInputStream fstream = new FileInputStream("/home/draft/raw.txt");
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		// Read File Line By Line
		StringBuilder output1 = new StringBuilder();
		StringBuilder output2 = new StringBuilder();
		StringBuilder output3 = new StringBuilder();
		Map<Integer,Map<Integer, String>> matrix1=new TreeMap<Integer, Map<Integer,String>>();
		Map<Integer,Map<Integer, String>> matrix2=new TreeMap<Integer, Map<Integer,String>>();
		Map<Integer,Map<Integer, String>> matrix3=new TreeMap<Integer, Map<Integer,String>>();
		while ((strLine = br.readLine()) != null)
		{
			if (strLine.contains(">>>>"))
			{
				int start=strLine.indexOf(">>>>", 4)+4;
				int end=strLine.indexOf(";", start);
				String first=strLine.substring(start,end);
				//
				start=end+1;
				end=strLine.indexOf(";", start);
				String second=strLine.substring(start,end);
				//
				start=end+1;
				end=strLine.indexOf(";", start);
				String third=strLine.substring(start,end);
				//
				start=end+1;
				String fourth=strLine.substring(start);
				
				if(third.equals("1"))
				{
					output1.append(first).append(" ");
					output1.append(second).append(" ");
					output1.append(fourth).append("\n");
					putInMatrix(first, second, fourth, matrix1);
				}
				if(third.equals("2"))
				{
					output2.append(first).append(" ");
					output2.append(second).append(" ");
					output2.append(fourth).append("\n");
					putInMatrix(first, second, fourth, matrix2);
				}
				if(third.equals("3"))
				{
					output3.append(first).append(" ");
					output3.append(second).append(" ");
					output3.append(fourth).append("\n");
					putInMatrix(first, second, fourth, matrix3);
				}
			}

		}
		
		output1 = new StringBuilder();
		output2 = new StringBuilder();
		output3 = new StringBuilder();
		matri(output1, matrix1);
		matri(output2, matrix1);
		matri(output3, matrix3);
		
		// Close the input stream
		in.close();
		File file1 = new File("/home/draft/data1.txt");
		BufferedWriter writer1 = new BufferedWriter(new FileWriter(file1));
		writer1.write(output1.toString());
		writer1.close();
		
		File file2 = new File("/home/draft/data2.txt");
		BufferedWriter writer2 = new BufferedWriter(new FileWriter(file2));
		writer2.write(output2.toString());
		writer2.close();
		
		File file3 = new File("/home/draft/data3.txt");
		BufferedWriter writer3 = new BufferedWriter(new FileWriter(file3));
		writer3.write(output3.toString());
		writer3.close();

	}

	private void matri(StringBuilder output1, Map<Integer, Map<Integer, String>> matrix1)
	{
		for(Integer i1:matrix1.keySet())
		{
			Map<Integer, String> tmp=matrix1.get(i1);
			for(Integer i2:tmp.keySet())
			{
				String val=tmp.get(i2);
				output1.append(val).append(" ");
			}
			output1.append("\n");
		}
	}

	private void putInMatrix(String first, String second, String fourth, Map<Integer, Map<Integer, String>> matrix1)
	{
		int i1=Integer.parseInt(first);
		int i2=Integer.parseInt(second);
		Map<Integer, String> tmp;
		if(matrix1.containsKey(i1))
		{
			tmp=matrix1.get(i1);
		}
		else
		{
			tmp=new TreeMap<Integer, String>();
			matrix1.put(i1, tmp);
		}
		tmp.put(i2, fourth);
	}

}
