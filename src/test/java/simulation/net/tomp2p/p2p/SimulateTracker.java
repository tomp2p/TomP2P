package net.tomp2p.p2p;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.config.ConfigurationTrackerGet;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.ShortString;
import net.tomp2p.rpc.SimpleBloomFilter;

import org.junit.Assert;

public class SimulateTracker
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

	public static void main(String[] args) throws Exception
	{
		SimulateTracker simulateTracker = new SimulateTracker();
		// simulateTracker.simulationParametersPull();
		// simulateTracker.simulationParametersPrimary();
		simulateTracker.simulationParametersPEX();
		// simulateTracker.simulationParametersPullPex();
	}

	// measurements
	public void simulationParametersPull() throws Exception
	{
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (int askTrackers = 1; askTrackers <= 2; askTrackers++)
		{
			for (int uploadTo = 1; uploadTo <= 2; uploadTo++)
			{

				final Random rnd = new Random(42L);

				Peer master = null;
				try
				{
					master = new Peer(1, new Number160(rnd), CONFIGURATION);
					master.listen(4001, 4001, new File("/tmp/log-pull-" + askTrackers + "-" + uploadTo + ".txt"));
					Peer[] nodes = createNodes(master, 1000);
					// perfect routing
					for (int i = 0; i < nodes.length; i++)
					{
						// nodes[i].getPeerBean().getTrackerStorage()
						// .setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
						for (int j = 0; j < nodes.length; j++)
							nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
					}

					RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
					TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 10, 2);
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
						tmp.add(nodes[300 + i].getPeerAddress().getID());

					}
					for (int i = 0; i <= 300; i++)
					{
						FutureTracker ft = nodes[600 - i].addToTracker(trackerID, cts);
						ft.awaitUninterruptibly();
						// System.err.println("added " + nodes[300 +
						// i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
						tmp.add(nodes[600 - i].getPeerAddress().getID());
					}
					tc = new TrackerConfiguration(1, 1, askTrackers, 301, 20, 5);
					ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
					ctg.setDomain(new ShortString("test").toNumber160());
					ctg.setRoutingConfiguration(rc);
					ctg.setTrackerConfiguration(tc);
					ctg.setEvaluationScheme(new VotingSchemeTracker());
					SimpleBloomFilter<Number160> knownPeers = null;
					for (int repetition = 1; repetition <= 3; repetition++)
					{
						counter++;
						FutureTracker ft;
						if (knownPeers != null)
							ctg.setUseSecondaryTrackers(true);
						if (knownPeers == null)
							ft = nodes[299].getFromTracker(trackerID, ctg);
						else
							ft = nodes[299].getFromTracker(trackerID, ctg, knownPeers);
						ft.awaitUninterruptibly();
						knownPeers = ft.getKnownPeers();
						Assert.assertEquals(true, ft.isSuccess());
						for (PeerAddress pa : ft.getRawPeersOnTracker().keySet())
						{
							tmp.remove(pa.getID());
						}
						for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
								.getKeys(new Number320(trackerID, ctg.getDomain())))
						{
							tmp.remove(n480.getContentKey());
						}
						for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
								.getKeys(new Number320(trackerID, ctg.getDomain().xor(Number160.MAX_VALUE))))
						{
							tmp.remove(n480.getContentKey());
						}
						sb.append(
								">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";"
										+ tmp.size() + ";").append("\n");
						System.err.println(">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition
								+ ";" + tmp.size() + ";");
					}
				}
				finally
				{
					master.shutdown();
				}

			}
		}
		simulationPrepare(sb.toString(), "/tmp/data-pull");
	}

	// measurements
	public void simulationParametersPrimary() throws Exception
	{
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (int askTrackers = 1; askTrackers <= 2; askTrackers++)
		{
			for (int uploadTo = 1; uploadTo <= 2; uploadTo++)
			{

				final Random rnd = new Random(42L);

				Peer master = null;
				try
				{
					master = new Peer(1, new Number160(rnd), CONFIGURATION);
					master.listen(4001, 4001, new File("/tmp/log-primary-" + askTrackers + "-" + uploadTo + ".txt"));
					Peer[] nodes = createNodes(master, 1000);
					// perfect routing
					for (int i = 0; i < nodes.length; i++)
					{
						// nodes[i].getPeerBean().getTrackerStorage()
						// .setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
						for (int j = 0; j < nodes.length; j++)
							nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
						nodes[i].getPeerBean().getTrackerStorage().setPrimanyFactor(5);
					}

					RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
					TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 10, 30);
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
						tmp.add(nodes[300 + i].getPeerAddress().getID());

					}
					for (int i = 0; i <= 300; i++)
					{
						FutureTracker ft = nodes[600 - i].addToTracker(trackerID, cts);
						ft.awaitUninterruptibly();
						// System.err.println("added " + nodes[300 +
						// i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
						tmp.add(nodes[600 - i].getPeerAddress().getID());
					}

					/*
					 * for (int i = 0; i < nodes.length; i++) { boolean
					 * secondary=nodes[i].getPeerBean().getTrackerStorage().
					 * isSecondaryTracker(trackerID, cts.getDomain());
					 * System.err
					 * .println("size11 ("+secondary+"): "+nodes[i].getPeerBean
					 * ().getTrackerStorage().get(new Number320(trackerID,
					 * cts.getDomain())).size()); }
					 */

					tc = new TrackerConfiguration(1, 1, askTrackers, 301, 20, 30);
					ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
					ctg.setDomain(new ShortString("test").toNumber160());
					ctg.setRoutingConfiguration(rc);
					ctg.setTrackerConfiguration(tc);
					ctg.setEvaluationScheme(new VotingSchemeTracker());
					SimpleBloomFilter<Number160> knownPeers = null;
					for (int repetition = 1; repetition <= 3; repetition++)
					{
						counter++;
						FutureTracker ft;
						if (knownPeers == null)
							ft = nodes[299].getFromTracker(trackerID, ctg);
						else
							ft = nodes[299].getFromTracker(trackerID, ctg, knownPeers);
						ft.awaitUninterruptibly();
						knownPeers = ft.getKnownPeers();
						Assert.assertEquals(true, ft.isSuccess());
						for (PeerAddress pa : ft.getRawPeersOnTracker().keySet())
						{
							tmp.remove(pa.getID());
						}
						for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
								.getKeys(new Number320(trackerID, ctg.getDomain())))
						{
							tmp.remove(n480.getContentKey());
						}
						for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
								.getKeys(new Number320(trackerID, ctg.getDomain().xor(Number160.MAX_VALUE))))
						{
							tmp.remove(n480.getContentKey());
						}
						sb.append(
								">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";"
										+ tmp.size() + ";").append("\n");
						System.err.println(">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition
								+ ";" + tmp.size() + ";");
					}
				}
				finally
				{
					master.shutdown();
				}

			}
		}
		simulationPrepare(sb.toString(), "/tmp/data-primary");
	}

	// measurements
	public void simulationParametersPEX() throws Exception
	{
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (int askPex = 1; askPex <= 3; askPex++)
		{
			for (int askTrackers = 1; askTrackers <= 3; askTrackers++)
			{
				for (int uploadTo = 1; uploadTo <= 3; uploadTo++)
				{

					final Random rnd = new Random(42L);

					Peer master = null;
					try
					{
						master = new Peer(1, new Number160(rnd), CONFIGURATION);
						master.listen(4001, 4001, new File("/tmp/log-pex-" + askTrackers + "-" + uploadTo + ".txt"));
						Peer[] nodes = createNodes(master, 1000);
						// perfect routing
						for (int i = 0; i < nodes.length; i++)
						{
							// nodes[i].getPeerBean().getTrackerStorage()
							// .setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
							for (int j = 0; j < nodes.length; j++)
								nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
							nodes[i].getPeerBean().getTrackerStorage().setPrimanyFactor(5);
						}

						RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
						TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 10, 30);
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
							tmp.add(nodes[300 + i].getPeerAddress().getID());

						}
						for (int i = 0; i <= 300; i++)
						{
							FutureTracker ft = nodes[600 - i].addToTracker(trackerID, cts);
							ft.awaitUninterruptibly();
							// System.err.println("added " + nodes[300 +
							// i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
							tmp.add(nodes[600 - i].getPeerAddress().getID());
						}

						/*
						 * for (int i = 0; i < nodes.length; i++) { boolean
						 * secondary=nodes[i].getPeerBean().getTrackerStorage().
						 * isSecondaryTracker(trackerID, cts.getDomain());
						 * System
						 * .err.println("size11 ("+secondary+"): "+nodes[i]
						 * .getPeerBean().getTrackerStorage().get(new
						 * Number320(trackerID, cts.getDomain())).size()); }
						 */

						tc = new TrackerConfiguration(1, 1, askTrackers, 301, 20, 30);
						ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
						ctg.setDomain(new ShortString("test").toNumber160());
						ctg.setRoutingConfiguration(rc);
						ctg.setTrackerConfiguration(tc);
						ctg.setEvaluationScheme(new VotingSchemeTracker());
						SimpleBloomFilter<Number160> knownPeers = null;
						for (int repetition = 1; repetition <= 3; repetition++)
						{
							counter++;
							FutureTracker ft;
							if (knownPeers == null)
								ft = nodes[299].getFromTracker(trackerID, ctg);
							else
								ft = nodes[299].getFromTracker(trackerID, ctg, knownPeers);
							ft.awaitUninterruptibly();
							knownPeers = ft.getKnownPeers();		

							for (PeerAddress pa : ft.getRawPeersOnTracker().keySet())
							{
								tmp.remove(pa.getID());
							}
							for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
									.getKeys(new Number320(trackerID, ctg.getDomain())))
							{
								tmp.remove(n480.getContentKey());
							}
							for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
									.getKeys(new Number320(trackerID, ctg.getDomain().xor(Number160.MAX_VALUE))))
							{
								tmp.remove(n480.getContentKey());
							}

							for (int i = 0; i <= 300; i++)
							{
								nodes[600 - i].getTracker().startExchange(trackerID, ctg.getDomain(), askPex);
							}
							sb.append(
									">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";"
											+ tmp.size() + ";" + askPex).append("\n");
							System.err.println(">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";"
									+ repetition + ";" + tmp.size() + ";" + askPex);
						}
					}
					finally
					{
						master.shutdown();
					}

				}
			}
		}
		simulationPrepare(sb.toString(), "/tmp/data-pex");
	}

	// measurements
	public void simulationParametersPullPex() throws Exception
	{
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (int askPex = 1; askPex <= 2; askPex++)
		{
			for (int askTrackers = 1; askTrackers <= 2; askTrackers++)
			{
				for (int uploadTo = 1; uploadTo <= 2; uploadTo++)
				{

					final Random rnd = new Random(42L);

					Peer master = null;
					try
					{
						master = new Peer(1, new Number160(rnd), CONFIGURATION);
						master.listen(4001, 4001,
								new File("/tmp/log-pull-pex-" + askTrackers + "-" + uploadTo + ".txt"));
						Peer[] nodes = createNodes(master, 1000);
						// perfect routing
						for (int i = 0; i < nodes.length; i++)
						{
							// nodes[i].getPeerBean().getTrackerStorage()
							// .setTrackerStoreSize(nodes[i].getPeerBean().getTrackerStorage().getTrackerSize());
							for (int j = 0; j < nodes.length; j++)
								nodes[i].getPeerBean().getPeerMap().peerOnline(nodes[j].getPeerAddress(), null);
						}

						RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
						TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 10, 2);
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
							tmp.add(nodes[300 + i].getPeerAddress().getID());

						}
						for (int i = 0; i <= 300; i++)
						{
							FutureTracker ft = nodes[600 - i].addToTracker(trackerID, cts);
							ft.awaitUninterruptibly();
							// System.err.println("added " + nodes[300 +
							// i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
							tmp.add(nodes[600 - i].getPeerAddress().getID());
						}
						tc = new TrackerConfiguration(1, 1, askTrackers, 301, 20, 5);
						ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
						ctg.setDomain(new ShortString("test").toNumber160());
						ctg.setRoutingConfiguration(rc);
						ctg.setTrackerConfiguration(tc);
						ctg.setEvaluationScheme(new VotingSchemeTracker());
						SimpleBloomFilter<Number160> knownPeers = null;
						for (int repetition = 1; repetition <= 3; repetition++)
						{
							counter++;
							FutureTracker ft;
							if (knownPeers != null)
								ctg.setUseSecondaryTrackers(true);
							if (knownPeers == null)
								ft = nodes[299].getFromTracker(trackerID, ctg);
							else
								ft = nodes[299].getFromTracker(trackerID, ctg, knownPeers);
							ft.awaitUninterruptibly();
							knownPeers = ft.getKnownPeers();
							Assert.assertEquals(true, ft.isSuccess());
							for (PeerAddress pa : ft.getRawPeersOnTracker().keySet())
							{
								tmp.remove(pa.getID());
							}
							for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
									.getKeys(new Number320(trackerID, ctg.getDomain())))
							{
								tmp.remove(n480.getContentKey());
							}
							for (Number480 n480 : nodes[299].getPeerBean().getTrackerStorage()
									.getKeys(new Number320(trackerID, ctg.getDomain().xor(Number160.MAX_VALUE))))
							{
								tmp.remove(n480.getContentKey());
							}
							for (int i = 0; i <= 300; i++)
							{
								nodes[600 - i].getTracker().startExchange(trackerID, ctg.getDomain(), askPex);
							}
							sb.append(
									">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";"
											+ tmp.size() + ";" + askPex).append("\n");
							System.err.println(">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";"
									+ repetition + ";" + tmp.size() + ";" + askPex);
						}
					}
					finally
					{
						master.shutdown();
					}

				}
			}
		}
		simulationPrepare(sb.toString(), "/tmp/data-pull-pex");
	}

	// measurements
	public void simulationPrepare(String input, String file) throws IOException
	{
		// Open the file that is the first
		// command line parameter
		// FileInputStream fstream = new FileInputStream("/home/draft/raw.txt");
		// Get the object of DataInputStream
		// DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new StringReader(input));
		String strLine;
		// Read File Line By Line
		StringBuilder output1 = new StringBuilder();
		StringBuilder output2 = new StringBuilder();
		StringBuilder output3 = new StringBuilder();
		Map<Integer, Map<Integer, String>> matrix1 = new TreeMap<Integer, Map<Integer, String>>();
		Map<Integer, Map<Integer, String>> matrix2 = new TreeMap<Integer, Map<Integer, String>>();
		Map<Integer, Map<Integer, String>> matrix3 = new TreeMap<Integer, Map<Integer, String>>();
		while ((strLine = br.readLine()) != null)
		{
			if (strLine.contains(">>>>"))
			{
				int start = strLine.indexOf(">>>>", 4) + 4;
				int end = strLine.indexOf(";", start);
				String first = strLine.substring(start, end);
				//
				start = end + 1;
				end = strLine.indexOf(";", start);
				String second = strLine.substring(start, end);
				//
				start = end + 1;
				end = strLine.indexOf(";", start);
				String third = strLine.substring(start, end);
				//
				start = end + 1;
				end = strLine.indexOf(";", start);
				String fourth = strLine.substring(start, end);
				// unused
				start = end + 1;
				String fifth = strLine.substring(start);

				if (third.equals("1"))
				{
					output1.append(first).append(" ");
					output1.append(second).append(" ");
					output1.append(fourth).append("\n");
					putInMatrix(first, second, fourth, matrix1);
				}
				if (third.equals("2"))
				{
					output2.append(first).append(" ");
					output2.append(second).append(" ");
					output2.append(fourth).append("\n");
					putInMatrix(first, second, fourth, matrix2);
				}
				if (third.equals("3"))
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
		matri(output2, matrix2);
		matri(output3, matrix3);

		// Close the input stream
		// in.close();
		File file1 = new File(file + "1.txt");
		BufferedWriter writer1 = new BufferedWriter(new FileWriter(file1));
		writer1.write(output1.toString());
		writer1.close();

		File file2 = new File(file + "2.txt");
		BufferedWriter writer2 = new BufferedWriter(new FileWriter(file2));
		writer2.write(output2.toString());
		writer2.close();

		File file3 = new File(file + "3.txt");
		BufferedWriter writer3 = new BufferedWriter(new FileWriter(file3));
		writer3.write(output3.toString());
		writer3.close();

	}

	private void matri(StringBuilder output1, Map<Integer, Map<Integer, String>> matrix1)
	{
		for (Integer i1 : matrix1.keySet())
		{
			Map<Integer, String> tmp = matrix1.get(i1);
			for (Integer i2 : tmp.keySet())
			{
				String val = tmp.get(i2);
				output1.append(val).append(" ");
			}
			output1.append("\n");
		}
	}

	private void putInMatrix(String first, String second, String fourth, Map<Integer, Map<Integer, String>> matrix1)
	{
		int i1 = Integer.parseInt(first);
		int i2 = Integer.parseInt(second);
		Map<Integer, String> tmp;
		if (matrix1.containsKey(i1))
		{
			tmp = matrix1.get(i1);
		}
		else
		{
			tmp = new TreeMap<Integer, String>();
			matrix1.put(i1, tmp);
		}
		tmp.put(i2, fourth);
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
			if (replication)
				nodes[i].setDefaultTrackerReplication();
		}
		return nodes;
	}

}
