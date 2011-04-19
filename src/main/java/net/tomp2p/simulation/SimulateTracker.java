package net.tomp2p.simulation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.DistributedTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.p2p.VotingSchemeTracker;
import net.tomp2p.p2p.config.ConfigurationTrackerGet;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.ShortString;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.Utils;

public class SimulateTracker
{
	final private static int NODE_NR = 301;
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
		// simulateTracker.simulationParametersPull(1, 15, 1, 10, 1, 3);
		// simulateTracker.simulationParametersPrimary(1, 15, 1, 10, 1, 3);
		// simulateTracker.simulationParametersPEX(1, 15, 1, 10, 1, 3);
		// simulateTracker.simulationParametersPullPex(1, 15, 1, 10, 1, 3);
		
		for(int j=50;j<=450;j+=200)
		{
			for(int i=1;i<5;i++)
			{
				simulateTracker.simulationParametersPEXChurn("pex-"+j+"-"+i, (int) (i*0.1*j), j);
				simulateTracker.simulationParametersPrimaryChurn("primary-"+j+"-"+i, (int) (i*0.1*j), j, false);
				simulateTracker.simulationParametersPullChurn("pull_bf-"+j+"-"+i, (int) (i*0.1*j), j, true);
				simulateTracker.simulationParametersPullChurn("pull-"+j+"-"+i, (int) (i*0.1*j), j, false);
			}
		}
	}

	private static Peer tmp;

	private Peer[] prepare(String name, Peer master, int swarmSize, Number160 masterId, Number160 trackerID,
			Number160 domain, boolean useSecondary, int maxPrimaryTrackers) throws Exception
	{
		master = new Peer(1, masterId, CONFIGURATION);
		tmp = master;
		master.listen(4001, 4001, new File("/tmp/log-" + name + ".txt.gz"));
		Peer[] nodes = createNodes(master, 1000);
		// perfect routing
		for (int i = 0; i < nodes.length; i++)
		{
			nodes[i].getPeerBean().getTrackerStorage().setFillPrimaryStorageFast(false);
			for (int j = 0; j < nodes.length; j++)
				nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			nodes[i].getPeerBean().getTrackerStorage().setPrimaryStorageOnly(!useSecondary);
		}
		DistributedTracker.tmpUseSecondary = useSecondary;

		RoutingConfiguration rc = new RoutingConfiguration(1, 1, 20, 1);
		TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 0, 20, maxPrimaryTrackers);
		ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();

		cts.setDomain(domain);
		cts.setRoutingConfiguration(rc);
		cts.setTrackerConfiguration(tc);
		Set<Number160> tmp = new HashSet<Number160>();
		List<FutureTracker> list = new ArrayList<FutureTracker>();
		for (int i = 0; i < swarmSize; i++)
		{
			FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
			list.add(ft);
			if (i == 0)
			{
				ft.awaitUninterruptibly();
				System.err.println("node 300 stored on " + ft.getDirectTrackers());
			}
			tmp.add(nodes[300 + i].getPeerAddress().getID());
		}
		for (FutureTracker ft : list)
		{
			ft.awaitUninterruptibly();
		}
		list.clear();
		for (int i = 0; i < swarmSize; i++)
		{
			FutureTracker ft = nodes[599 - i].addToTracker(trackerID, cts);
			list.add(ft);
			tmp.add(nodes[599 - i].getPeerAddress().getID());
		}
		// int k=0;
		for (FutureTracker ft : list)
		{
			ft.awaitUninterruptibly();
			// known.get(k).merge(ft.getKnownPeers());
			// k++;
		}
		list.clear();
		// now we are well announced.
		// make sure the nodes have enough trackers
		for (int i = 0; i < swarmSize; i++)
		{
			System.err.println("size1: " + nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
		}
		return nodes;
	}

	public void simulationParametersPEXChurn(String name, int offline, int swarmSize) throws Exception
	{
		Peer master = null;
		try
		{
			Peer[] nodes = prepare(name, master, swarmSize, masterID, trackerID, domain, false, 35);
			master = tmp;
			//get all 35 contacts
			List<FutureTracker> list = new ArrayList<FutureTracker>();
			for (int i = 0; i < nodes.length; i++)
			{
				nodes[i].getPeerBean().getTrackerStorage().setFillPrimaryStorageFast(true);
			}
			for (int i = 0; i < swarmSize; i++)
			{
				TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 35, -1, 20);
				RoutingConfiguration rc = new RoutingConfiguration(20, 1, 1);
				ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
				ctg.setDomain(domain);
				ctg.setRoutingConfiguration(rc);
				ctg.setTrackerConfiguration(tc);
				ctg.setUseSecondaryTrackers(true);
				Collection<Number160> tmp1 = nodes[300 + i].getPeerBean().getTrackerStorage()
						.knownPeers(trackerID, domain);
				FutureTracker ft = nodes[300 + i].getFromTrackerCreateBloomfilter2(trackerID, ctg, tmp1);
				list.add(ft);
			}
			for (FutureTracker ft : list)
				ft.awaitUninterruptibly();
			list.clear();
			for (int i = 0; i < swarmSize; i++)
			{
				System.err.println("size1x: " + nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
				nodes[300+i].getTracker().startExchange(trackerID, domain);
			}
			Utils.sleep(10000);
			// start measurements here!!!
			master.getConnectionHandler().customLoggerMessage("$$$$$$$$$$$$$-Measure from here, the thing above is making everything ready");
			goOffline(offline, nodes);
			goOnline(offline, master, nodes, 20);
			
			for (int i = 0; i < swarmSize; i++)
			{
				nodes[300+i].getTracker().startExchange(trackerID, domain);
			}
			boolean repeat=true;
			while(repeat)
			{
				repeat = false;
				for (int i = 0; i < swarmSize; i++)
				{
					int size = nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain);
					if (size < 35)
					{
						System.err.println("size2: "
								+ nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
						TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 35 - size, -1, 20);
						RoutingConfiguration rc = new RoutingConfiguration(20, 1, 1);
						ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
						ctg.setDomain(domain);
						ctg.setRoutingConfiguration(rc);
						ctg.setTrackerConfiguration(tc);
						ctg.setUseSecondaryTrackers(true);
						FutureTracker ft = nodes[300 + i].getFromTracker(trackerID, ctg, new EmptySet<Number160>());
						list.add(ft);
						repeat = true;
					}
				}
				for (FutureTracker ft : list)
					ft.awaitUninterruptibly();
				list.clear();
			}

			for (int i = 0; i < swarmSize; i++)
			{
				if (nodes[300 + i].isListening())
				{
					int size = nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain);
					if (size < 35)
					{
						System.err.println("size3: "
								+ nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
					}
				}
			}
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			master.shutdown();
		}
	}

	Number160 domain = new ShortString("test").toNumber160();
	Random rnd = new Random();
	Number160 trackerID = new Number160(rnd);
	Number160 masterID = new Number160(rnd);

	public void simulationParametersPrimaryChurn(String name, int offline, int swarmSize, boolean bf) throws Exception
	{
		Peer master = null;
		try
		{
			Peer[] nodes = prepare(name, master, swarmSize, masterID, trackerID, domain, false, 35);
			master = tmp;
			//get all 35 contacts
			for (int i = 0; i < nodes.length; i++)
			{
				nodes[i].getPeerBean().getTrackerStorage().setFillPrimaryStorageFast(true);
			}
			List<FutureTracker> list = new ArrayList<FutureTracker>();
			for (int i = 0; i < swarmSize; i++)
			{
				TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 35, -1, 20);
				RoutingConfiguration rc = new RoutingConfiguration(20, 1, 1);
				ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
				ctg.setDomain(domain);
				ctg.setRoutingConfiguration(rc);
				ctg.setTrackerConfiguration(tc);
				ctg.setUseSecondaryTrackers(true);
				Collection<Number160> tmp1 = nodes[300 + i].getPeerBean().getTrackerStorage()
						.knownPeers(trackerID, domain);
				FutureTracker ft = nodes[300 + i].getFromTrackerCreateBloomfilter2(trackerID, ctg, tmp1);
				list.add(ft);
			}
			for (FutureTracker ft : list)
				ft.awaitUninterruptibly();
			list.clear();

			// start measurements here!!!
			master.getConnectionHandler().customLoggerMessage("$$$$$$$$$$$$$-Measure from here, the thing above is making everything ready");
			goOffline(offline, nodes);
			goOnline(offline, master, nodes, 20);
			// now check if we should add a new neighbor
			boolean repeat=true;
			while(repeat)
			{
				repeat = false;
				for (int i = 0; i < swarmSize; i++)
				{
					int size = nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain);
					if (size < 35)
					{
						System.err.println("size2: "
								+ nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
						TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 35 - size, -1, 20);
						RoutingConfiguration rc = new RoutingConfiguration(20, 1, 1);
						ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
						ctg.setDomain(domain);
						ctg.setRoutingConfiguration(rc);
						ctg.setTrackerConfiguration(tc);
						ctg.setUseSecondaryTrackers(true);
						if (!bf)
						{
							FutureTracker ft = nodes[300 + i].getFromTracker(trackerID, ctg, new EmptySet<Number160>());
							list.add(ft);
						}
						else
						{
							Collection<Number160> tmp1 = nodes[300 + i].getPeerBean().getTrackerStorage()
								.knownPeers(trackerID, domain);
							FutureTracker ft = nodes[300 + i].getFromTrackerCreateBloomfilter2(trackerID, ctg, tmp1);
							list.add(ft);
						}
						repeat = true;
					}
				}
				for (FutureTracker ft : list)
				ft.awaitUninterruptibly();
				list.clear();
			}
			for (int i = 0; i < swarmSize; i++)
			{
				if (nodes[300 + i].isListening())
				{
					int size = nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain);
					if (size < 35)
					{
						System.err.println("size3: "
								+ nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
					}
				}
			}

		}
		finally
		{
			master.shutdown();
		}
	}

	public void simulationParametersPullChurn(String name, int offline, int swarmSize, boolean bf) throws Exception
	{
		Peer master = null;
		try
		{
			Peer[] nodes = prepare(name, master, swarmSize, masterID, trackerID, domain, true, 1);
			master = tmp;
			for (int i = 0; i < nodes.length; i++)
			{
				nodes[i].getPeerBean().getTrackerStorage().setFillPrimaryStorageFast(true);
			}
			List<FutureTracker> list = new ArrayList<FutureTracker>();
			for (int i = 0; i < swarmSize; i++)
			{
				TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 35, -1, 5);
				RoutingConfiguration rc = new RoutingConfiguration(20, 1, 1);
				ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
				ctg.setDomain(domain);
				ctg.setRoutingConfiguration(rc);
				ctg.setTrackerConfiguration(tc);
				ctg.setUseSecondaryTrackers(true);
				Collection<Number160> tmp1 = nodes[300 + i].getPeerBean().getTrackerStorage()
						.knownPeers(trackerID, domain);
				FutureTracker ft = nodes[300 + i].getFromTrackerCreateBloomfilter2(trackerID, ctg, tmp1);
				list.add(ft);
			}
			for (FutureTracker ft : list)
				ft.awaitUninterruptibly();
			list.clear();
			// start measurements here!!!
			master.getConnectionHandler().customLoggerMessage("$$$$$$$$$$$$$-Measure from here, the thing above is making everything ready");
			goOffline(offline, nodes);
			goOnline(offline, master, nodes, 1);
			// now check if we should add a new neighbor
			boolean repeat=true;
			while(repeat)
			{
				repeat=false;
				for (int i = 0; i < swarmSize; i++)
				{
					int size = nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain);
					if (size < 35)
					{
						System.err.println("size2: "
								+ nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
						TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 35 - size, -1, 20);
						//RoutingConfiguration rc = new RoutingConfiguration(20, 1, 1);
						ConfigurationTrackerGet ctg = Configurations.defaultTrackerGetConfiguration();
						ctg.setDomain(domain);
						ctg.setRoutingConfiguration(null);
						ctg.setTrackerConfiguration(tc);
						ctg.setUseSecondaryTrackers(true);
						if(bf)
						{
							Collection<Number160> tmp1 = nodes[300 + i].getPeerBean().getTrackerStorage()
								.knownPeers(trackerID, domain);
							FutureTracker ft = nodes[300 + i].getFromTrackerCreateBloomfilter2(trackerID, ctg, tmp1);
							list.add(ft);
						}
						else
						{
							FutureTracker ft = nodes[300 + i].getFromTracker(trackerID, ctg, new EmptySet<Number160>());
							list.add(ft);
						}
						repeat=true;
					}
				}
				//List<Set<Number160>> known = new ArrayList<Set<Number160>>();
				for (FutureTracker ft : list)
				{
					ft.awaitUninterruptibly();
					//known.add(ft.getKnownPeers());
				}
				list.clear();
			}
			for (int i = 0; i < swarmSize; i++)
			{
				if (nodes[300 + i].isListening())
				{
					int size = nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain);
					if (size < 35)
					{
						System.err.println("size3: "
								+ nodes[300 + i].getPeerBean().getTrackerStorage().size(trackerID, domain));
					}
				}
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	private void goOnline(int offline, Peer master, Peer[] nodes, int maxPrimaryTracker) throws Exception
	{
		for (int i = 0; i < offline; i++)
		{
			nodes[300 + i] = new Peer(1, new Number160(rnd), CONFIGURATION);
			nodes[300 + i].listen(master);
			nodes[300 + i].getPeerBean().getTrackerStorage().setFillPrimaryStorageFast(true);
			// perfect routing
			for (int j = 0; j < nodes.length; j++)
			{
				if(nodes[j].isListening())
					nodes[j].getPeerBean().getPeerMap().peerFound(nodes[300 + i].getPeerAddress(), null);
			}
			for (int j = 0; j < nodes.length; j++)
			{
				if(nodes[j].isListening())
					nodes[300 + i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(), null);
			}
			// online again
			RoutingConfiguration rc = new RoutingConfiguration(20, 1, 20, 1);
			TrackerConfiguration tc = new TrackerConfiguration(1, 1, 20, 0, 20, maxPrimaryTracker);
			ConfigurationTrackerStore cts = Configurations.defaultTrackerStoreConfiguration();
			cts.setDomain(domain);
			cts.setRoutingConfiguration(rc);
			cts.setTrackerConfiguration(tc);
			// nodes[300+i].getPeerBean().getTrackerStorage().setFillPrimaryStorageFast(true);
			nodes[300 + i].addToTracker(trackerID, cts).awaitUninterruptibly();
		}
	}
	
	private void goOffline(int offline, Peer[] nodes)
	{
		for (int i = 0; i < offline; i++)
		{
			PeerAddress pa = nodes[300 + i].getPeerAddress();
			nodes[300 + i].shutdown();
			for (int j = 0; j < nodes.length; j++)
			{
				if(nodes[j].isListening())
				{
					nodes[j].getPeerBean().getPeerMap().peerOffline(pa, true);
				}
			}
		}
	}

	// measurements
	/*
	 * public void simulationParametersPull(int minAskTrackers, int
	 * maxAskTrackers, int minUploadTo, int maxUploadTo, int minRepeat, int
	 * maxRepeat) throws Exception { StringBuilder sb = new StringBuilder(); int
	 * counter = 0; for (int askTrackers = minAskTrackers; askTrackers <=
	 * maxAskTrackers; askTrackers++) { for (int uploadTo = minUploadTo;
	 * uploadTo <= maxUploadTo; uploadTo++) {
	 * 
	 * final Random rnd = new Random(42L);
	 * 
	 * Peer master = null; try { master = new Peer(1, new Number160(rnd),
	 * CONFIGURATION); master.listen(4001, 4001, new File("/tmp/log-pull-" +
	 * askTrackers + "-" + uploadTo + ".txt")); Peer[] nodes =
	 * createNodes(master, 1000); // perfect routing for (int i = 0; i <
	 * nodes.length; i++) { // nodes[i].getPeerBean().getTrackerStorage() //
	 * .setTrackerStoreSize
	 * (nodes[i].getPeerBean().getTrackerStorage().getTrackerSize()); for (int j
	 * = 0; j < nodes.length; j++)
	 * nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(),
	 * null); }
	 * 
	 * RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
	 * TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 10,
	 * 2); Number160 trackerID = new Number160(rnd); ConfigurationTrackerStore
	 * cts = Configurations.defaultTrackerStoreConfiguration();
	 * cts.setDomain(new ShortString("test").toNumber160());
	 * cts.setRoutingConfiguration(rc); cts.setTrackerConfiguration(tc);
	 * Set<Number160> tmp = new HashSet<Number160>(); for (int i = 0; i <= 300;
	 * i++) { FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
	 * ft.awaitUninterruptibly(); tmp.add(nodes[300 +
	 * i].getPeerAddress().getID());
	 * 
	 * } for (int i = 0; i <= 300; i++) { FutureTracker ft = nodes[600 -
	 * i].addToTracker(trackerID, cts); ft.awaitUninterruptibly();
	 * tmp.add(nodes[600 - i].getPeerAddress().getID()); } tc = new
	 * TrackerConfiguration(1, 1, askTrackers, 301, -1, 5);
	 * ConfigurationTrackerGet ctg =
	 * Configurations.defaultTrackerGetConfiguration(); ctg.setDomain(new
	 * ShortString("test").toNumber160()); ctg.setRoutingConfiguration(rc);
	 * ctg.setTrackerConfiguration(tc); ctg.setEvaluationScheme(new
	 * VotingSchemeTracker()); SimpleBloomFilter<Number160> knownPeers = null;
	 * for (int repetition = minRepeat; repetition <= maxRepeat; repetition++) {
	 * counter++; FutureTracker ft; if (knownPeers != null)
	 * ctg.setUseSecondaryTrackers(true); if (knownPeers == null) ft =
	 * nodes[NODE_NR].getFromTracker(trackerID, ctg); else ft =
	 * nodes[NODE_NR].getFromTracker(trackerID, ctg, knownPeers);
	 * ft.awaitUninterruptibly(); knownPeers = ft.getKnownPeers(); for
	 * (PeerAddress pa : ft.getRawPeersOnTracker().keySet()) {
	 * tmp.remove(pa.getID()); } for (Number480 n480 :
	 * nodes[NODE_NR].getPeerBean().getTrackerStorage() .getKeys(new
	 * Number320(trackerID, ctg.getDomain()))) {
	 * tmp.remove(n480.getContentKey()); } for (Number480 n480 :
	 * nodes[NODE_NR].getPeerBean().getTrackerStorage() .getKeys(new
	 * Number320(trackerID, ctg.getDomain().xor(Number160.MAX_VALUE)))) {
	 * tmp.remove(n480.getContentKey()); } sb.append( ">>>>" + counter + ">>>>"
	 * + askTrackers + ";" + uploadTo + ";" + repetition + ";" + tmp.size() +
	 * ";").append("\n"); System.err.println(">>>>" + counter + ">>>>" +
	 * askTrackers + ";" + uploadTo + ";" + repetition + ";" + tmp.size() +
	 * ";"); master.getConnectionHandler().customLoggerMessage( ">>>>" + counter
	 * + ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";" +
	 * tmp.size() + ";"); } } finally { master.shutdown(); }
	 * 
	 * } } simulationPrepare(sb.toString(), "/tmp/data-pull"); }
	 * 
	 * // measurements public void simulationParametersPrimary(int
	 * minAskTrackers, int maxAskTrackers, int minUploadTo, int maxUploadTo, int
	 * minRepeat, int maxRepeat) throws Exception {
	 * DistributedTracker.tmpUseSecondary=true; StringBuilder sb = new
	 * StringBuilder(); int counter = 0; for (int askTrackers = minAskTrackers;
	 * askTrackers <= maxAskTrackers; askTrackers++) { for (int uploadTo =
	 * minUploadTo; uploadTo <= maxUploadTo; uploadTo++) {
	 * 
	 * final Random rnd = new Random(42L);
	 * 
	 * Peer master = null; try { master = new Peer(1, new Number160(rnd),
	 * CONFIGURATION); master.listen(4001, 4001, new File("/tmp/log-primary-" +
	 * askTrackers + "-" + uploadTo + ".txt")); Peer[] nodes =
	 * createNodes(master, 1000); // perfect routing for (int i = 0; i <
	 * nodes.length; i++) { // nodes[i].getPeerBean().getTrackerStorage() //
	 * .setTrackerStoreSize
	 * (nodes[i].getPeerBean().getTrackerStorage().getTrackerSize()); for (int j
	 * = 0; j < nodes.length; j++)
	 * nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(),
	 * null); nodes[i].getPeerBean().getTrackerStorage().setPrimanyFactor(5); }
	 * RoutingConfiguration rc = new RoutingConfiguration(1, 1, 30, 1);
	 * TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 30,
	 * 30); Number160 trackerID = new Number160(rnd); ConfigurationTrackerStore
	 * cts = Configurations.defaultTrackerStoreConfiguration();
	 * cts.setDomain(new ShortString("test").toNumber160());
	 * cts.setRoutingConfiguration(rc); cts.setTrackerConfiguration(tc);
	 * Set<Number160> tmp = new HashSet<Number160>(); for (int i = 0; i <= 300;
	 * i++) { FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
	 * ft.awaitUninterruptibly(); tmp.add(nodes[300 +
	 * i].getPeerAddress().getID()); //System.err.println("for node:"+nodes[300
	 * + i].getPeerAddress().getID()+" we stored on"+ft.getDirectTrackers()); }
	 * for (int i = 0; i <= 300; i++) { FutureTracker ft = nodes[600 -
	 * i].addToTracker(trackerID, cts); ft.awaitUninterruptibly(); //
	 * System.err.println("added " + nodes[300 + //
	 * i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
	 * tmp.add(nodes[600 - i].getPeerAddress().getID()); }
	 * 
	 * for (int i = 0; i < nodes.length; i++) { boolean secondary =
	 * nodes[i].getPeerBean().getTrackerStorage() .isSecondaryTracker(trackerID,
	 * cts.getDomain()); //System.err.println("size11 (" // + secondary // +
	 * "): " // + nodes[i].getPeerBean().getTrackerStorage() // .get(new
	 * Number320(trackerID, cts.getDomain())).size()); }
	 * 
	 * tc = new TrackerConfiguration(1, 1, askTrackers, 301, -1, 30);
	 * ConfigurationTrackerGet ctg =
	 * Configurations.defaultTrackerGetConfiguration(); ctg.setDomain(new
	 * ShortString("test").toNumber160()); ctg.setRoutingConfiguration(rc);
	 * ctg.setTrackerConfiguration(tc); ctg.setEvaluationScheme(new
	 * VotingSchemeTracker()); SimpleBloomFilter<Number160> knownPeers = null;
	 * for (int repetition = minRepeat; repetition <= maxRepeat; repetition++) {
	 * counter++; FutureTracker ft; //if (knownPeers == null) ft =
	 * nodes[NODE_NR].getFromTracker(trackerID, ctg); //else // ft =
	 * nodes[NODE_NR].getFromTracker(trackerID, ctg, knownPeers);
	 * ft.awaitUninterruptibly(); knownPeers = ft.getKnownPeers(); for
	 * (PeerAddress pa : ft.getRawPeersOnTracker().keySet()) {
	 * tmp.remove(pa.getID()); } for (Number480 n480 :
	 * nodes[NODE_NR].getPeerBean().getTrackerStorage() .getKeys(new
	 * Number320(trackerID, ctg.getDomain()))) {
	 * tmp.remove(n480.getContentKey()); } for (Number480 n480 :
	 * nodes[NODE_NR].getPeerBean().getTrackerStorage() .getKeys(new
	 * Number320(trackerID, ctg.getDomain().xor(Number160.MAX_VALUE)))) {
	 * tmp.remove(n480.getContentKey()); } //for(Number160 nr:tmp) //{ //
	 * System.err.println("not found: "+nr); //} sb.append( ">>>>" + counter +
	 * ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";" +
	 * tmp.size() + ";").append("\n"); System.err.println(">>>>" + counter +
	 * ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";" +
	 * tmp.size() + ";"); master.getConnectionHandler().customLoggerMessage(
	 * ">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";" +
	 * repetition + ";" + tmp.size() + ";"); } } finally { master.shutdown(); }
	 * 
	 * } } simulationPrepare(sb.toString(), "/tmp/data-primary");
	 * DistributedTracker.tmpUseSecondary=false; }
	 * 
	 * // measurements public void simulationParametersPEX(int minAskTrackers,
	 * int maxAskTrackers, int minUploadTo, int maxUploadTo, int minRepeat, int
	 * maxRepeat) throws Exception { DistributedTracker.tmpUseSecondary=true;
	 * StringBuilder sb = new StringBuilder(); int counter = 0;
	 * 
	 * for (int askTrackers = minAskTrackers; askTrackers <= maxAskTrackers;
	 * askTrackers++) { for (int uploadTo = minUploadTo; uploadTo <=
	 * maxUploadTo; uploadTo++) {
	 * 
	 * final Random rnd = new Random(42L);
	 * 
	 * Peer master = null; try { master = new Peer(1, new Number160(rnd),
	 * CONFIGURATION); master.listen(4001, 4001, new File("/tmp/log-pex-" +
	 * askTrackers + "-" + uploadTo + ".txt")); Peer[] nodes =
	 * createNodes(master, 1000); // perfect routing for (int i = 0; i <
	 * nodes.length; i++) { // nodes[i].getPeerBean().getTrackerStorage() //
	 * .setTrackerStoreSize
	 * (nodes[i].getPeerBean().getTrackerStorage().getTrackerSize()); for (int j
	 * = 0; j < nodes.length; j++)
	 * nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(),
	 * null); nodes[i].getPeerBean().getTrackerStorage().setPrimanyFactor(5); }
	 * System.err.println("Node NODE_NR is: " + nodes[NODE_NR].getPeerID());
	 * RoutingConfiguration rc = new RoutingConfiguration(1, 1, 30, 1);
	 * TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 30,
	 * 30); Number160 trackerID = new Number160(rnd); ConfigurationTrackerStore
	 * cts = Configurations.defaultTrackerStoreConfiguration();
	 * cts.setDomain(new ShortString("test").toNumber160());
	 * cts.setRoutingConfiguration(rc); cts.setTrackerConfiguration(tc);
	 * Set<Number160> tmp = new HashSet<Number160>(); for (int i = 0; i <= 300;
	 * i++) { FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
	 * ft.awaitUninterruptibly(); tmp.add(nodes[300 +
	 * i].getPeerAddress().getID());
	 * 
	 * } for (int i = 0; i <= 300; i++) { FutureTracker ft = nodes[600 -
	 * i].addToTracker(trackerID, cts); ft.awaitUninterruptibly(); //
	 * System.err.println("added " + nodes[300 + //
	 * i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
	 * tmp.add(nodes[600 - i].getPeerAddress().getID()); }
	 * 
	 * 
	 * 
	 * tc = new TrackerConfiguration(1, 1, askTrackers, 301, -1, 5);
	 * ConfigurationTrackerGet ctg =
	 * Configurations.defaultTrackerGetConfiguration(); ctg.setDomain(new
	 * ShortString("test").toNumber160()); ctg.setRoutingConfiguration(rc);
	 * ctg.setTrackerConfiguration(tc); ctg.setEvaluationScheme(new
	 * VotingSchemeTracker()); Number160 domain = new
	 * ShortString("test").toNumber160(); SimpleBloomFilter<Number160>
	 * knownPeers = null; for (int repetition = minRepeat; repetition <=
	 * maxRepeat; repetition++) { counter++; FutureTracker ft; if (repetition ==
	 * minRepeat) { for (int i = 0; i <= 300; i++) { nodes[300 +
	 * i].getFromTracker(trackerID, ctg).awaitUninterruptibly(); } ft =
	 * nodes[NODE_NR].getFromTracker(trackerID, ctg); ft.awaitUninterruptibly();
	 * // knownPeers = ft.getKnownPeers();
	 * 
	 * for (PeerAddress pa : ft.getRawPeersOnTracker().keySet()) {
	 * tmp.remove(pa.getID()); } }
	 * 
	 * for (Number480 n480 : nodes[NODE_NR].getPeerBean().getTrackerStorage()
	 * .getKeys(new Number320(trackerID, domain))) {
	 * tmp.remove(n480.getContentKey()); } for (Number480 n480 :
	 * nodes[NODE_NR].getPeerBean().getTrackerStorage() .getKeys(new
	 * Number320(trackerID, domain.xor(Number160.MAX_VALUE)))) {
	 * tmp.remove(n480.getContentKey()); } for (int i = 0; i <= 300; i++) {
	 * nodes[600 - i].getTracker().startExchange(trackerID, domain); }
	 * Utils.sleep(500); sb.append( ">>>>" + counter + ">>>>" + askTrackers +
	 * ";" + uploadTo + ";" + repetition + ";" + tmp.size() + ";").append("\n");
	 * System.err.println(">>>>" + counter + ">>>>" + askTrackers + ";" +
	 * uploadTo + ";" + repetition + ";" + tmp.size() + ";");
	 * master.getConnectionHandler().customLoggerMessage( ">>>>" + counter +
	 * ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";" +
	 * tmp.size() + ";"); } }
	 * 
	 * finally { master.shutdown(); }
	 * 
	 * } } simulationPrepare(sb.toString(), "/tmp/data-pex");
	 * DistributedTracker.tmpUseSecondary=false; }
	 * 
	 * // measurements public void simulationParametersPullPex(int
	 * minAskTrackers, int maxAskTrackers, int minUploadTo, int maxUploadTo, int
	 * minRepeat, int maxRepeat) throws Exception { StringBuilder sb = new
	 * StringBuilder(); int counter = 0;
	 * 
	 * for (int askTrackers = minAskTrackers; askTrackers <= maxAskTrackers;
	 * askTrackers++) { for (int uploadTo = minUploadTo; uploadTo <=
	 * maxUploadTo; uploadTo++) {
	 * 
	 * final Random rnd = new Random(42L);
	 * 
	 * Peer master = null; try { master = new Peer(1, new Number160(rnd),
	 * CONFIGURATION); master.listen(4001, 4001, new File("/tmp/log-pull-pex-" +
	 * askTrackers + "-" + uploadTo + ".txt")); Peer[] nodes =
	 * createNodes(master, 1000); // perfect routing for (int i = 0; i <
	 * nodes.length; i++) { // nodes[i].getPeerBean().getTrackerStorage() //
	 * .setTrackerStoreSize
	 * (nodes[i].getPeerBean().getTrackerStorage().getTrackerSize()); for (int j
	 * = 0; j < nodes.length; j++)
	 * nodes[i].getPeerBean().getPeerMap().peerFound(nodes[j].getPeerAddress(),
	 * null); }
	 * 
	 * RoutingConfiguration rc = new RoutingConfiguration(1, 1, 1);
	 * TrackerConfiguration tc = new TrackerConfiguration(1, 1, uploadTo, 0, 10,
	 * 2); Number160 trackerID = new Number160(rnd); ConfigurationTrackerStore
	 * cts = Configurations.defaultTrackerStoreConfiguration();
	 * cts.setDomain(new ShortString("test").toNumber160());
	 * cts.setRoutingConfiguration(rc); cts.setTrackerConfiguration(tc);
	 * Set<Number160> tmp = new HashSet<Number160>(); for (int i = 0; i <= 300;
	 * i++) { FutureTracker ft = nodes[300 + i].addToTracker(trackerID, cts);
	 * ft.awaitUninterruptibly(); tmp.add(nodes[300 +
	 * i].getPeerAddress().getID());
	 * 
	 * } for (int i = 0; i <= 300; i++) { FutureTracker ft = nodes[600 -
	 * i].addToTracker(trackerID, cts); ft.awaitUninterruptibly(); //
	 * System.err.println("added " + nodes[300 + //
	 * i].getPeerAddress().getID()+" on "+ft.getDirectTrackers());
	 * tmp.add(nodes[600 - i].getPeerAddress().getID()); } tc = new
	 * TrackerConfiguration(1, 1, askTrackers, 301, -1, 5);
	 * ConfigurationTrackerGet ctg =
	 * Configurations.defaultTrackerGetConfiguration(); ctg.setDomain(new
	 * ShortString("test").toNumber160()); ctg.setRoutingConfiguration(rc);
	 * ctg.setTrackerConfiguration(tc); ctg.setEvaluationScheme(new
	 * VotingSchemeTracker()); SimpleBloomFilter<Number160> knownPeers = null;
	 * for (int repetition = minRepeat; repetition <= maxRepeat; repetition++) {
	 * counter++; FutureTracker ft; if (knownPeers != null)
	 * ctg.setUseSecondaryTrackers(true); if (knownPeers == null) ft =
	 * nodes[NODE_NR].getFromTracker(trackerID, ctg); else ft =
	 * nodes[NODE_NR].getFromTracker(trackerID, ctg, knownPeers);
	 * ft.awaitUninterruptibly(); knownPeers = ft.getKnownPeers();
	 * 
	 * for (PeerAddress pa : ft.getRawPeersOnTracker().keySet()) {
	 * tmp.remove(pa.getID()); } for (int i = 0; i <= 300; i++) { nodes[600 -
	 * i].getTracker().startExchange(trackerID, ctg.getDomain()); }
	 * Utils.sleep(500); for (Number480 n480 :
	 * nodes[NODE_NR].getPeerBean().getTrackerStorage() .getKeys(new
	 * Number320(trackerID, ctg.getDomain()))) {
	 * tmp.remove(n480.getContentKey()); } for (Number480 n480 :
	 * nodes[NODE_NR].getPeerBean().getTrackerStorage() .getKeys(new
	 * Number320(trackerID, ctg.getDomain().xor(Number160.MAX_VALUE)))) {
	 * tmp.remove(n480.getContentKey()); }
	 * 
	 * sb.append( ">>>>" + counter + ">>>>" + askTrackers + ";" + uploadTo + ";"
	 * + repetition + ";" + tmp.size() + ";").append("\n");
	 * System.err.println(">>>>" + counter + ">>>>" + askTrackers + ";" +
	 * uploadTo + ";" + repetition + ";" + tmp.size() + ";");
	 * master.getConnectionHandler().customLoggerMessage( ">>>>" + counter +
	 * ">>>>" + askTrackers + ";" + uploadTo + ";" + repetition + ";" +
	 * tmp.size() + ";"); } } finally { master.shutdown(); }
	 * 
	 * } }
	 * 
	 * simulationPrepare(sb.toString(), "/tmp/data-pull-pex"); }
	 */

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
		final Random rnd = new Random(42L);
		Peer[] nodes = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			nodes[i] = new Peer(1, new Number160(rnd), CONFIGURATION);
			nodes[i].listen(master);
		}
		return nodes;
	}

	public static class EmptySet<E> implements Set<E>
	{

		@Override
		public int size()
		{
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean isEmpty()
		{
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public boolean contains(Object o)
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Iterator<E> iterator()
		{
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object[] toArray()
		{
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <T> T[] toArray(T[] a)
		{
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean add(E e)
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean remove(Object o)
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean containsAll(Collection<?> c)
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean addAll(Collection<? extends E> c)
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean retainAll(Collection<?> c)
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean removeAll(Collection<?> c)
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void clear()
		{
			// TODO Auto-generated method stub

		}

	}
}
