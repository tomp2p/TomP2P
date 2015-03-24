package net.tomp2p;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.Ports;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.utils.InteropRandom;

public class BenchmarkUtil {

	/**
	 * Creates peers for benchmarking. The first peer will be used as the master.
	 * This means that shutting down the master will shut down all other peers as well.
	 * 
	 * @param nrOfPeers Number of peers to create.
	 * @param rnd The random object used for peer ID creation.
	 * @param port The UDP and TCP port.
	 * @param maintenance Indicates whether maintenance should be enabled.
	 * @param timeout Indicates whether timeout should be enabled.
	 * @return
	 * @throws IOException
	 */
	public static Peer[] createNodes(int nrOfPeers, InteropRandom rnd, int port, boolean maintenance,
			boolean timeout) throws IOException {
		Peer[] peers = new Peer[nrOfPeers];

		Number160 masterId = createRandomId(rnd);
		PeerMap masterMap = new PeerMap(new PeerMapConfiguration(masterId));
		PeerBuilder pb = new PeerBuilder(masterId).ports(port).enableMaintenance(maintenance)
				.externalBindings(new Bindings()).peerMap(masterMap);
		if (!timeout) {
			pb.channelServerConfiguration(createInfiniteTimeoutChannelServerConfiguration(port));
		}
		peers[0] = pb.start();
		//System.out.printf("Created master peer: %s.\n", peers[0].peerID());

		for (int i = 1; i < nrOfPeers; i++) {
			peers[i] = createSlave(peers[0], rnd, maintenance, timeout);
		}
		return peers;
	}

	public static Peer createSlave(Peer master, InteropRandom rnd, boolean maintenance, boolean timeout)
			throws IOException {
		Number160 slaveId = createRandomId(rnd);
		PeerMap slaveMap = new PeerMap(new PeerMapConfiguration(slaveId).peerNoVerification());
		PeerBuilder pb = new PeerBuilder(slaveId).masterPeer(master).enableMaintenance(maintenance)
				.externalBindings(new Bindings()).peerMap(slaveMap);
		if (!timeout) {
			pb.channelServerConfiguration(createInfiniteTimeoutChannelServerConfiguration(Ports.DEFAULT_PORT));
		}
		Peer slave = pb.start();
		//System.out.printf("Created slave peer %s.\n", slave.peerID());
		return slave;
	}

	/**
	 * Creates and returns a ChannelServerConfiguration that has infinite values for all timeouts.
	 * 
	 * @param port
	 * @return
	 */
	public static ChannelServerConfiguration createInfiniteTimeoutChannelServerConfiguration(int port) {
		return PeerBuilder.createDefaultChannelServerConfiguration().idleTCPSeconds(0).idleUDPSeconds(0)
				.connectionTimeoutTCPMillis(0).ports(new Ports(port, port));
	}

	/*public static long startBenchmark(String argument) {
		warmupTimer();
		reclaimResources();
		System.out.printf("%s: Starting Benchmarking...\n", argument);
		return System.nanoTime();
	}

	public static double stopBenchmark(long start, String argument) {
		long stop = System.nanoTime();
		long nanos = stop - start;
		System.out.printf("%s: Stopped Benchmarking.\n", argument);
		System.out.printf("%s: %s ns | %s ms | %s s\n", argument, toNanos(nanos), toMillis(nanos),
				toSeconds(nanos));
		return toMillis(nanos);
	}*/

	public static void warmupTimer() {
		System.out.println("Timer warmup...");
		long anker = 0;
		for (int i = 0; i < 100; i++) {
			anker |= System.nanoTime();
		}
		ankerTrash(anker);
	}
	
	public static void reclaimResources() {
		// best-effort only
		System.out.println("Garbage Collection attempted...");
		System.out.println("Object Finalization attempted...");
		restoreJvm();
	}

	private static Number160 createRandomId(InteropRandom rnd) {
		int[] vals = new int[Number160.INT_ARRAY_SIZE];
		for (int i = 0; i < vals.length; i++) {
			vals[i] = rnd.nextInt(Integer.MAX_VALUE);
		}
		return new Number160(vals);
	}

	/**
	 * This helper method receives an "anker object" just to "throw it away".
	 * This allows such an object to be "used".
	 * @param anker
	 * @return
	 */
	public static Object ankerTrash(Object anker) {
		return anker;
	}
	
	// ----------------------------------------------------
	// Java Specific: Force GC / OF
	// ----------------------------------------------------

	/**
	 * Tries to resotre the JVM to as clean a state as possible.
	 * @param maxLoops
	 */
	private static void restoreJvm() {

		long memoryUsedPrev = memoryUsed();
		for (int i = 0; i < 100; i++) {
			Runtime.getRuntime().runFinalization();
			Runtime.getRuntime().gc();

			long memoryUsedNow = memoryUsed();
			if (ManagementFactory.getMemoryMXBean().getObjectPendingFinalizationCount() == 0
					&& memoryUsedNow >= memoryUsedPrev) {
				break;
			}
			else {
				memoryUsedPrev = memoryUsedNow;
			}
		}
	}

	/**
	 * Returns the amount of memory on the heap that is currently being used by the JVM.
	 * 
	 * @return
	 */
	private static long memoryUsed() {

		Runtime rt = Runtime.getRuntime();
		return rt.totalMemory() - rt.freeMemory();
	}
}
