package net.tomp2p;

import java.io.IOException;

import net.tomp2p.connection.Bindings;
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
	 * @param nrOfPeers Number of peers to create.
	 * @param rnd The random object used for peer ID creation.
	 * @param port The UDP and TCP port.
	 * @param maintenance Indicates whether maintenance should be enabled.
	 * @return
	 * @throws IOException 
	 */
	public static Peer[] createNodes(int nrOfPeers, InteropRandom rnd, int port, boolean maintenance) throws IOException
    {
        Peer[] peers = new Peer[nrOfPeers];

        Number160 masterId = createRandomId(rnd);
        PeerMap masterMap = new PeerMap(new PeerMapConfiguration(masterId));
        peers[0] = new PeerBuilder(masterId)
            .ports(port)
            .enableMaintenance(maintenance)
            .externalBindings(new Bindings())
            .peerMap(masterMap)
            .start();
        System.out.printf("Created master peer: %s.\n", peers[0].peerID());

        for (int i = 1; i < nrOfPeers; i++)
        {
            peers[i] = createSlave(peers[0], rnd, maintenance);
        }
        return peers;
    }

    public static Peer createSlave(Peer master, InteropRandom rnd, boolean maintenance) throws IOException
    {
        Number160 slaveId = createRandomId(rnd);
        PeerMap slaveMap = new PeerMap(new PeerMapConfiguration(slaveId).peerNoVerification());
        Peer slave = new PeerBuilder(slaveId)
            .masterPeer(master)
            .enableMaintenance(maintenance)
            .externalBindings(new Bindings())
            .peerMap(slaveMap)
            .start();
        System.out.printf("Created slave peer %s.\n", slave.peerID());
        return slave;
    }

    public static long StartBenchmark(String caller)
    {
    	System.out.printf("%s: Starting Benchmarking...\n", caller);
        return System.currentTimeMillis();
    }

    public static void StopBenchmark(long start, String caller)
    {
    	long stop = System.currentTimeMillis();
    	long millis = stop - start;
        System.out.printf("%s: Stopped Benchmarking.\n", caller);
        System.out.printf("%s: %s ns | %s ms | %s s\n", caller, toNanos(millis), toMillis(millis), toSeconds(millis));
    }

    private static double toSeconds(long millis)
    {
        return (double) millis / 1000;
    }

    private static double toMillis(long millis)
    {
    	return (double) millis;
    }

    private static double toNanos(long millis)
    {
        return (double) toSeconds(millis) * 1000000000;
    }
    
    private static Number160 createRandomId(InteropRandom rnd) {
    	int[] vals = new int[Number160.INT_ARRAY_SIZE];
    	for (int i = 0; i < vals.length; i++) {
    		vals[i] = rnd.nextInt(Integer.MAX_VALUE);
    	}
    	return new Number160(vals);
    }
}
