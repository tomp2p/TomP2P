package net.tomp2p;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.InteropRandom;

public class BootstrapBenchmark {

	// TODO add logger
	
	public static void benchmark1(int repetitionNr) throws Exception {
		
		// each run should create same IDs
		InteropRandom rnd = new InteropRandom(42);
		Peer master = null;
		
		try {
			// setup
			Peer[] peers = BenchmarkUtil.createNodes(500, rnd, 9099, true, false);
			master = peers[0];
			
			// bootstrap all slaves to the master
			FutureBootstrap[] futures = new FutureBootstrap[peers.length-1];
			for (int i = 1; i < peers.length; i++) {
				futures[i-1] = peers[i].bootstrap().peerAddress(master.peerAddress()).start();
			}
			for (FutureBootstrap future : futures) {
				future.awaitUninterruptibly();
			}
			System.out.printf("Bootstrap environment set up with %s peers.\n", peers.length);
			
			// wait for peers to know each other
			final int delaySec = 30;
			System.out.printf("Waiting %s seconds.\n", delaySec);
			Thread.sleep(delaySec*1000);
			
			// bootstrap a new peer, measure time
			Peer newPeer = BenchmarkUtil.createSlave(master, rnd, true, false);
			
			long start = BenchmarkUtil.StartBenchmark("benchmark1");
			FutureBootstrap future = newPeer.bootstrap().peerAddress(master.peerAddress()).start();
			future.awaitUninterruptibly();
			BenchmarkUtil.StopBenchmark(start, "benchmark1");
			
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
}
