package net.tomp2p;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.InteropRandom;

public class BootstrapBenchmark {

	public static double benchmark1(Arguments args) throws Exception {

		// each run should create same IDs
		InteropRandom rnd = new InteropRandom(42);
		Peer master = null;

		try {
			Peer[] peers = setupNetwork(args, rnd);
			master = peers[0];
			
			// benchmark: bootstrap
			Peer newPeer = BenchmarkUtil.createSlave(master, rnd, true, false);

			long start = BenchmarkUtil.startBenchmark(args.getBmArg());
			FutureBootstrap future = newPeer.bootstrap().peerAddress(master.peerAddress()).start();
			future.awaitUninterruptibly();
			return BenchmarkUtil.stopBenchmark(start, args.getBmArg());

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	public static double benchmark2(Arguments args) throws Exception {

		// each run should create same IDs
		InteropRandom rnd = new InteropRandom(42);
		Peer master = null;

		try {
			Peer[] peers = setupNetwork(args, rnd);
			master = peers[0];
			
			// benchmark: peer creation, bootstrap
			long start = BenchmarkUtil.startBenchmark(args.getBmArg());
			Peer newPeer = BenchmarkUtil.createSlave(master, rnd, true, false);
			FutureBootstrap future = newPeer.bootstrap().peerAddress(master.peerAddress()).start();
			future.awaitUninterruptibly();
			return BenchmarkUtil.stopBenchmark(start, args.getBmArg());

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	public static double benchmark3(Arguments args) throws Exception {

		// each run should create same IDs
		InteropRandom rnd = new InteropRandom(42);
		Peer master = null;

		try {
			Peer[] peers = setupNetwork(args, rnd);
			master = peers[0];
			
			/// benchmark: 20x peer creation, bootstrap
			long start = BenchmarkUtil.startBenchmark(args.getBmArg());
			
			FutureBootstrap[] futures = new FutureBootstrap[20];
			for (int i = 0; i < futures.length; i++) {
				Peer newPeer = BenchmarkUtil.createSlave(master, rnd, true, false);
				futures[i] = newPeer.bootstrap().peerAddress(master.peerAddress()).start();
			}
			for (FutureBootstrap future : futures) {
				future.awaitUninterruptibly();
			}
			
			return BenchmarkUtil.stopBenchmark(start, args.getBmArg());

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	private static Peer[] setupNetwork(Arguments args, InteropRandom rnd) throws Exception {
		// setup
		Peer[] peers = BenchmarkUtil.createNodes(500, rnd, 9099, true, false);

		// bootstrap all slaves to the master
		FutureBootstrap[] futures = new FutureBootstrap[peers.length - 1];
		for (int i = 1; i < peers.length; i++) {
			futures[i - 1] = peers[i].bootstrap().peerAddress(peers[0].peerAddress()).start();
		}
		System.out.println("Waiting for all peers to finish bootstrap...");
		for (FutureBootstrap future : futures) {
			future.awaitUninterruptibly();
		}
		System.out.printf("Bootstrap environment set up with %s peers.\n", peers.length);

		// wait for peers to know each other
		System.out.printf("Waiting %s seconds...\n", args.getWarmupSec());
		Thread.sleep(args.getWarmupSec() * 1000);
		return peers;
	}
}
