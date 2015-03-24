package net.tomp2p;

import java.io.IOException;

import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.InteropRandom;

public abstract class BaseBenchmark {

	public static final int NETWORK_SIZE = 5;
	
	public double[] benchmark(Arguments args) throws Exception {
		
		System.out.println("Setting up...");
		setup();
		
		long[] warmups = new long[args.getNrWarmups()];
        long[] repetitions = new long[args.getNrRepetitions()];

        BenchmarkUtil.warmupTimer();
        BenchmarkUtil.reclaimResources();
        System.out.printf("Started Benchmarking with %s warmups, %s repetitions...\n", warmups.length, repetitions.length);
        long start;

        // warmups
        for (int i = 0; i < warmups.length; i++)
        {
        	System.out.printf("Warmup %s...\n", i);
            start = System.nanoTime();
            execute();
            warmups[i] = System.nanoTime() - start;
        }
        
        // repetitions
        for (int i = 0; i < repetitions.length; i++)
        {
        	System.out.printf("Repetitions %s...\n", i);
            start = System.nanoTime();
            execute();
            repetitions[i] = System.nanoTime() - start;
        }

        System.out.println("Stopped Benchmarking.");

        // combine warmup and benchmark results
        long[] results = new long[warmups.length + repetitions.length];
        double[] resultsD = new double[results.length];
        System.arraycopy(warmups, 0, results, 0, warmups.length);
        System.arraycopy(repetitions, 0, results, warmups.length, repetitions.length);
        
        // convert results from ns to ms
        for (int i = 0; i < results.length; i++)
        {
            resultsD[i] = toMillis(results[i]);
        }
        return resultsD;
	}
	
	protected abstract void setup() throws Exception;
	
	protected abstract void execute() throws Exception;
	
	protected static Peer[] setupNetwork(InteropRandom rnd) throws IOException
    {
		System.out.printf("Creating network with %s peers...\n", NETWORK_SIZE);
        return BenchmarkUtil.createNodes(NETWORK_SIZE, rnd, 7077, false, false);
    }

	private static double toMillis(double nanos) {
		return nanos / 1000000;
	}
}
