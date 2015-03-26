package net.tomp2p;

import java.io.IOException;

import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.InteropRandom;

public abstract class Profiler {

	public static final int NETWORK_SIZE = 5;
	
	public double[] profileCpu(Arguments args) throws Exception {
		
		BenchmarkUtil.printStopwatchProperties();
		
		System.out.println("Setting up...");
		setup();
		
		long[] warmups = new long[args.getNrWarmups()];
        long[] repetitions = new long[args.getNrRepetitions()];

        BenchmarkUtil.warmupTimer();
        BenchmarkUtil.reclaimResources();
        System.out.printf("Started benchmarking with %s warmups, %s repetitions...\n", warmups.length, repetitions.length);
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

        System.out.println("Stopped benchmarking.");

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
	
	public double[] profileMemory(Arguments args) throws Exception {
		
		Runtime rt = Runtime.getRuntime();

		System.out.println("Setting up...");
		setup();
		
		long[] warmups = new long[args.getNrWarmups()];
        long[] repetitions = new long[args.getNrRepetitions()];

        BenchmarkUtil.reclaimResources();
        System.out.printf("Started memory profiling with %s warmups, %s repetitions...\n", warmups.length, repetitions.length);

        // TODO combine memory/repetitions?
        // warmups
        for (int i = 0; i < warmups.length; i++)
        {
        	System.out.printf("Warmup %s...\n", i);
            execute();
            warmups[i] = rt.totalMemory() - rt.freeMemory();
        }
        
        // repetitions
        for (int i = 0; i < repetitions.length; i++)
        {
        	System.out.printf("Repetitions %s...\n", i);
            execute();
            repetitions[i] = rt.totalMemory() - rt.freeMemory();
        }

        System.out.println("Stopped memory profiling.");

        // combine warmup and benchmark results
        long[] results = new long[warmups.length + repetitions.length];
        double[] resultsD = new double[results.length];
        System.arraycopy(warmups, 0, results, 0, warmups.length);
        System.arraycopy(repetitions, 0, results, warmups.length, repetitions.length);
        
        // convert results from bytes to kilobytes
        for (int i = 0; i < results.length; i++)
        {
            resultsD[i] = results[i] / 1000;
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
