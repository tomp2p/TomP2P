package net.tomp2p;

import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.InteropRandom;

public abstract class Profiler {

	protected static final InteropRandom Rnd = new InteropRandom(42);
	protected Peer[] Network;
	
	public double[] profileCpu(Arguments args) throws Exception {
		
		try {
			BenchmarkUtil.printStopwatchProperties();
			
			System.out.println("Setting up...");
			setup();
			
			long[] warmups = new long[args.getNrWarmups()];
	        long[] repetitions = new long[args.getNrRepetitions()];

	        BenchmarkUtil.warmupTimer();
	        BenchmarkUtil.reclaimResources();
	        System.out.printf("Started CPU profiling with %s warmups, %s repetitions...\n", warmups.length, repetitions.length);
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

	        System.out.println("Stopped CPU profiling.");

	        // combine warmup and benchmark results
	        long[] results = new long[warmups.length + repetitions.length];
	        double[] resultsD = new double[results.length];
	        System.arraycopy(warmups, 0, results, 0, warmups.length);
	        System.arraycopy(repetitions, 0, results, warmups.length, repetitions.length);
	        
	        // convert results from ns to ms
	        for (int i = 0; i < results.length; i++)
	        {
	            resultsD[i] = results[i] / 1000000;
	        }
	        return resultsD;
		} finally {
			
			System.out.println("Shutting down...");
			shutdown();
			System.out.println("Shut down.");
		}
	}
	
	public double[] profileMemory(Arguments args) throws Exception {
		
		try {
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
	        
	        // convert results from bytes to megabytes
	        for (int i = 0; i < results.length; i++)
	        {
	            resultsD[i] = results[i] / 1000000;
	        }
	        return resultsD;
		} finally {
			System.out.println("Shutting down...");
			shutdown();
			System.out.println("Shut down.");
		}
	}
	
	protected abstract void setup() throws Exception;
	
	protected abstract void shutdown() throws Exception;
	
	protected abstract void execute() throws Exception;
}
