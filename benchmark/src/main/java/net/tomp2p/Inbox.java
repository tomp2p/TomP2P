package net.tomp2p;

import java.io.IOException;

public class Inbox {

	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			System.err.println("Argument missing.");
			System.in.read();
			System.exit(-1);
		}
		String argument = args[0];
		int repetitions = args.length >= 2 ? Integer.parseInt(args[1]) : 1;

		try {
			if (repetitions < 1) {
				throw new IllegalArgumentException("Repetitions must be >= 1.");
			}
			System.out.println(String.format("Argument: %s ", argument));
			System.out.println(String.format("Repetitions: %s ", repetitions));
			execute(argument, repetitions);
		} catch (Exception ex) {
			System.err.println(String.format("Exception occurred:\n%s.", ex));
			System.out.println("Exiting due to error.");
			System.exit(-2);
		}
		System.out.println("Exiting with success.");
		System.in.read();
		System.exit(0);

	}

	private static void execute(String argument, int repetitions) throws Exception {
		printStopwatchProperties();
		double[] results = new double[repetitions];
		for (int i = 0; i < repetitions; i++) {
			System.out.printf("Executing repetition %s / %s:\n", i + 1, repetitions);
			double repetitionResult = 0;
			switch (argument) {
				case "bb1":
					repetitionResult = BootstrapBenchmark.benchmark1();
					break;
				default:
					throw new IllegalArgumentException("No valid benchmark argument.");
			}
			
			// store repetition result
			results[i] = repetitionResult;
		}
		
		printResults(results);
	}

	private static void printStopwatchProperties() {

	}
	
	private static void printResults(double[] results)
    {
		System.out.println("----------- RESULTS -----------");
        for (double res : results)
        {
        	System.out.println(res);
        }
        System.out.printf("Mean: %s ms.\n", Statistics.calculateMean(results));
        System.out.printf("Variance: %s ms.\n", Statistics.calculateVariance(results));
        System.out.printf("Standard Deviation: %s ms.\n", Statistics.calculateStdDev(results));
        System.out.println("-------------------------------");
    }
}
