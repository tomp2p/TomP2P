package net.tomp2p;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Inbox {

	// [bmArg] [nrWarmups] [nrRepetitions] [resultsDir] ([suffix])
	public static void main(String[] args) throws IOException {

		if (args.length < 4) {
			System.err.println("Argument(s) missing.");
			System.exit(-1);
		}
		String bmArg = args[0];
		int nrWarmups = Integer.parseInt(args[1]);
		int nrRepetitions = Integer.parseInt(args[2]);
		String resultsDir = args[3];
		String suffix = args.length >= 5 ? args[4] : "";
		Arguments arguments = new Arguments(bmArg, nrWarmups, nrRepetitions, resultsDir, suffix);

		try {
			if (nrRepetitions < 1) {
				throw new IllegalArgumentException("NrRepetitions must be >= 1.");
			}
			System.out.println(arguments);
			execute(arguments);
		} catch (Exception ex) {
			System.err.println(String.format("Exception occurred:\n%s.", ex));
			System.out.println("Exiting due to error.");
			System.exit(-2);
		}
		System.out.println("Exiting with success.");
		System.exit(0);
	}

	private static void execute(Arguments args) throws Exception {

		System.out.println(String.format("Argument: %s ", args.getBmArg()));
		printStopwatchProperties();

		double[] results;
		switch (args.getBmArg()) {
			case "bootstrap":
				results = new BootstrapBenchmark().benchmark(args);
				break;
			default:
				throw new IllegalArgumentException("No valid benchmark argument.");
		}

		printResults(results);
		writeFile(args, results);
	}

	private static void printStopwatchProperties() {

	}

	private static void printResults(double[] results) {
		System.out.println("-------------------- RESULTS --------------------");
		for (double res : results) {
			System.out.println(res);
		}
		System.out.printf("Mean: %s ms.\n", Statistics.calculateMean(results));
		System.out.printf("Variance: %s ms.\n", Statistics.calculateVariance(results));
		System.out.printf("Standard Deviation: %s ms.\n", Statistics.calculateStdDev(results));
		System.out.println("-------------------------------------------------");
	}

	private static void writeFile(Arguments args, double[] results) throws IOException {
		File file = new File(args.getResultsDir() + "/" + args.getBmArg() + "_java" + args.getSuffix()
				+ ".txt");
		FileOutputStream fos = new FileOutputStream(file);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		try {
			bw.write(String.format("%s, %s", "Iteration", "Java" + args.getSuffix()));
			bw.newLine();
			for (int i = 0; i < results.length; i++) {
				bw.write(String.format("%s, %s", i, results[i]));
				bw.newLine();
			}
		} finally {
			bw.close();
		}
		System.out.printf("Results written to %s.\n", file.getAbsolutePath());
	}
}
