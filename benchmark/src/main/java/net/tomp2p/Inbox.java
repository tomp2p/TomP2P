package net.tomp2p;

public class Inbox {

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Argument missing.");
			System.exit(-1);
		}
		String argument = args[0];
		int repetitions = args.length >= 2 ? Integer.parseInt(args[1]) : 1;

		try {
			System.out.println(String.format("Argument: %s ", argument));
			System.out.println(String.format("Repetitions: %s ", repetitions));
			execute(argument, repetitions);
		} catch (Exception ex) {
			System.err.println(String.format("Exception occurred:\n%s.", ex));
			System.out.println("Exiting due to error.");
			System.exit(-2);
		}
		System.out.println("Exiting with success.");
		System.exit(0);
		
	}
	
	private static void execute(String argument, int repetitions) throws Exception {
		printStopwatchProperties();
		for (int i = 0; i < repetitions; i++) {
			System.out.printf("Executing repetition %s / %s:\n", i+1, repetitions);
			switch (argument) {
				case "bb1":
					BootstrapBenchmark.benchmark1(i);
					break;
			}
		}
	}
	
	private static void printStopwatchProperties() {
		
	}
}
