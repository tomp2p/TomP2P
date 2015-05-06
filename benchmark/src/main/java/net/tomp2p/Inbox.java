package net.tomp2p;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class Inbox {

	// [bmArg] [type] [nrWarmups] [nrRepetitions] [resultsDir] ([suffix])
	public static void main(String[] args) throws Exception {

		if (args.length == 1 && args[0].equals("server")) {
            Server.setup();
        } else if (args.length < 5) {
			System.err.println("Argument(s) missing.");
			System.exit(-1);
		} else {
			String bmArg = args[0];
			String type = args[1];
			int nrWarmups = Integer.parseInt(args[2]);
			int nrRepetitions = Integer.parseInt(args[3]);
			String resultsDir = args[4];
			String suffix = args.length >= 6 ? args[5] : "";
			Arguments arguments = new Arguments(bmArg, type, nrWarmups, nrRepetitions, resultsDir, suffix);

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
		}
		System.out.println("Exiting with success.");
		System.exit(0);
	}

	private static void execute(Arguments args) throws Exception {

		System.out.println(String.format("Argument: %s ", args.getBmArg()));

		double[] results = null;
		switch (args.getBmArg()) {
			case "bootstrap":
				switch (args.getType()) {
					case "cpu":
						results = new BootstrapProfiler().profileCpu(args);
						break;
					case "memory":
						results = new BootstrapProfiler().profileMemory(args);
						break;
				}
				break;
			case "send-local-udp":
				switch (args.getType()) {
					case "cpu":
						results = new SendDirectLocalProfiler(true).profileCpu(args);
						break;
					case "memory":
						results = new SendDirectLocalProfiler(true).profileMemory(args);
						break;
				}
				break;
			case "send-local-tcp":
				switch (args.getType()) {
					case "cpu":
						results = new SendDirectLocalProfiler(false).profileCpu(args);
						break;
					case "memory":
						results = new SendDirectLocalProfiler(false).profileMemory(args);
						break;
				}
				break;
			case "send-remote-udp":
				determineServerAddress(args);
				switch (args.getType()) {
					case "cpu":
						results = new SendDirectRemoteProfiler(true).profileCpu(args);
						break;
					case "memory":
						results = new SendDirectRemoteProfiler(true).profileMemory(args);
						break;
				}
				break;
			case "send-remote-tcp":
				determineServerAddress(args);
				switch (args.getType()) {
					case "cpu":
						results = new SendDirectRemoteProfiler(false).profileCpu(args);
						break;
					case "memory":
						results = new SendDirectRemoteProfiler(false).profileMemory(args);
						break;
				}
				break;
			default:
				throw new IllegalArgumentException("No valid benchmark argument.");
		}

		printResults(results);
		writeFile(args, results);
	}

	/**
	 * Requests the server address from the user and stores it to the arguments.
	 * @param args
	 * @throws UnknownHostException 
	 */
	private static void determineServerAddress(Arguments args) throws UnknownHostException {
        // ask user for remote address
        System.out.println("Please enter server address: [PeerID] [IP address] [TCP port] [UDP port]");
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();
        scanner.close();
        if (input != null)
        {
            String[] parts = input.split(" ");
            if (parts.length != 4)
            {
                throw new IllegalArgumentException("PeerID, IP address, TCP and UDP ports required.");
            }
            Number160 n160 = new Number160(parts[0]);
            InetAddress ip = InetAddress.getByName(parts[1]);
            int tcpPort = Integer.parseInt(parts[2]);
            int udpPort = Integer.parseInt(parts[3]);
            PeerAddress serverAddress = new PeerAddress(n160, ip, tcpPort, udpPort);
            args.setParam(serverAddress);
        }
        else
        {
            throw new NullPointerException("input");
        }
    }
		
	private static void printResults(double[] results) {
		System.out.println("-------------------- RESULTS --------------------");
		for (double res : results) {
			System.out.println(res);
		}
		System.out.printf("Mean: %s\n", Statistics.calculateMean(results));
		System.out.printf("Variance: %s\n", Statistics.calculateVariance(results));
		System.out.printf("Standard Deviation: %s\n", Statistics.calculateStdDev(results));
		System.out.println("-------------------------------------------------");
	}

	private static void writeFile(Arguments args, double[] results) throws IOException {
		
		String path = String.format("%s/%s-%s_java%s.txt", args.getResultsDir(), args.getBmArg(), args.getType(), args.getSuffix());
		File file = new File(path);
		FileOutputStream fos = new FileOutputStream(file);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		try {
			bw.write(String.format("%s, %s", "Iteration", "Java" + args.getType() + args.getSuffix()));
			bw.newLine();
			for (int i = 0; i < results.length; i++) {
				bw.write(String.format("%s, %s", i, results[i]));
				bw.newLine();
			}
		} finally {
			bw.close();
		}
		System.out.printf("Results written to %s.\n", path);
	}
}
