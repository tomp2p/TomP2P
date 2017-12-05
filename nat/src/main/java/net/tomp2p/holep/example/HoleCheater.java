package net.tomp2p.holep.example;

import java.io.IOException;

public class HoleCheater {

	/**
	 * This is only for testing
	 * 
	 * TODO jwa: delete this.
	 * 
	 * @param args
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException {
		cheatHolePunch(args[0], Integer.valueOf(args[1]), args[2], Integer.valueOf(args[3]));
	}
	
	/**
	 * This method only works if the OS is linux and sendip is installed. The class
	 * calls the linux terminal to punch a specific hole in the NAT used. Thus a
	 * specific UDP NAT mapping will be created.
	 * 
	 * @param localAddress
	 * @param localPort
	 * @param remoteAddress
	 * @param remotePort
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void cheatHolePunch(String localAddress, int localPort, String remoteAddress, int remotePort)
			throws IOException, InterruptedException {

		StringBuilder builder = new StringBuilder();
		builder.append("sendip -p ipv4 ");
		builder.append("-is " + localAddress + " ");
		builder.append("-p udp ");
		builder.append("-us " + localPort + " ");
		builder.append("-ud " + remotePort + " ");
		builder.append("-d \"HoleP\" ");
		builder.append("-v " + remoteAddress + " ");

		Runtime runtime = Runtime.getRuntime();
		Process process = runtime.exec(builder.toString());
		process.waitFor();
		if (process.isAlive()) {
			System.out.println("not yet finished");
		}
	}
}
