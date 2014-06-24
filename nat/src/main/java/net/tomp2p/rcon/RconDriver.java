package net.tomp2p.rcon;

import java.io.IOException;
import java.net.UnknownHostException;

public class RconDriver {

	public static void main(String[] args) throws IOException {
		
		if (args.length > 0) {
			SimpleRconClient.start(false);
			
			if (args.length == 1) {
				SimpleRconClient.usualBootstrap(args[0]);
				System.out.println();
				System.out.println("usualBootstrap Success!");
				System.out.println();
			} else if (args.length > 1){
				System.out.println();
				System.out.println("Start relaying");
				System.out.println();
				
				SimpleRconClient.natBootstrap(args[0]);
			}
			sendDummy();
		} else {
			SimpleRconClient.start(true);
		}
	}

	private static void sendDummy() throws IOException {
		System.out.println();
		System.out.println("SENDDUMMY");
		System.out.println();
		
		SimpleRconClient.sendDummy("yoMama");
	}
}
