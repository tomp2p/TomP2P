package net.tomp2p.rcon;

public class RconDriver {

	public static void main(String[] args) {
		
		SimpleRconClient.start();
		if (args.length > 0) {
			if (args.length == 1) {
				System.out.println();
				System.out.println("usualBootstrap Success!");
				System.out.println();
			} else if (args.length > 1){				
				System.out.println();
				System.out.println("Start relaying");
				System.out.println();
				
				SimpleRconClient.natBootstrap(args[0]);
			}
			System.out.println();
			System.out.println("SENDDUMMY");
			System.out.println();
			
			SimpleRconClient.sendDummy("yoMama", true);
		}
	}
}
