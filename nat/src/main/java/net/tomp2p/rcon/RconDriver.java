package net.tomp2p.rcon;

public class RconDriver {

	public static void main(String[] args) {
		boolean success = false;
		
		SimpleRconClient.start();
		if (args.length == 1) {
			success = SimpleRconClient.usualBootstrap(args[0]);
			System.out.println();
			System.out.println("usualBootstrap Success: " + success);
			System.out.println();
		}
		if (!success) {
			System.out.println();
			System.out.println("Start relaying");
			System.out.println();
			
			SimpleRconClient.natBootstrap(args[0]);
			
			System.out.println();
			System.out.println("SENDDUMMY");
			System.out.println();
			
			SimpleRconClient.sendDummy("yoMama", true);
		} else {
			System.out.println();
			System.out.println("SENDDUMMY");
			System.out.println();
			SimpleRconClient.sendDummy("yoMama", false);
		}
	}
}
