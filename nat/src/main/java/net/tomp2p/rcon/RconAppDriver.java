package net.tomp2p.rcon;

import java.io.IOException;
import java.net.UnknownHostException;

import net.tomp2p.rcon.prototype.SimpleRconClient;

public class RconAppDriver {

	private static RconAppDriver appDriver = new RconAppDriver();

	private RconAppDriver() {

	}

	public static RconAppDriver getInstance() {
		return appDriver;
	}

	public static void main(String[] args) throws UnknownHostException {

		if (args.length > 0) {
			SimpleRconClient.start(false, args[1]);

			if (args.length == 2) {
				SimpleRconClient.usualBootstrap(args[0]);
				System.out.println();
				System.out.println("usualBootstrap Success!");
				System.out.println();
			} else if (args.length > 2) {
				System.out.println();
				System.out.println("Start relaying");
				System.out.println();

				SimpleRconClient.natBootstrap(args[0]);
			}
		} else {
			SimpleRconClient.start(true, null);
		}

		RconController rController = new RconController();
		rController.start();
	}
}
