package net.tomp2p.rcon.prototype;

import java.net.UnknownHostException;

public class RconAppDriver {

	private static RconAppDriver appDriver = new RconAppDriver();

	private RconAppDriver() {

	}

	public static RconAppDriver getInstance() {
		return appDriver;
	}

	/**
	 * no args = this peer is the masterpeer args[0] = ip of master (String)
	 * args[1] = my own id (String) (optional) args[2] = use relay bootstrapping
	 * 
	 * @param args
	 * @throws UnknownHostException
	 */
	public static void main(String[] args) throws UnknownHostException {

		if (args.length > 0) {
			SimpleRconClient.start(false, args[1]);

			// do usual Bootstrap (normal peer)
			if (args.length == 2) {
				SimpleRconClient.usualBootstrap(args[0]);
				System.out.println();
				System.out.println("usualBootstrap Success!");
				System.out.println();

			// do relay bootstrapping (nat peer)
			} else if (args.length > 2) {
				System.out.println();
				System.out.println("Start relaying");
				System.out.println();

				SimpleRconClient.natBootstrap(args[0]);
			}
		} else {
			SimpleRconClient.start(true, null);
		}

		// start GUI
		RconController rController = new RconController();
		rController.start();
	}
}
