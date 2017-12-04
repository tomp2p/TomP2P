package net.tomp2p.rcon.prototype;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class RconAppDriver {

	private static RconAppDriver appDriver = new RconAppDriver();
	private static final Logger LOG = LoggerFactory.getLogger(RconAppDriver.class);
	
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

		// set Logger Level
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
				.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.WARN);
		LOG.warn("Logger with Level " + Level.WARN.toString() + " initialized");

		if (args.length > 0) {
			SimpleRconClient.start(false, args[1]);

			// do usual Bootstrap (normal peer)
			if (args.length == 2) {
				SimpleRconClient.usualBootstrap(args[0]);
				System.err.println();
				System.err.println("usualBootstrap Success!");
				System.err.println();

				// do relay bootstrapping (nat peer)
			} else if (args.length > 2) {
				System.err.println();
				System.err.println("Start relaying");
				System.err.println();

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
