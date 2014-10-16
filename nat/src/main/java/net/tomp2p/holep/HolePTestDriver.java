package net.tomp2p.holep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class HolePTestDriver {

	private static final Logger LOG = LoggerFactory.getLogger(HolePTestDriver.class);
	
	public static void main(String[] args) throws Exception {

		// set Logger Level
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
				.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.WARN);
		LOG.warn("Logger with Level " + Level.WARN.toString() + " initialized");

		HolePTestApp testApp = new HolePTestApp();
		if (!checkArguments(args)) {
			testApp.startMasterPeer();
		} else {
			testApp.startPeer(args);
		}
		testApp.runTextInterface();
	}

	private static boolean checkArguments(String[] args) {
		if (args.length > 1) {
			return true;
		} else if (args.length == 1) {
			throw new IllegalArgumentException(
					"The Application can't start with the given arguments. The arguments have to be like this: \n args[0] = 192.168.2.xxx \n args[1] = \"id\n");
		} else {
			return false;
		}
	}
}
