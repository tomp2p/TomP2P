package net.tomp2p.holep.testapp;

import ch.qos.logback.classic.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HolePTestDriver {

	private static final Logger LOG = LoggerFactory.getLogger(HolePTestDriver.class);

	public static void main(String[] args) throws Exception {

		// set Logger Level
		ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
				.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
		LOG.warn("Logger with Level " + Level.TRACE.toString() + " initialized");

		HolePTestApp testApp = new HolePTestApp();
		HolePTestController controller = new HolePTestController("DaView", testApp);

		switch (args.length) {
		case 0:
			testApp.startMasterPeer();
			break;
		case 2:
			testApp.startNATPeer(args);
			break;
		case 3:
			testApp.startNormalPeer(args);
			break;
		default:
			throw new IllegalArgumentException(
					"The Application can't start with the given arguments. The arguments have to be like this: \n args[0] = 192.168.2.xxx \n args[1] = \"id\n");
		}

//		testApp.runTextInterface();
	}
}
