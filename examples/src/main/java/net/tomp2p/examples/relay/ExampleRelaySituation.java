package net.tomp2p.examples.relay;

import java.io.IOException;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts three Peers (in this order):
 * <ul>
 * <li>Relay Node</li>
 * <li>Firewalled Node (Mobile)</li>
 * <li>Query Node</li>
 * </ul>
 * The firewalled node is relayed by the relay node. The query node tries to make put / get / remove requests
 * on these nodes.
 * 
 * The Google Cloud Messaging Authentication Key is required as argument.
 * 
 * @author Nico Rutishauser
 * 
 */
public class ExampleRelaySituation {

	private static final Logger LOG = LoggerFactory.getLogger(ExampleRelaySituation.class);

	private static final Number160 RELAY_PEER_ID = new Number160(2828); // 0xb0c
	private static final Number160 MOBILE_PEER_ID = new Number160(1111); // 0x457
	private static final Number160 QUERY_PEER_ID = new Number160(7777);

	private static final int RELAY_PORT = 4622;
	private static final int MOBILE_PORT = 4623;
	private static final int QUERY_PORT = 4624;

	private static final long MEDIUM_SLEEP_TIME_MS = 5000;
	private static final int MEDIUM_DATA_SIZE_BYTES = 1024;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String gcmKey = null;
		long gcmSender = -1;
		if (args.length != 2) {
			LOG.warn("Need the GCM Authentication key and GCM sender ID as arguments when using with Android.");
		} else if (args.length == 2) {
			LOG.debug("{} is the GCM key used", args[0]);
			gcmKey = args[0];

			LOG.debug("{} is the sender ID used", args[0]);
			gcmSender = Long.valueOf(args[1]);
		}

		new RelayNode().start(RELAY_PEER_ID, RELAY_PORT, gcmKey);
		Thread.sleep(1000);

		MobileNode mobile = new MobileNode(MOBILE_PEER_ID, MOBILE_PORT, gcmKey, gcmSender);
		mobile.start(RELAY_PEER_ID, RELAY_PORT);
		Thread.sleep(5000); // give some time to init relaying

		Number640 key = mobile.putOwnStorage(new Data(new String("Test")));

		QueryNode queryNode = new QueryNode(MEDIUM_SLEEP_TIME_MS, MEDIUM_DATA_SIZE_BYTES);
		queryNode.start(QUERY_PEER_ID, QUERY_PORT, RELAY_PORT);

		 LOG.debug("GET 1: {}", queryNode.get(key));
		 LOG.debug("REMOVE: {}", queryNode.remove(key));
		 LOG.debug("GET 2: {}", queryNode.get(key));

		// queryNode.putGetSpecific(key);
	}

}
