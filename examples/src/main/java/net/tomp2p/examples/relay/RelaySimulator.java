package net.tomp2p.examples.relay;

import java.io.IOException;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 * Just starts a Peer and randomly makes a put / get / remove onto the DHT
 * 
 * @author Nico
 * 
 */
public class RelaySimulator {

	private static final Number160 RELAY_PEER_ID = new Number160(2828); // 0xb0c
	private static final Number160 MOBILE_PEER_ID = new Number160(1111); // 0x457
	private static final Number160 QUERY_PEER_ID = new Number160(7777);

	private static final int RELAY_PORT = 4622;
	private static final int MOBILE_PORT = 4623;
	private static final int QUERY_PORT = 4624;

	private static final long MEDIUM_SLEEP_TIME_MS = 5000;
	private static final int MEDIUM_DATA_SIZE_BYTES = 1024;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		new RelayNode().start(RELAY_PEER_ID, RELAY_PORT);
		Thread.sleep(1000);

		MobileNode mobile = new MobileNode(MOBILE_PEER_ID, MOBILE_PORT);
		mobile.start(RELAY_PEER_ID, RELAY_PORT);
		Thread.sleep(5000); // give some time to init relaying

		Number640 key = mobile.putOwnStorage(new Data(new String("Test")));
		Thread.sleep(5000); // give some time to init relaying

		QueryNode queryNode = new QueryNode(MEDIUM_SLEEP_TIME_MS, MEDIUM_DATA_SIZE_BYTES);
		queryNode.start(QUERY_PEER_ID, QUERY_PORT, RELAY_PORT);
		System.out.println("GET 1: " + queryNode.get(key));
		System.out.println("REMOVE: " + queryNode.remove(key));
		System.out.println("GET 2: " + queryNode.get(key));

	}

}
