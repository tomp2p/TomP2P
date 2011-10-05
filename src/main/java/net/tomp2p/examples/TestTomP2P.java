package net.tomp2p.examples;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author E.Boldyrev
 */
public class TestTomP2P {
	private static final String CLASS_NAME = TestTomP2P.class.getName();
	private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

	private final static int DEFAULT_NODE_ID = 4001;
	private final static int DIALOGS_QUEUE_ID = 1;
	private final static String STORE_PEER = "store_peer";
	private final static String GET_PEER = "get_peer";

	private final static Number160 DIALOG_QUEUE_HASH =
Number160.createHash(DIALOGS_QUEUE_ID);

	final private Peer peer;
	PeerAddress spAddr;

	public TestTomP2P(int nodeId) throws Exception {
		this.peer = new Peer(Number160.createHash(nodeId));
		this.peer.listen(4000 + nodeId, 4000 + nodeId);

		initSuperPeer();
		bootstrapPeer();
	}

	public TestTomP2P(String nodeId, int port) throws Exception {
		this.peer = new Peer(Number160.createHash(nodeId));
		this.peer.listen(port, port);

		initSuperPeer();
		bootstrapPeer();
	}

	private void initSuperPeer() throws Exception {
		LOG.info("begin initSuperPeer()");
		Number160 superPeerId = Number160.createHash(1000);
		spAddr = new PeerAddress(superPeerId, InetAddress.getByName("127.0.0.1"), 5000, 5000);
		FutureDiscover future = peer.discover(spAddr);
		future.awaitUninterruptibly();
		LOG.info("end initSuperPeer()");
	}

	private void bootstrapPeer() throws UnknownHostException {
		LOG.info("begin bootstrapPeer()");
		// peer.getPeerBean().getPeerMap().addAddressFilter(InetAddress.getByName("0.0.0.0"));
		// FutureBootstrap fb = this.peer.bootstrapBroadcast(4001);
		FutureBootstrap fb = this.peer.bootstrap(spAddr);// Broadcast(4001);
		fb.awaitUninterruptibly();
		LOG.info("end bootstrapPeer()");
		peer.discover(fb.getBootstrapTo().iterator().next()).awaitUninterruptibly();
	}

	public static void main(String[] args) throws NumberFormatException,
Exception {
		LOG.info("1");
		TestTomP2P dns = null;
		// if (args.length == 3) {
		// dns = new TestTomp2p(Integer.parseInt(args[0]));
		// dns.store(args[1], args[2]);
		// }

		if (args.length == 2) {

		}

		if (args.length == 1) {
			LOG.info("2");
			if (args[0].equals("store")) {
				LOG.info("Store");
				dns = new TestTomP2P(STORE_PEER, 4001);
				dns.storeTestMap();
			} else if (args[0].equals("get")) {
				LOG.info("Get");
				dns = new TestTomP2P(GET_PEER, 4002);
				LOG.info("Dialog queue :" + dns.getTestMap());
			}
		}
	}
	
	private void storeTestMap() throws IOException {
		ArrayList<ServiceMessageData> dialogsList = new
ArrayList<ServiceMessageData>();

		ServiceMessageData smd = new ServiceMessageData();
		smd.address = Number160.createHash((int) (Math.random() * 100));
		smd.command = "10";

		ServiceMessageData smd2 = new ServiceMessageData();
		smd2.address = Number160.createHash((int) (Math.random() * 100));
		smd2.command = "21";

		ServiceMessageData smd3 = new ServiceMessageData();
		smd3.address = Number160.createHash((int) (Math.random() * 100));
		smd3.command = "31";

		ConfigurationStore cs = new ConfigurationStore();
		cs.setContentKey(DIALOG_QUEUE_HASH);

		BaseFuture baseFurute = peer.put(Number160.createHash(STORE_PEER),
new Data(dialogsList), cs)
				.awaitUninterruptibly();

		if (baseFurute.isSuccess())
			LOG.info("Successfully stored the value");
		else {
			LOG.info("ERROR. Failed to store the value: ");
			LOG.info(baseFurute.getFailedReason());
		}
	}

	private String getTestMap() throws ClassNotFoundException, IOException {
		// HashSet<Number160> keys = new HashSet<Number160>();
		// keys.add(DIALOG_QUEUE_HASH);

		// FutureDHT futureDHT = peer
		// .get(Number160.createHash(STORE_PEER), keys,
//Configurations.defaultGetConfiguration());

		ConfigurationGet cs = new ConfigurationGet();
		cs.setContentKey(DIALOG_QUEUE_HASH);

		FutureDHT futureDHT = peer.get(Number160.createHash(STORE_PEER), cs);

		futureDHT.awaitUninterruptibly();
		if (futureDHT.isSuccess()) {
			ArrayList<ServiceMessageData> dialogsList =
(ArrayList<ServiceMessageData>) futureDHT.getData().values()
					.iterator().next().getObject();
			StringBuilder result = new StringBuilder();
			for (ServiceMessageData smd : dialogsList) {
				result.append("address: ");
				result.append(smd.address);
				result.append(", command: ");
				result.append(smd.command);
				result.append(";\n");
			}
			return result.toString();//
//futureDHT.getData().values().iterator().next().getObject().toString();
		}
		return "not found";
	}

	class ServiceMessageData {
		Number160 address;
		String command;
	}
}


