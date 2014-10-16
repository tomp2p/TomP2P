package net.tomp2p.examples.relay;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.android.AndroidRelayConfiguration;
import net.tomp2p.relay.android.GCMServerCredentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts three kinds of Peers (in this order):
 * <ul>
 * <li>Relay Node(s)</li>
 * <li>Firewalled Node(s)</li>
 * <li>Query Node(s)</li>
 * </ul>
 * 
 * The firewalled nodes are relayed by the relay nodes. The query nodes try to make put / get / remove
 * requests on these nodes.
 * 
 * The Google Cloud Messaging Authentication Key is required as argument when using {@link RelayType#ANDROID}
 * 
 * @author Nico Rutishauser
 * 
 */
public class ExampleRelaySituation {

	private static final Logger LOG = LoggerFactory.getLogger(ExampleRelaySituation.class);

	private static final int NUM_RELAY_PEERS = 4;
	private static final int NUM_MOBILE_PEERS = 6;
	private static final int NUM_QUERY_PEERS = 10;

	public static void main(String[] args) throws IOException, InterruptedException {
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

		// setup relay peers and start querying
		ExampleRelaySituation situation = new ExampleRelaySituation(NUM_RELAY_PEERS, NUM_MOBILE_PEERS, NUM_QUERY_PEERS,
				gcmKey, gcmSender);
		situation.setupPeers();

		// wait for finding each other
		Thread.sleep(5000);
		situation.startQueries();
	}

	/**
	 * Port configuration
	 */
	private static final int RELAY_START_PORT = 4000;
	private static final int MOBILE_START_PORT = 8000;
	private static final int QUERY_START_PORT = 6000;

	/**
	 * Unreachable peer configuration
	 */
	private static final int PEER_MAP_UPDATE_INTERVAL_S = 60;
	private static final String GCM_REGISTRATION_ID = "abc";
	private static final RelayType RELAY_TYPE = RelayType.OPENTCP;
	private final String gcmKey;
	private final long gcmSenderId;

	/**
	 * Query peer configuration
	 */
	private static final long MEDIUM_SLEEP_TIME_MS = 5000;
	private static final int MEDIUM_DATA_SIZE_BYTES = 1024;

	/**
	 * Number of peers of each kind
	 */
	private final int relayPeers;
	private final int mobilePeers;
	private final int queryPeers;

	/**
	 * Holds active peers by each kind
	 */
	private final List<Peer> relays;
	private final List<PeerNAT> mobiles;
	private final List<QueryNode> queries;

	public ExampleRelaySituation(int relayPeers, int mobilePeers, int queryPeers, String gcmKey, long gcmSenderId) {
		this.gcmKey = gcmKey;
		this.gcmSenderId = gcmSenderId;
		assert relayPeers > 0 && relayPeers <= RELAY_TYPE.maxRelayCount();
		this.relayPeers = relayPeers;
		this.mobilePeers = mobilePeers;
		this.queryPeers = queryPeers;

		this.relays = new ArrayList<Peer>(relayPeers);
		this.mobiles = new ArrayList<PeerNAT>(mobilePeers);
		this.queries = new ArrayList<QueryNode>(queryPeers);
	}

	public void setupPeers() throws IOException {
		/**
		 * Init the relay peers first
		 */
		for (int i = 0; i < relayPeers; i++) {
			Peer peer = new PeerBuilder(new Number160(RELAY_START_PORT + i)).ports(RELAY_START_PORT + i).start();
			// Note: Does not work if relay does not have a PeerDHT
			new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("RELAY", false)).start();
			new PeerBuilderNAT(peer).androidRelayConfiguration(new AndroidRelayConfiguration().bufferAgeLimit(30 * 1000))
					.start();

			relays.add(peer);
			LOG.debug("Relay peer {} started", i);
		}

		/**
		 * Then init the mobile peers
		 */
		for (int i = 0; i < mobilePeers; i++) {
			Peer peer = new PeerBuilder(new Number160(MOBILE_START_PORT + i)).ports(MOBILE_START_PORT + i).start();
			bootstrap(peer);

			// start DHT capability
			new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("UNREACHABLE", true)).start();

			LOG.debug("Connecting to Relay now");
			Set<PeerAddress> relayAddresses = new HashSet<PeerAddress>(relayPeers);
			for (Peer relay : relays) {
				relayAddresses.add(relay.peerAddress());
			}

			PeerBuilderNAT builder = new PeerBuilderNAT(peer).peerMapUpdateInterval(PEER_MAP_UPDATE_INTERVAL_S)
					.relays(relayAddresses).relayType(RELAY_TYPE);
			if (RELAY_TYPE == RelayType.ANDROID) {
				GCMServerCredentials gcmCredentials = new GCMServerCredentials().senderAuthenticationKey(gcmKey)
						.senderId(gcmSenderId).registrationId(GCM_REGISTRATION_ID);
				builder.gcmServerCredentials(gcmCredentials);
			}

			PeerNAT peerNat = builder.start();
			FutureRelayNAT futureRelayNAT = peerNat.startRelay(relays.get(0).peerAddress()).awaitUninterruptibly();
			if (!futureRelayNAT.isSuccess()) {
				LOG.error("Cannot connect to Relay(s). Reason: {}", futureRelayNAT.failedReason());
				return;
			}

			mobiles.add(peerNat);
			LOG.debug("Mobile peer {} connected to DHT and Relay(s)", i);
		}

		/**
		 * Finally init the query peers
		 */
		for (int i = 0; i < queryPeers; i++) {
			Peer peer = new PeerBuilder(new Number160(QUERY_START_PORT + i)).ports(QUERY_START_PORT + i).start();
			bootstrap(peer);

			PeerDHT peerDHT = new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("QUERY", false)).start();
			new PeerBuilderNAT(peer).start();

			queries.add(new QueryNode(peerDHT, MEDIUM_SLEEP_TIME_MS, MEDIUM_DATA_SIZE_BYTES));
			LOG.debug("Query peer {} started", i);
		}
	}

	private boolean bootstrap(Peer peer) throws UnknownHostException {
		// bootstrap
		InetAddress bootstrapTo = InetAddress.getLocalHost();
		BootstrapBuilder bootstrapBuilder = peer.bootstrap().inetAddress(bootstrapTo).ports(RELAY_START_PORT);
		BaseFuture bootstrap = bootstrapBuilder.start().awaitUninterruptibly();

		if (bootstrap == null) {
			LOG.error("Cannot bootstrap");
			return false;
		} else if (bootstrap.isFailed()) {
			LOG.error("Cannot bootstrap. Reason: {}", bootstrap.failedReason());
			return false;
		}

		LOG.debug("Peer has bootstrapped");
		return true;
	}

	public void startQueries() {
		for (final QueryNode queryPeer : queries) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					Random rnd = new Random();
					Number160 peerID = mobiles.get(rnd.nextInt(mobilePeers)).peer().peerID();
					try {
						queryPeer.putGetSpecific(new Number640(peerID, Number160.ZERO, new Number160(rnd), Number160.ZERO));
					} catch (Exception e) {
						LOG.error("Cannot put / get / remove", e);
					}
				}
			}, "Query node").start();
		}
	}
}
