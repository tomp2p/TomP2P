package net.tomp2p.examples.relay;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.Ports;
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
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayClientConfig;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.buffer.MessageBufferConfiguration;

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
 * The Google Cloud Messaging Authentication Key is required as argument when using
 * {@link RelayClientConfig#ANDROID}
 * 
 * @author Nico Rutishauser
 * 
 */
public class ExampleRelaySituation {

	private static final Logger LOG = LoggerFactory.getLogger(ExampleRelaySituation.class);

	private static final int NUM_RELAY_PEERS = 1;
	private static final int NUM_MOBILE_PEERS = 0;
	private static final int NUM_QUERY_PEERS = 1;

	public static void main(String[] args) throws IOException, InterruptedException {
		String gcmKey = null;
		if (args.length < 1) {
			LOG.warn("Need the GCM Authentication key as arguments when using with Android.");
		} else {
			LOG.debug("{} is the GCM key used", args[0]);
			gcmKey = args[0];
		}

		// setup relay peers and start querying
		ExampleRelaySituation situation = new ExampleRelaySituation(gcmKey);
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
	 * Relay buffer configuration
	 */
	private static final int MAX_MESSAGE_NUM = Integer.MAX_VALUE;
	private static final long MAX_BUFFER_SIZE = Long.MAX_VALUE;
	private static final long MAX_BUFFER_AGE = 15 * 1000;
	private static final int GCM_SEND_RETIES = 5;

	/**
	 * Unreachable peer configuration
	 */
	private static final RelayType RELAY_TYPE = RelayType.ANDROID;
	private final String gcmKey;

	/**
	 * Query peer configuration
	 */
	private static final long INTERVAL_MS = 2000;
	private static final long DURATION_MS = 5 * 60 * 1000;
	private static final int DATA_SIZE_BYTES = 128;

	/**
	 * Holds active peers by each kind
	 */
	private final List<PeerNAT> relays;
	private final List<PeerNAT> mobiles;
	private final List<PeriodicQueryNode> queries;

	private final MessageBufferConfiguration bufferConfig;

	public ExampleRelaySituation(String gcmKey) {
		this.gcmKey = gcmKey;
		assert NUM_RELAY_PEERS > 0 && NUM_RELAY_PEERS <= RELAY_TYPE.maxRelayCount();

		this.relays = new ArrayList<PeerNAT>(NUM_RELAY_PEERS);
		this.mobiles = new ArrayList<PeerNAT>(NUM_MOBILE_PEERS);
		this.queries = new ArrayList<PeriodicQueryNode>(NUM_QUERY_PEERS);

		bufferConfig = new MessageBufferConfiguration().bufferAgeLimit(MAX_BUFFER_AGE).bufferCountLimit(MAX_MESSAGE_NUM)
				.bufferSizeLimit(MAX_BUFFER_SIZE);
	}

	public void setupPeers() throws IOException {
		// global configuration
		ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP = 10 * 1000;
		ConnectionBean.DEFAULT_TCP_IDLE_SECONDS = 10;
		ConnectionBean.DEFAULT_UDP_IDLE_SECONDS = 10;
		
		/**
		 * Init the relay peers first
		 */
		for (int i = 0; i < NUM_RELAY_PEERS; i++) {
			Peer peer = createPeer(RELAY_START_PORT + i);
			// Note: Does not work if relay does not have a PeerDHT
			new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("RELAY", false)).start();
			PeerNAT peerNAT = new PeerBuilderNAT(peer).bufferConfiguration(bufferConfig).gcmAuthenticationKey(gcmKey)
					.gcmSendRetries(GCM_SEND_RETIES).start();

			relays.add(peerNAT);
			LOG.debug("Relay peer {} started", i);
		}

		/**
		 * Then init the mobile peers
		 */
		for (int i = 0; i < NUM_MOBILE_PEERS; i++) {
			Peer peer = createPeer(MOBILE_START_PORT + i);
			bootstrap(peer);

			// start DHT capability
			new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("UNREACHABLE", true)).start();

			LOG.debug("Connecting to Relay now");
			Set<PeerAddress> relayAddresses = new HashSet<PeerAddress>(NUM_RELAY_PEERS);
			for (PeerNAT relay : relays) {
				relayAddresses.add(relay.peer().peerAddress());
			}

			RelayClientConfig config;
			PeerBuilderNAT builder = new PeerBuilderNAT(peer);
			if (RELAY_TYPE == RelayType.ANDROID) {
				config = RelayClientConfig.Android("abc").manualRelays(relayAddresses);
			} else {
				config = RelayClientConfig.OpenTCP();
			}

			PeerNAT peerNat = builder.start();
			FutureRelayNAT futureRelayNAT = peerNat.startRelay(config, relays.get(0).peer().peerAddress())
					.awaitUninterruptibly();
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
		for (int i = 0; i < NUM_QUERY_PEERS; i++) {
			Peer peer = createPeer(QUERY_START_PORT + i);
			bootstrap(peer);

			PeerDHT peerDHT = new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("QUERY", false)).start();
			new PeerBuilderNAT(peer).start();

			queries.add(new PeriodicQueryNode(peerDHT, INTERVAL_MS, DURATION_MS, DATA_SIZE_BYTES));
			LOG.debug("Query peer {} started", i);
		}

	}

	private Peer createPeer(int port) throws IOException {
		ChannelServerConfiguration csc = PeerBuilder.createDefaultChannelServerConfiguration();
		csc.ports(new Ports(port, port));
		csc.portsForwarding(new Ports(port, port));
		csc.connectionTimeoutTCPMillis(10 * 1000);
		csc.idleTCPSeconds(10);
		csc.idleUDPSeconds(10);
		
		return new PeerBuilder(new Number160(port)).ports(port).channelServerConfiguration(csc).start();
		
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
		for (final PeriodicQueryNode queryPeer : queries) {
			queryPeer.start(getRandomUnreachable().peerId());
		}
	}

	private PeerAddress getRandomUnreachable() {
		Random rnd = new Random();
		if (mobiles.isEmpty()) {
			List<PeerNAT> relayCopy = new ArrayList<PeerNAT>(relays);
			while (!relayCopy.isEmpty()) {
				PeerNAT relay = relayCopy.remove(rnd.nextInt(relayCopy.size()));
				Set<PeerAddress> unreachablePeers = relay.relayRPC().unreachablePeers();
				Iterator<PeerAddress> iterator = unreachablePeers.iterator();
				if (iterator.hasNext()) {
					return iterator.next();
				}
			}
			// no connected unreachable peer found
			return null;
		} else {
			return mobiles.get(rnd.nextInt(NUM_MOBILE_PEERS)).peer().peerAddress();
		}
	}
}
