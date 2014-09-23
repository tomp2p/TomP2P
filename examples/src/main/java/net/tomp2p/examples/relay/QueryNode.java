package net.tomp2p.examples.relay;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.FutureRemove;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a Peer and randomly makes a put / get / remove onto the DHT
 * 
 * @author Nico Rutishauser
 * 
 */
public class QueryNode {

	private static final Logger LOG = LoggerFactory.getLogger(QueryNode.class);

	private final Random random;
	private final long avgSleepTime;
	private final int avgBytes;
	private final List<Number160> validKeys;
	private final RoutingConfiguration routingConfig;
	private final RequestP2PConfiguration requestConfig;

	private PeerDHT peerDHT;

	public QueryNode(long avgSleepTime, int avgBytes) {
		this.avgSleepTime = avgSleepTime;
		this.avgBytes = avgBytes;
		this.random = new Random(42L);
		this.validKeys = new ArrayList<Number160>();
		this.routingConfig = new RoutingConfiguration(5, 1, 1);
		this.requestConfig = new RequestP2PConfiguration(1, 1, 0);
	}

	public void start(Number160 peerId, int port, int bootstrapPort) throws IOException {
		Peer peer = new PeerBuilder(peerId).ports(port).start();
		InetAddress bootstrapTo = InetAddress.getLocalHost();
		BootstrapBuilder bootstrapBuilder = peer.bootstrap().inetAddress(bootstrapTo).ports(bootstrapPort);
		BaseFuture bootstrap = bootstrapBuilder.start().awaitUninterruptibly();

		if (bootstrap == null) {
			LOG.error("Cannot bootstrap");
			return;
		} else if (bootstrap.isFailed()) {
			LOG.error("Cannot bootstrap. Reason: {}", bootstrap.failedReason());
			return;
		}

		peerDHT = new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("QUERY", false)).start();
		new PeerBuilderNAT(peer).start();
		LOG.debug("Peer started");
	}

	/**
	 * Start put / get / remove random keys
	 */
	public void putGetRandom() throws IOException, ClassNotFoundException {
		while (true) {
			// put some data
			if (random.nextBoolean()) {
				Number160 key = new Number160(random);
				put(key);
				sleep();
			}

			// get some data
			if (random.nextBoolean() && !validKeys.isEmpty()) {
				get(validKeys.get(random.nextInt(validKeys.size())));
				sleep();
			}

			// remove some data
			if (random.nextBoolean() && !validKeys.isEmpty()) {
				remove(validKeys.get(random.nextInt(validKeys.size())));
				sleep();
			}
		}
	}

	private void put(Number160 key) {
		Data data = generateRandomData();
		FuturePut futurePut = peerDHT.put(key).data(data).start().awaitUninterruptibly();
		LOG.debug("Put of {} bytes is success = {}", data.length(), futurePut.isSuccess());
		validKeys.add(key);
	}

	private void get(Number160 key) {
		Data data = get(new Number640(key, Number160.ZERO, Number160.ZERO, Number160.ZERO));
		byte[] byteArray = new byte[0];
		if (data != null) {
			byteArray = (byte[]) data.toBytes();
		}
		LOG.debug("Got {} bytes", byteArray.length);
	}

	public Data get(Number640 key) {
		FutureGet futureGet = peerDHT.get(key.locationKey()).contentKey(key.contentKey()).domainKey(key.domainKey())
				.versionKey(key.versionKey()).fastGet(false).routingConfiguration(routingConfig)
				.requestP2PConfiguration(requestConfig).start().awaitUninterruptibly();
		if (futureGet.data() != null) {
			return futureGet.data();
		} else {
			return null;
		}
	}

	private void remove(Number160 key) {
		if (remove(new Number640(key, Number160.ZERO, Number160.ZERO, Number160.ZERO))) {
			LOG.debug("Removed object {}", key);
			validKeys.remove(key);
		} else {
			LOG.warn("Could not remove object {}");
		}
	}

	public boolean remove(Number640 key) {
		FutureRemove remove = peerDHT.remove(key.locationKey()).contentKey(key.contentKey()).domainKey(key.domainKey())
				.versionKey(key.versionKey()).fastGet(false).start().awaitUninterruptibly();
		return remove.isSuccess();
	}

	private void sleep() {
		long sleepTime = (long) ((random.nextGaussian() + 1) * (double) avgSleepTime);
		if (sleepTime < 0) {
			sleepTime = 0;
		}
		LOG.debug("Sleeping for {}ms", sleepTime);
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			// ignore
		}
	}

	private Data generateRandomData() {
		int bytes = Math.abs((int) ((random.nextGaussian() + 1) * (double) avgBytes));
		byte[] data = new byte[bytes];
		random.nextBytes(data);
		return new Data(data);
	}
}
