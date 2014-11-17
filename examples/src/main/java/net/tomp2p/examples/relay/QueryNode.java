package net.tomp2p.examples.relay;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.FutureRemove;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
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
	private final PeerDHT peerDHT;
	
	private final DHTQueryStatistics putStats;
	private final DHTQueryStatistics getStats;
	private final DHTQueryStatistics rmvStats;
	
	public QueryNode(PeerDHT peerDHT, long avgSleepTime, int avgBytes) {
		this.peerDHT = peerDHT;
		this.avgSleepTime = avgSleepTime;
		this.avgBytes = avgBytes;
		this.random = new Random();
		this.validKeys = new ArrayList<Number160>();
		this.routingConfig = new RoutingConfiguration(5, 1, 1);
		this.requestConfig = new RequestP2PConfiguration(1, 1, 0);
		this.putStats = new DHTQueryStatistics();
		this.getStats = new DHTQueryStatistics();
		this.rmvStats = new DHTQueryStatistics();
	}

	/**
	 * Start put / get / remove random keys
	 */
	public void queryRandom() throws IOException, ClassNotFoundException {
		sleep();
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
			
			printStats();
		}
	}

	/**
	 * Start put / get / remove specific key
	 */
	public void querySpecific(Number640 key) throws IOException, ClassNotFoundException {
		sleep();
		while (true) {
			put(key);
			sleep();

			get(key);
			sleep();

			remove(key);
			sleep();
			
			printStats();
		}
	}
	
	private void printStats() {
		StringBuilder sb = new StringBuilder("*************************\n");
		sb.append("Stats of peer ").append(peerDHT.peer().peerID()).append(": \n");
		sb.append("PUT:  count: ").append(putStats.getCount()).append(" | avgtime: ").append(putStats.getAverageTime()).append("ms | success: ").append(putStats.getSuccessRate()).append("\n");
		sb.append("GET:  count: ").append(putStats.getCount()).append(" | avgtime: ").append(getStats.getAverageTime()).append("ms | success: ").append(getStats.getSuccessRate()).append("\n");
		sb.append("RMV:  count: ").append(putStats.getCount()).append(" | avgtime: ").append(rmvStats.getAverageTime()).append("ms | success: ").append(rmvStats.getSuccessRate());
		System.out.println(sb.toString());
	}

	private void put(Number160 key) {
		put(new Number640(key, Number160.ZERO, Number160.ZERO, Number160.ZERO));
		validKeys.add(key);
	}

	public boolean put(Number640 key) {
		Data data = generateRandomData();
		putStats.start();
		FuturePut futurePut = peerDHT.put(key.locationKey()).domainKey(key.domainKey()).versionKey(key.versionKey())
				.data(key.contentKey(), data).routingConfiguration(routingConfig).requestP2PConfiguration(requestConfig)
				.start().awaitUninterruptibly();
		putStats.finished(futurePut.isSuccess());
		
		LOG.debug("Put of {} bytes is success = {}. Reason: {}", data.length(), futurePut.isSuccess(),
				futurePut.failedReason());
		return futurePut.isSuccess();
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
		getStats.start();
		FutureGet futureGet = peerDHT.get(key.locationKey()).contentKey(key.contentKey()).domainKey(key.domainKey())
				.versionKey(key.versionKey()).routingConfiguration(routingConfig).requestP2PConfiguration(requestConfig)
				.start().awaitUninterruptibly();
		getStats.finished(futureGet.isSuccess());
		LOG.debug("Get is success {}. Reason: {}", futureGet.isSuccess(), futureGet.failedReason());

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
			LOG.warn("Could not remove object {}", key);
		}
	}

	public boolean remove(Number640 key) {
		rmvStats.start();
		FutureRemove remove = peerDHT.remove(key.locationKey()).contentKey(key.contentKey()).domainKey(key.domainKey())
				.versionKey(key.versionKey()).routingConfiguration(routingConfig).requestP2PConfiguration(requestConfig)
				.start().awaitUninterruptibly();
		rmvStats.finished(remove.isSuccess());
		LOG.debug("Remove is success {}. Reason: {}", remove.isSuccess(), remove.failedReason());

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
	
	@Override
	public String toString() {
		return "Query-Peer " + peerDHT.peerID();
	}
}
