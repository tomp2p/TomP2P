package net.tomp2p.examples.relay;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.dht.FuturePut;
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
public class PeriodicQueryNode {

	private static final Logger LOG = LoggerFactory.getLogger(PeriodicQueryNode.class);

	private final Random random;
	private final RoutingConfiguration routingConfig;
	private final RequestP2PConfiguration requestConfig;
	private final PeerDHT peerDHT;
	
	private final DHTQueryStatistics putStats;

	private final long intervalMS;
	private final long durationMS;
	private final int numBytes;

	public PeriodicQueryNode(PeerDHT peerDHT, long intervalMS, long durationMS, int numBytes) {
		this.peerDHT = peerDHT;
		this.intervalMS = intervalMS;
		this.durationMS = durationMS;
		this.numBytes = numBytes;
		this.random = new Random();
		this.routingConfig = new RoutingConfiguration(5, 1, 1);
		this.requestConfig = new RequestP2PConfiguration(1, 1, 0);
		this.putStats = new DHTQueryStatistics();
	}
	
	public void start(Number160 locationKey) {
		Timer timer = new Timer();
		Putter putter = new Putter(locationKey);
		timer.scheduleAtFixedRate(putter, 0, intervalMS);
		
		try {
			Thread.sleep(durationMS);
		} catch (InterruptedException e) {
			LOG.error("Cannot sleep", e);
		}
		
		timer.cancel();
		timer.purge();
		
		long timeout = peerDHT.peer().connectionBean().channelServer().channelServerConfiguration().slowResponseTimeoutSeconds() * 1000;
		try {
			Thread.sleep((long) (timeout * 1.2));
		} catch (InterruptedException e) {
			LOG.error("Cannot wait for last threads", e);
		}
		
		// mark all running threads as failed in orther to have proper stats
		for (int i = 0; i < putter.getRunning(); i++) {
			putStats.report(timeout, false);
		}
		printStats();
	}
	
	private void printStats() {
		StringBuilder sb = new StringBuilder("*************************\n");
		sb.append("Stats of peer ").append(peerDHT.peer().peerID()).append(": \n");
		sb.append("Duration: ").append(durationMS).append("ms").append(" | Interval: ").append(intervalMS).append("ms | Data size: ").append(numBytes).append("bytes\n");
		sb.append("PUT:  count: ").append(putStats.getCount()).append(" | avgtime: ").append(putStats.getAverageTime()).append("ms | success: ").append(putStats.getSuccessRate()).append("\n");
		System.out.println(sb.toString());
	}
	
	private class Putter extends TimerTask {
		
		private final Number160 locationKey;
		private final AtomicInteger running;

		public Putter(Number160 locationKey) {
			this.locationKey = locationKey;
			this.running = new AtomicInteger();
		}
		
		@Override
		public void run() {
			new Thread(new Runnable() {
				@Override
				public void run() {
					running.incrementAndGet();
					put(new Number640(locationKey, Number160.ZERO, new Number160(random), Number160.ZERO));
					running.decrementAndGet();
				}
			}).start();
		}
		
		public int getRunning() {
			return running.get();
		}
	}
	

	public boolean put(Number640 key) {
		Data data = generateRandomData();
		long startTime = System.currentTimeMillis();
		FuturePut futurePut = peerDHT.put(key.locationKey()).domainKey(key.domainKey()).versionKey(key.versionKey())
				.data(key.contentKey(), data).routingConfiguration(routingConfig).requestP2PConfiguration(requestConfig)
				.start().awaitUninterruptibly();
		putStats.report(System.currentTimeMillis() - startTime, futurePut.isSuccess());
		
		LOG.debug("Put of {} bytes is success = {}. Reason: {}", data.length(), futurePut.isSuccess(),
				futurePut.failedReason());
		return futurePut.isSuccess();
	}

	private Data generateRandomData() {
		byte[] data = new byte[numBytes];
		random.nextBytes(data);
		return new Data(data);
	}
	
	@Override
	public String toString() {
		return "Query-Peer " + peerDHT.peerID();
	}
}
