package net.tomp2p.p2p;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.Before;
import org.junit.Test;

public class TestNestedCall {

	private Peer seed;
	private Peer peer;
	private ConfigurationGet cg;
	private ConfigurationStore cs;
	private final Random rnd = new SecureRandom();

	@Before
	public void setUp() throws Exception {
		final int seedPort = 5001;
		seed = new Peer(new Number160(rnd));
		try {
			seed.listen(seedPort, seedPort);
		} catch (Exception e) {
			e.printStackTrace();
		}
		peer = new Peer(new Number160(rnd));
		peer.listen(seed);

		final int maxSuccess = 10;
		final int parallel = 1;
		final int maxFailure = 3;
		final int parallelDiff = 0;
		RoutingConfiguration rc = new RoutingConfiguration(1, 0, maxSuccess, parallel);
		RequestP2PConfiguration pc = new RequestP2PConfiguration(2, maxFailure, parallelDiff);

		cg = Configurations.defaultGetConfiguration();
		cg.setRoutingConfiguration(rc);
		cg.setRequestP2PConfiguration(pc);

		cs = Configurations.defaultStoreConfiguration();
		cs.setRoutingConfiguration(rc);
		cs.setRequestP2PConfiguration(pc);
	}

	@Test
	public void test() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(2);
		final byte[] key = "foo".getBytes();
		final byte[] value = "bla".getBytes();
		final byte[] otherValue = "foobla".getBytes();
		putIfAbsent(key, value, 60, new Callback<Boolean>() {
			@Override
			public void onMessage(final Boolean result) {
				assertTrue("The put should succeed, given this key is new", result);
				latch.countDown();
				// try another put for the same key (that should fail)
				putIfAbsent(key, otherValue, 60, new Callback<Boolean>() {
					@Override
					public void onMessage(final Boolean res) {
						assertFalse("The put should not have happened given a value already exists", res);
						latch.countDown();
					}
				});
			}
		});
		assertTrue("Tests did not complete in time", latch.await(500, TimeUnit.MILLISECONDS));
	}

	public void putIfAbsent(final byte[] key, final byte[] value, final int timeout, final Callback<Boolean> callback) {

		final Number160 tomKey = new Number160(key);
		peer.get(tomKey, cg).addListener(new BaseFutureAdapter<BaseFuture>() {
			@Override
			public void operationComplete(final BaseFuture result) throws Exception {
				if (result.isSuccess()) {
					// abort put operation given that the key exists
					System.out.println("Put failed because a key exists");
					callback.onMessage(false);
				} else if (result.isFailed()) {
					Data data = new Data(value);
					data.setTTLSeconds(timeout);
					peer.put(tomKey, data, cs).addListener(new BaseFutureListener<BaseFuture>() {
						@Override
						public void operationComplete(BaseFuture res) throws Exception {
							if (res.isSuccess()) {
								System.out.println("Put if absent succeeded");
								callback.onMessage(true);
							} else if (res.isFailed()) {
								System.out.println("Put if absent failed: " + res.getFailedReason());
								callback.onMessage(false);
							}

						}

						@Override
						public void exceptionCaught(Throwable t) throws Exception {
							throw new IllegalStateException(t);
						}
					});
				}
			}
		});
	}

	public interface Callback<T> {
		void onMessage(T data);
	}
}
