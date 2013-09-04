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
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNestedCall {
    private Peer seed;

    private Peer peer;

    private RoutingConfiguration rc;

    private RequestP2PConfiguration pc;

    private final Random rnd = new SecureRandom();

    @Before
    public void setUp() throws Exception {
        System.err.println("setup");
        final int seedPort = 5001;
        seed = new PeerMaker(new Number160(rnd)).ports(seedPort).makeAndListen();
        peer = new PeerMaker(new Number160(rnd)).masterPeer(seed).makeAndListen();
        final int maxSuccess = 10;
        final int parallel = 1;
        final int maxFailure = 3;
        final int parallelDiff = 0;
        rc = new RoutingConfiguration(1, 0, maxSuccess, parallel);
        pc = new RequestP2PConfiguration(2, maxFailure, parallelDiff);
    }

    @After
    public void tearDown() throws InterruptedException {
        System.err.println("teardown");
        seed.shutdown().awaitListenersUninterruptibly();
        peer.shutdown().awaitListenersUninterruptibly();
    }

    @Test
    public void testSuccess() throws InterruptedException {
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

    @Test
    public void testLimit() throws InterruptedException {
        final byte[] key = "foo".getBytes();
        final byte[] value = "bla".getBytes();
        final byte[] otherValue = "foobla".getBytes();
        for (int i = 0; i < 1000; i++) {
            putIfAbsent(key, value, 60, new Callback<Boolean>() {
                @Override
                public void onMessage(final Boolean result) {
                    // try another put for the same key (that should fail)
                    putIfAbsent(key, otherValue, 60, new Callback<Boolean>() {
                        @Override
                        public void onMessage(final Boolean res) {
                            // do nothing
                        }
                    });
                }
            });
        }
        System.err.println("done");
    }

    public void putIfAbsent(final byte[] key, final byte[] value, final int timeout, final Callback<Boolean> callback) {
        final Number160 tomKey = new Number160(key);
        BaseFutureAdapter<BaseFuture> adapter = new BaseFutureAdapter<BaseFuture>() {
            @Override
            public void operationComplete(final BaseFuture result) throws Exception {
                if (result.isSuccess()) {
                    // abort put operation given that the key exists
                    System.out.println("Put failed because a key exists");
                    callback.onMessage(false);
                } else if (result.isFailed()) {
                    Data data = new Data(value);
                    data.ttlSeconds(timeout);
                    BaseFutureListener<BaseFuture> listener = new BaseFutureListener<BaseFuture>() {
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
                    };
                    peer.put(tomKey).setData(data).setRequestP2PConfiguration(pc).setRoutingConfiguration(rc).start()
                            .addListener(listener);
                }
            }
        };
        peer.get(tomKey).setRequestP2PConfiguration(pc).setRoutingConfiguration(rc).start().addListener(adapter);
    }

    public interface Callback<T> {
        void onMessage(T data);
    }
}
