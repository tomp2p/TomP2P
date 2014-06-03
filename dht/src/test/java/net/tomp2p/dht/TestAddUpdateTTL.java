package net.tomp2p.dht;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAddUpdateTTL {

    private Random rnd = new Random(42L);

    private static final int DHT_UPDATE_INTERVAL = 1;

    private final ConcurrentMap<byte[], List<byte[]>> keyValueStore = new ConcurrentHashMap<byte[], List<byte[]>>();

    private Peer seed;

    private PeerDHT peer;

    private Timer timer1;

    private Timer timer2;

    private CountDownLatch latch = new CountDownLatch(21); // wait for 21
                                                           // results for
                                                           // stopping the test

    private boolean testSuccess = true;

    @Before
    public void setup() throws Exception {
        keyValueStore.put("foo1".getBytes(), Arrays.asList("bla1a".getBytes(), "bla1b".getBytes()));
        keyValueStore.put("foo2".getBytes(), Arrays.asList("bla2a".getBytes()));
        keyValueStore.put("foo3".getBytes(), Arrays.asList("bla3a".getBytes(), "bla3b".getBytes(), "bla3c".getBytes()));
        seed = new PeerBuilder(new Number160(rnd)).ports(5002).start();
    }

    @Test
    public void test() throws Exception {
        peer = createAndAttachRemotePeer();
        Thread.sleep(1000);
        for (byte[] key : keyValueStore.keySet()) {
            for (byte[] value : keyValueStore.get(key)) {
                add(peer, key, value);
            }
        }
        Thread.sleep(1000);
        timer1 = new Timer("DHT Update Service");
        timer1.schedule(new DhtUpdater(peer), Calendar.getInstance().getTime(), DHT_UPDATE_INTERVAL * 1000);

        timer2 = new Timer("DHT Stats Service");
        timer2.schedule(new DhtStats(peer), Calendar.getInstance().getTime(), 1000);

        latch.await(1, TimeUnit.MINUTES);
        Assert.assertTrue(testSuccess);
    }

    @After
    public void cleanup() {
        timer1.cancel();
        timer2.cancel();
        seed.shutdown().awaitListenersUninterruptibly();
        peer.peer().shutdown().awaitListenersUninterruptibly();
    }

    public PeerDHT createAndAttachRemotePeer() {
        final Peer peer;
        try {
            peer = new PeerBuilder(new Number160(rnd)).ports(5003).start();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
            return null;
        }
        final FutureBootstrap fb = peer.bootstrap().broadcast().ports(seed.peerAddress().udpPort()).start();
        fb.awaitUninterruptibly();
        peer.discover().peerAddress(fb.bootstrapTo().iterator().next()).start();
        fb.addListener(new BaseFutureListener<BaseFuture>() {
            @Override
            public void operationComplete(BaseFuture future) throws Exception {
                Collection<PeerAddress> addresses = fb.bootstrapTo();
                if (addresses != null && !addresses.isEmpty()) {
                    peer.discover().peerAddress(addresses.iterator().next()).start().awaitUninterruptibly();
                } else {
                    Assert.assertTrue("Unable to boostrap to peers in the network", false);
                }
            }

            @Override
            public void exceptionCaught(Throwable t) throws Exception {
                t.fillInStackTrace();
            }
        });
        return new PeerDHT(peer);
    }

    private void add(PeerDHT peer, byte[] key, byte[] value) throws InterruptedException {
        Data data = new Data(value);
        data.ttlSeconds(3);
        peer.add(new Number160(key)).data(data).routingConfiguration(new RoutingConfiguration(1, 0, 10, 1))
                .requestP2PConfiguration(new RequestP2PConfiguration(3, 5, 0)).start()
                .addListener(new BaseFutureAdapter<FuturePut>() {
                    @Override
                    public void operationComplete(final FuturePut future) throws Exception {
                        System.out.println(future.rawResult());
                    }
                });
    }

    class DhtUpdater extends TimerTask {

        private PeerDHT peer;

        public DhtUpdater(PeerDHT peer) {
            this.peer = peer;
        }

        @Override
        public void run() {
            System.out.println("updating data");
            for (byte[] key : keyValueStore.keySet()) {
                for (byte[] value : keyValueStore.get(key)) {
                    try {
                        add(peer, key, value);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    class DhtStats extends TimerTask {

        private PeerDHT peer;

        public DhtStats(PeerDHT peer) {
            this.peer = peer;
        }

        @Override
        public void run() {
            for (final byte[] key : keyValueStore.keySet()) {
                peer.get(new Number160(key)).all().start().addListener(new BaseFutureAdapter<FutureGet>() {
                    @Override
                    public void operationComplete(final FutureGet future) throws Exception {
                        if (!future.isSuccess()) {
                            testSuccess = false;
                            System.out.println("getAll failed " + future.failedReason());
                        } else {
                            System.out.println("getAll result: " + future.dataMap().size());
                        }
                        latch.countDown();
                    }
                });
            }
        }
    }

}