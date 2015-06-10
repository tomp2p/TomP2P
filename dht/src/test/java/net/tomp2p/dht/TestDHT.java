package net.tomp2p.dht;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Buffer;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DataBuffer;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDHT {
	final private static Random rnd = new Random(42L);
	private static final Logger LOG = LoggerFactory.getLogger(TestDHT.class);
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };
	
	@Test
	public void testPutBig() throws Exception {
		PeerDHT[] peers = null;
		try {
			peers = testPutBig1("1");
			//Thread.sleep(5 * 1000);
			//master = testPutBig1("2");
			//Thread.sleep(5 * 1000);
			//master = testPutBig1("3");
			//Thread.sleep(5 * 1000);
			//Thread.sleep(Integer.MAX_VALUE);
		} finally {
			for (PeerDHT peerDHT:peers) {
				if(peerDHT!=null) {
					peerDHT.shutdown().await();
				}
			}
			System.out.println("done");
		}
	}
	
	private PeerDHT[] testPutBig1(String key) throws Exception {
		
		
			Peer[] peers = UtilsDHT2.createRealNodes(10, rnd, 4001, new AutomaticFuture() {
				@Override
				public void futureCreated(BaseFuture future) {					}
			});
			
			PeerDHT[] peers2 = new PeerDHT[10];
			for(int i=0;i<peers.length;i++) {
				peers2[i] = new PeerBuilderDHT(peers[i]).start();
			}
			UtilsDHT2.perfectRouting(peers2);
			Data data1 = new Data(new byte[10*1024*1024]);
			RequestP2PConfiguration rp = new RequestP2PConfiguration(1, 0, 0);
			//for(int i=0;i<10000;i++) {
			PutBuilder pb = peers2[0].put(Number160.createHash(key)).requestP2PConfiguration(rp).data(data1);
			FuturePut futurePut = pb.start();
			futurePut.awaitUninterruptibly();
			
			Assert.assertEquals(true, futurePut.isSuccess());
			
			System.out.println("stored on "+futurePut.result());
			
			FutureRemove fr = peers2[0].remove(Number160.createHash(key)).start().awaitUninterruptibly();
			System.out.println("removed from "+fr.result());
			//}
			return peers2;
			
			//
		

	}

	@Test
	public void testPutTwo() throws Exception {
		PeerDHT master = null;
		try {
			PeerDHT[] peers = UtilsDHT2.createNodes(10, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			final Data data1 = new Data(new byte[1]);
			data1.ttlSeconds(3);
			FuturePut futurePut = master.put(Number160.createHash("test")).data(data1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertEquals(true, futurePut.isSuccess());
			FutureGet futureGet = peers[1].get(Number160.createHash("test")).start();
			futureGet.awaitUninterruptibly();
			Assert.assertEquals(true, futureGet.isSuccess());
			// LOG.error("done");

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}

	}

	@Test
	public void testPutVersion() throws Exception {
		final Random rnd = new Random(42L);
		PeerDHT master = null;
		try {
			PeerDHT[] peers = UtilsDHT2.createNodes(10, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			final Data data1 = new Data(new byte[1]);
			data1.ttlSeconds(3);
			FuturePut futurePut = master.put(Number160.createHash("test")).versionKey(Number160.MAX_VALUE)
			        .data(data1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertEquals(true, futurePut.isSuccess());
			//
			Map<Number640, Data> map = peers[0].storageLayer().get();
			Assert.assertEquals(Number160.MAX_VALUE, map.entrySet().iterator().next().getKey().versionKey());
			LOG.error("done");
		} finally {
			if (master != null) {
				master.shutdown().awaitUninterruptibly();
				master.shutdown().awaitListenersUninterruptibly();
			}
		}
	}

	@Test
	public void testPutPerforomance() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);

			for (int i = 0; i < 500; i++) {
				//long start = System.currentTimeMillis();
				FuturePut fp = peers[444].put(Number160.createHash("1")).data(new Data("test")).start();
				fp.awaitUninterruptibly();
				fp.futureRequests().awaitUninterruptibly();
				
				//long stop = System.currentTimeMillis();
				//System.out.println("Test " + fp.failedReason() + " / " + (stop - start) + "ms");
				Assert.assertEquals(true, fp.isSuccess());
			}

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPut() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fp = peers[444].put(peers[30].peerID())
			        .data(Number160.createHash("test"), new Number160(5), data).requestP2PConfiguration(pc)
			        .routingConfiguration(rc).start();

			fp.awaitUninterruptibly();
			fp.futureRequests().awaitUninterruptibly();
			System.out.println("Test " + fp.failedReason());
			Assert.assertEquals(true, fp.isSuccess());
			peers[30].peerBean().peerMap();
			// search top 3
			TreeMap<PeerAddress, Integer> tmp = new TreeMap<PeerAddress, Integer>(PeerMap.createXORAddressComparator(peers[30]
                    .peer().peerID()));
			int i = 0;
			for (PeerDHT node : peers) {
				tmp.put(node.peer().peerAddress(), i);
				i++;
			}
			Entry<PeerAddress, Integer> e = tmp.pollFirstEntry();
			System.out.println("1 (" + e.getValue() + ")" + e.getKey());
			Assert.assertEquals(peers[e.getValue()].peerAddress(), peers[30].peerAddress());
			testForArray(peers[e.getValue()], peers[30].peerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.out.println("2 (" + e.getValue() + ")" + e.getKey());
			testForArray(peers[e.getValue()], peers[30].peerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.out.println("3 " + e.getKey());
			testForArray(peers[e.getValue()], peers[30].peerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.out.println("4 " + e.getKey());
			testForArray(peers[e.getValue()], peers[30].peerID(), false);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGetAlone() throws Exception {
		PeerDHT master = null;
		try {
			master = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4001).start()).start();
			FuturePut fdht = master.put(Number160.ONE).data(new Data("hallo")).start();
			fdht.awaitUninterruptibly();
			fdht.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			FutureGet fdht2 = master.get(Number160.ONE).start();
			fdht2.awaitUninterruptibly();
			System.out.println(fdht2.failedReason());
			Assert.assertEquals(true, fdht2.isSuccess());
			Data tmp = fdht2.data();
			Assert.assertEquals("hallo", tmp.object().toString());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutTimeout() throws Exception {
		PeerDHT master = null;
		try {
			Peer pmaster = new PeerBuilder(new Number160(rnd)).ports(4001).start();
			master = new PeerBuilderDHT(pmaster).storage(new StorageMemory(1)).start();
			Data data = new Data("hallo");
			data.ttlSeconds(1);
			FuturePut fdht = master.put(Number160.ONE).data(data).start();
			fdht.awaitUninterruptibly();
			fdht.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			Thread.sleep(3000);
			FutureGet fdht2 = master.get(Number160.ONE).start();
			fdht2.awaitUninterruptibly();
			System.out.println(fdht2.failedReason());
			//we get an empty result, this means it did not fail, just it did not return anything
			Assert.assertEquals(true, fdht2.isSuccess());
			Data tmp = fdht2.data();
			Assert.assertNull(tmp);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPut2() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(500, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(0, 0, 1);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(1, 0, 0);
			FuturePut fdht = peers[444].put(peers[30].peerID()).data(new Number160(5), data)
			        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
			        .requestP2PConfiguration(pc).start();
			fdht.awaitUninterruptibly();
			fdht.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			peers[30].peerBean().peerMap();
			// search top 3
			TreeMap<PeerAddress, Integer> tmp = new TreeMap<PeerAddress, Integer>(PeerMap.createXORAddressComparator(peers[30]
                    .peerID()));
			int i = 0;
			for (PeerDHT node : peers) {
				tmp.put(node.peerAddress(), i);
				i++;
			}
			Entry<PeerAddress, Integer> e = tmp.pollFirstEntry();
			Assert.assertEquals(peers[e.getValue()].peerAddress(), peers[30].peerAddress());
			testForArray(peers[e.getValue()], peers[30].peerID(), true);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].peerID(), false);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].peerID(), false);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].peerID(), false);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGet() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);
			FuturePut fput = peers[444].put(peers[30].peerID()).data(new Number160(5), data)
			        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
			        .requestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(0, 0, 10, 1);
			pc = new RequestP2PConfiguration(1, 0, 0);

			FutureGet fget = peers[555].get(peers[30].peerID()).domainKey(Number160.createHash("test"))
			        .contentKey(new Number160(5)).routingConfiguration(rc).requestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(1, fget.rawData().size());
			Assert.assertEquals(true, fget.isMinReached());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	@Test
	public void testPutGetRelease() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);
			FuturePut fput = peers[444].put(peers[30].peerID()).data(new Number160(5), data)
			        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
			        .requestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(0, 0, 10, 1);
			pc = new RequestP2PConfiguration(1, 0, 0);

			FutureGet fget = peers[555].get(peers[30].peerID()).domainKey(Number160.createHash("test"))
			        .contentKey(new Number160(5)).routingConfiguration(rc).requestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(true, fget.rawData().values().iterator().next().values().iterator().next().isHeapBuffer());
			Assert.assertEquals(true, fget.requests().get(0).responseMessage().dataMap(0).dataMap().values().iterator().next().isHeapBuffer());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutConvert() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);
			NavigableMap<Number160, Data> tmp = new TreeMap<Number160, Data>();
			tmp.put(new Number160(5), data);
			FuturePut fput = peers[444].put(peers[30].peerID()).dataMapContent(tmp)
			        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
			        .requestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			System.out.println(fput.failedReason());
			Assert.assertEquals(true, fput.isSuccess());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGet2() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(1000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);

			FuturePut fput = peers[444].put(peers[30].peerID()).data(new Number160(5), data)
			        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
			        .requestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);

			FutureGet fget = peers[555].get(peers[30].peerID()).domainKey(Number160.createHash("test"))
			        .contentKey(new Number160(5)).routingConfiguration(rc).requestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(3, fget.rawData().size());
			Assert.assertEquals(true, fget.isMinReached());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGet3() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = peers[444].put(peers[30].peerID()).data(new Number160(5), data)
			        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
			        .requestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(1, 0, 10, 1);
			pc = new RequestP2PConfiguration(1, 0, 0);
			for (int i = 0; i < 1000; i++) {
				FutureGet fget = peers[100 + i].get(peers[30].peerID()).domainKey(Number160.createHash("test"))
				        .contentKey(new Number160(5)).routingConfiguration(rc).requestP2PConfiguration(pc)
				        .start();
				fget.awaitUninterruptibly();
				Assert.assertEquals(true, fget.isSuccess());
				Assert.assertEquals(1, fget.rawData().size());
				Assert.assertEquals(true, fget.isMinReached());
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGetRemove() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = peers[444].put(peers[30].peerID()).domainKey(Number160.createHash("test"))
			        .data(new Number160(5), data).routingConfiguration(rc).requestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(3, 0, 0);
			FutureRemove frem = peers[222].remove(peers[30].peerID()).domainKey(Number160.createHash("test"))
			        .contentKey(new Number160(5)).routingConfiguration(rc).requestP2PConfiguration(pc).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			Assert.assertEquals(3, frem.rawKeys().size());

			FutureGet fget = peers[555].get(peers[30].peerID()).domainKey(Number160.createHash("test"))
			        .contentKey(new Number160(5)).routingConfiguration(rc).requestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isEmpty());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGetRemove2() throws Exception {
		PeerDHT master = null;
		try {
			// rnd.setSeed(253406013991563L);
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = peers[444].put(peers[30].peerID()).data(new Number160(5), data)
			        .domainKey(Number160.createHash("test")).routingConfiguration(rc)
			        .requestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(3, fput.rawResult().size());
			Assert.assertEquals(true, fput.isSuccess());
			System.out.println("remove");
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(3, 0, 0);

			FutureRemove frem = peers[222].remove(peers[30].peerID()).returnResults()
			        .domainKey(Number160.createHash("test")).contentKey(new Number160(5))
			        .routingConfiguration(rc).requestP2PConfiguration(pc).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			Assert.assertEquals(3, frem.rawData().size());
			System.out.println("get");
			rc = new RoutingConfiguration(4, 0, 0, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);

			FutureGet fget = peers[555].get(peers[30].peerID()).domainKey(Number160.createHash("test"))
			        .contentKey(new Number160(5)).routingConfiguration(rc).requestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertTrue(fget.isEmpty());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testDirect() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(1000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			final AtomicInteger ai = new AtomicInteger(0);
			for (int i = 0; i < peers.length; i++) {
				peers[i].peer().objectDataReply(new ObjectDataReply() {
					@Override
					public Object reply(PeerAddress sender, Object request) throws Exception {
						ai.incrementAndGet();
						return "ja";
					}
				});
			}
			// do testing
			FutureSend fdir = peers[400].send(new Number160(rnd)).object("hallo").start();
			fdir.awaitUninterruptibly();
			System.out.println(fdir.failedReason());
			Assert.assertEquals(true, fdir.isSuccess());
			Assert.assertEquals(true, ai.get() >= 3 && ai.get() <= 6);
			System.out.println("called: " + ai.get());
			Assert.assertEquals("ja", fdir.object());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testAddListGet() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(200, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo1";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			FuturePut fput = peers[30].add(nr).data(data1).list(true).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].add(nr).data(data2).list(true).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			FutureGet fget = peers[77].get(nr).all().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			// majority voting with getDataMap is not possible since we create
			// random content key on the recipient
			Assert.assertEquals(2, fget.rawData().values().iterator().next().values().size());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testAddGet() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(200, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			FuturePut fput = peers[30].add(nr).data(data1).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].add(nr).data(data2).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			FutureGet fget = peers[77].get(nr).all().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testDigest() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(200, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			String toStore3 = "hallo3";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			Data data3 = new Data(toStore3.getBytes());
			FuturePut fput = peers[30].add(nr).data(data1).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].add(nr).data(data2).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			fput = peers[51].add(nr).data(data3).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore3 + " (" + fput.isSuccess() + ")");
			FutureDigest fget = peers[77].digest(nr).all().start();
			fget.awaitUninterruptibly();
			System.out.println(fget.failedReason());
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(3, fget.digest().keyDigest().size());
			Number160 test = new Number160("0x37bb570100c9f5445b534757ebc613a32df3836d");
			List<Number160> test2 = new ArrayList<Number160>();
			test2.add(test);
			fget = peers[67].digest(nr).contentKeys(test2).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(1, fget.digest().keyDigest().size());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	@Test
	public void removeTestLoop() throws IOException, ClassNotFoundException {
		for(int i=0;i<100;i++) {
			System.out.println("removeTestLoop() call nr "+i);
			removeTest();
		}
	}

	@Test
	public void removeTest() throws IOException, ClassNotFoundException {
		PeerDHT p1 = null;
		PeerDHT p2 = null;
		try {
			p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(5000).start()).start();
			p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer())
		        .start()).start();

			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();

			String locationKey = "location";
			String contentKey = "content";

			String data = "testme";
			// data.generateVersionKey();

			p2.put(Number160.createHash(locationKey)).data(Number160.createHash(contentKey), new Data(data))
		        .versionKey(Number160.ONE).start().awaitUninterruptibly();

			FutureRemove futureRemove = p1.remove(Number160.createHash(locationKey)).domainKey(Number160.ZERO)
					.contentKey(Number160.createHash(contentKey)).versionKey(Number160.ONE).start();
			futureRemove.awaitUninterruptibly();

			FutureDigest futureDigest = p1.digest(Number160.createHash(locationKey)).domainKey(Number160.ZERO)
		        .contentKey(Number160.createHash(contentKey)).versionKey(Number160.ONE).start();
			futureDigest.awaitUninterruptibly();

			Assert.assertTrue(futureDigest.digest().keyDigest().isEmpty());
		} finally {
			if(p1 != null) {
				p1.shutdown().awaitUninterruptibly();
			}
			if(p2 != null) {
				p2.shutdown().awaitUninterruptibly();
			}
		}
	}

	@Test
	public void testDigest2() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(200, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			String toStore3 = "hallo3";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			Data data3 = new Data(toStore3.getBytes());
			Number160 key1 = new Number160(1);
			Number160 key2 = new Number160(2);
			Number160 key3 = new Number160(3);
			FuturePut fput = peers[30].put(nr).data(key1, data1).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].put(nr).data(key2, data2).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			fput = peers[51].put(nr).data(key3, data3).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore3 + " (" + fput.isSuccess() + ")");
			Number640 from = new Number640(nr, Number160.ZERO, Number160.ZERO, Number160.ZERO);
			Number640 to = new Number640(nr, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE);
			FutureDigest fget = peers[77].digest(nr).from(from).to(to).returnNr(1).start();
			fget.awaitUninterruptibly();
			System.out.println(fget.failedReason());
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(1, fget.digest().keyDigest().size());
			Assert.assertEquals(key1, fget.digest().keyDigest().keySet().iterator().next().contentKey());
			fget = peers[67].digest(nr).from(from).to(to).returnNr(1).descending().start();
			fget.awaitUninterruptibly();
			System.out.println(fget.failedReason());
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(1, fget.digest().keyDigest().size());
			Assert.assertEquals(key3, fget.digest().keyDigest().keySet().iterator().next().contentKey());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testDigest3() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(200, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);

			// initialize test data
			Number160 lKey = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			String toStore3 = "hallo3";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			Data data3 = new Data(toStore3.getBytes());
			Number160 ckey = new Number160(rnd);
			data1.addBasedOn(Number160.ONE);
			Number160 versionKey1 = new Number160(1, data1.hash());
			data2.addBasedOn(versionKey1);
			Number160 versionKey2 = new Number160(2, data2.hash());
			data3.addBasedOn(versionKey2);
			Number160 versionKey3 = new Number160(3, data3.hash());
			
			// put test data
			FuturePut fput = peers[30].put(lKey).data(ckey, data1, versionKey1).start();
			fput.awaitUninterruptibly();
			fput = peers[50].put(lKey).data(ckey, data2, versionKey2).start();
			fput.awaitUninterruptibly();
			fput = peers[51].put(lKey).data(ckey, data3, versionKey3).start();
			fput.awaitUninterruptibly();

			// get digest
			FutureDigest fget = peers[77].digest(lKey).all().start();
			fget.awaitUninterruptibly();
			DigestResult dr = fget.digest();
			NavigableMap<Number640, Collection<Number160>> map = dr.keyDigest();
			
			// verify fetched digest
			Entry<Number640, Collection<Number160>> e1 = map.pollFirstEntry();
			Assert.assertEquals(Number160.ONE, e1.getValue().iterator().next());
			Assert.assertEquals(new Number640(lKey, Number160.ZERO, ckey, versionKey1), e1.getKey());
			Entry<Number640, Collection<Number160>> e2 = map.pollFirstEntry();
			Assert.assertEquals(versionKey1, e2.getValue().iterator().next());
			Assert.assertEquals(new Number640(lKey, Number160.ZERO, ckey, versionKey2), e2.getKey());
			Entry<Number640, Collection<Number160>> e3 = map.pollFirstEntry();
			Assert.assertEquals(versionKey2, e3.getValue().iterator().next());
			Assert.assertEquals(new Number640(lKey, Number160.ZERO, ckey, versionKey3), e3.getKey());

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testDigest4() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(100, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);

			// initialize test data
			Number160 lKey = new Number160(rnd);
			Number160 dKey = new Number160(rnd);
			Number160 ckey = new Number160(rnd);
			NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
			Number160 bKey = Number160.ONE;
			for (int i = 0; i < 10; i++) {
				Data data = new Data(UUID.randomUUID());
				data.addBasedOn(bKey);
				Number160 vKey = new Number160(i, data.hash());
				dataMap.put(new Number640(lKey, dKey, ckey, vKey), data);
				bKey = vKey;
			}

			// put test data
			for (Number640 key : dataMap.keySet()) {
				FuturePut fput = peers[rnd.nextInt(100)].put(lKey).domainKey(dKey)
						.data(ckey, dataMap.get(key)).versionKey(key.versionKey()).start();
				fput.awaitUninterruptibly();
			}

			// get digest
			FutureDigest fget = peers[rnd.nextInt(100)].digest(lKey)
					.from(new Number640(lKey, dKey, ckey, Number160.ZERO))
					.to(new Number640(lKey, dKey, ckey, Number160.MAX_VALUE)).start();
			fget.awaitUninterruptibly();
			DigestResult dr = fget.digest();
			NavigableMap<Number640, Collection<Number160>> fetchedDataMap = dr.keyDigest();

			// verify fetched digest
			Assert.assertEquals(dataMap.size(), fetchedDataMap.size());
			for (Number640 key : dataMap.keySet()) {
				Assert.assertTrue(fetchedDataMap.containsKey(key));
				Assert.assertEquals(dataMap.get(key).basedOnSet().iterator().next(), fetchedDataMap.get(key)
						.iterator().next());
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testData() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(200, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			ByteBuf c = Unpooled.buffer();
			c.writeInt(77);
			DataBuffer d = new DataBuffer(c);
			peers[50].peer().rawDataReply(new RawDataReply() {
				@Override
				public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) {
					System.out.println(requestBuffer.buffer().readInt());
					ByteBuf c = Unpooled.buffer();
					c.writeInt(88);
					Buffer ret = new Buffer(c);
					return ret;
				}
			});
			FutureDirect fd = master.peer().sendDirect(peers[50].peerAddress()).dataBuffer(d).start();
			fd.await();
			if (fd.buffer() == null) {
				System.out.println("damm");
				Assert.fail();
			}
			int read = fd.buffer().buffer().readInt();
			Assert.assertEquals(88, read);
			System.out.println("done");
			// for(FutureBootstrap fb:tmp)
			// fb.awaitUninterruptibly();
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testData2() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(200, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			ByteBuf c = Unpooled.buffer();
			c.writeInt(77);
			DataBuffer d = new DataBuffer(c);
			peers[50].peer().rawDataReply(new RawDataReply() {
				@Override
				public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) {
					System.out.println("got it");
					return requestBuffer;
				}
			});
			FutureDirect fd = master.peer().sendDirect(peers[50].peerAddress()).dataBuffer(d).start();
			fd.await();
			System.out.println("done1 " + fd.failedReason());
			Assert.assertEquals(true, fd.isSuccess());
			Assert.assertNull(fd.buffer());
			// int read = fd.getBuffer().readInt();
			// Assert.assertEquals(88, read);
			System.out.println("done2");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testObjectLoop() throws Exception {
		for (int i = 0; i < 1000; i++) {
			if(i%10==0) {
				System.out.println("nr: " + i);
			}
			testObject();
		}
	}

	@Test
	public void testObject() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(100, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			Data data1 = new Data(toStore1);
			Data data2 = new Data(toStore2);
			//System.out.println("begin add : ");
			FuturePut fput = peers[30].add(nr).data(data1).start();
			fput.awaitUninterruptibly();
			//System.out.println("stop added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].add(nr).data(data2).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			//System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			FutureGet fget = peers[77].get(nr).all().start();
			fget.awaitUninterruptibly();
			fget.futureRequests().awaitUninterruptibly();
			if (!fget.isSuccess())
				System.out.println(fget.failedReason());
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(2, fget.dataMap().size());
			//System.out.println("got it");
		} finally {
			if (master != null) {
				master.shutdown().awaitUninterruptibly();
				master.shutdown().awaitListenersUninterruptibly();
			}
		}
	}

	@Test
	public void testMaintenanceInit() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRoutingIndirect(peers);
			// do testing

			PeerStatistic peerStatatistic = master.peerBean().peerMap()
			        .nextForMaintenance(new ArrayList<PeerAddress>());
			Assert.assertNotEquals(master.peerAddress(), peerStatatistic.peerAddress());
			Thread.sleep(10000);
			System.out.println("DONE");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testAddGetPermits() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(2000, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			List<FuturePut> list = new ArrayList<FuturePut>();
			for (int i = 0; i < peers.length; i++) {
				String toStore1 = "hallo" + i;
				Data data1 = new Data(toStore1.getBytes());
				FuturePut fput = peers[i].add(nr).data(data1).start();
				list.add(fput);
			}
			for (FuturePut futureDHT : list) {
				futureDHT.awaitUninterruptibly();
				Assert.assertEquals(true, futureDHT.isSuccess());
			}
			System.out.println("DONE");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testhalt() throws Exception {
		PeerDHT master1 = null;
		PeerDHT master2 = null;
		PeerDHT master3 = null;
		try {
			master1 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4001).start()).start();
			master2 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4002).start()).start();
			master3 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4003).start()).start();
			// perfect routing
			master1.peerBean().peerMap().peerFound(master2.peerAddress(), null, null, null);
			master1.peerBean().peerMap().peerFound(master3.peerAddress(), null, null, null);
			master2.peerBean().peerMap().peerFound(master1.peerAddress(), null, null, null);
			master2.peerBean().peerMap().peerFound(master3.peerAddress(), null, null, null);
			master3.peerBean().peerMap().peerFound(master1.peerAddress(), null, null, null);
			master3.peerBean().peerMap().peerFound(master2.peerAddress(), null, null, null);
			Number160 id = master2.peerID();
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = master1.put(id).data(new Number160(5), data).domainKey(Number160.createHash("test"))
			        .requestP2PConfiguration(pc).routingConfiguration(rc).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			// Collection<Number160> tmp = new ArrayList<Number160>();
			// tmp.add(new Number160(5));

			//
			Assert.assertEquals(true, fput.isSuccess());
			// search top 3
			master2.shutdown().await();

			//

			FutureGet fget = master1.get(id).routingConfiguration(rc).requestP2PConfiguration(pc)
			        .domainKey(Number160.createHash("test")).contentKey(new Number160(5)).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());

			master1.peerBean().peerMap().peerFailed(master2.peerAddress(), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
			master3.peerBean().peerMap().peerFailed(master2.peerAddress(), new PeerException(AbortCause.SHUTDOWN, "shutdown"));
			master2 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).p2pId(1).ports(4002).start()).start();
			master1.peerBean().peerMap().peerFound(master2.peerAddress(), null, null, null);
			master3.peerBean().peerMap().peerFound(master2.peerAddress(), null, null, null);
			master2.peerBean().peerMap().peerFound(master1.peerAddress(), null, null, null);
			master2.peerBean().peerMap().peerFound(master3.peerAddress(), null, null, null);

			System.out.println("no more exceptions here!!");

			fget = master1.get(id).routingConfiguration(rc).requestP2PConfiguration(pc)
			        .domainKey(Number160.createHash("test")).contentKey(new Number160(5)).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());

		} finally {
			if (master1 != null) {
				master1.shutdown().await();
			}
			if (master2 != null) {
				master2.shutdown().await();
			}
			if (master3 != null) {
				master3.shutdown().await();
			}
		}
	}

	@Test
	public void testObjectSendExample() throws Exception {
		Peer p1 = null;
		Peer p2 = null;
		try {
			p1 = new PeerBuilder(new Number160(rnd)).ports(4001).start();
			p2 = new PeerBuilder(new Number160(rnd)).ports(4002).start();
			// attach reply handler
			p2.objectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					System.out.println("request [" + request + "]");
					return "world";
				}
			});
			FutureDirect futureData = p1.sendDirect(p2.peerAddress()).object("hello").start();
			futureData.awaitUninterruptibly();
			System.out.println("reply [" + futureData.object() + "]");
		} finally {
			if (p1 != null) {
				p1.shutdown().await();
			}
			if (p2 != null) {
				p2.shutdown().await();
			}

		}
	}

	@Test
	public void testObjectSend() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(500, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			for (int i = 0; i < peers.length; i++) {
				System.out.println("node " + i);
				peers[i].peer().objectDataReply(new ObjectDataReply() {
					@Override
					public Object reply(PeerAddress sender, Object request) throws Exception {
						return request;
					}
				});
				peers[i].peer().rawDataReply(new RawDataReply() {
					@Override
					public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception {
						return requestBuffer;
					}
				});
			}
			// do testing
			System.out.println("round start");
			Random rnd = new Random(42L);
			byte[] toStore1 = new byte[10 * 1024];
			for (int j = 0; j < 5; j++) {
				System.out.println("round " + j);
				for (int i = 0; i < peers.length - 1; i++) {
					send1(peers[rnd.nextInt(peers.length)], peers[rnd.nextInt(peers.length)], toStore1, 100);
					send2(peers[rnd.nextInt(peers.length)], peers[rnd.nextInt(peers.length)],
					        Unpooled.wrappedBuffer(toStore1), 100);
					System.out.println("round1 " + i);
				}
			}
			System.out.println("DONE");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testKeys() throws Exception {
		final Random rnd = new Random(42L);
		PeerDHT p1 = null;
		PeerDHT p2 = null;
		try {
			Number160 n1 = new Number160(rnd);
			Data d1 = new Data("hello");
			Data d2 = new Data("world!");
			// setup (step 1)
			p1 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4001).start()).start();
			FuturePut fput = p1.add(n1).data(d1).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			p2 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4002).start()).start();
			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
			// test (step 2)
			fput = p1.add(n1).data(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			FutureGet fget = p2.get(n1).all().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(2, fget.dataMap().size());
			// test (step 3)
			FutureRemove frem = p1.remove(n1).contentKey(d2.hash()).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			fget = p2.get(n1).all().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(1, fget.dataMap().size());
			// test (step 4)
			fput = p1.add(n1).data(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			fget = p2.get(n1).all().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(2, fget.dataMap().size());
			// test (remove all)
			frem = p1.remove(n1).contentKey(d1.hash()).start();
			frem.awaitUninterruptibly();
			frem = p1.remove(n1).contentKey(d2.hash()).start();
			frem.awaitUninterruptibly();
			fget = p2.get(n1).all().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(0, fget.dataMap().size());
			System.out.println("testKeys done");
		} finally {
			if (p1 != null) {
				p1.shutdown().await();
			}
			if (p2 != null) {
				p2.shutdown().await();
			}
		}
	}

	@Test
	public void testKeys2() throws Exception {
		final Random rnd = new Random(42L);
		PeerDHT p1 = null;
		PeerDHT p2 = null;
		try {
			Number160 n1 = new Number160(rnd);
			Number160 n2 = new Number160(rnd);
			Data d1 = new Data("hello");
			Data d2 = new Data("world!");
			// setup (step 1)
			p1 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4001).start()).start();
			FuturePut fput = p1.put(n1).data(d1).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			p2 = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4002).start()).start();
			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
			// test (step 2)
			fput = p1.put(n2).data(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			FutureGet fget = p2.get(n2).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(1, fget.dataMap().size());
			// test (step 3)
			FutureRemove frem = p1.remove(n2).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			fget = p2.get(n2).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(0, fget.dataMap().size());
			// test (step 4)
			fput = p1.put(n2).data(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			fget = p2.get(n2).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(1, fget.dataMap().size());
			System.out.println("testKeys2 done");
		} finally {
			if (p1 != null) {
				p1.shutdown().await();
			}
			if (p2 != null) {
				p2.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGetAll() throws Exception {
		final AtomicBoolean running = new AtomicBoolean(true);
		PeerDHT master = null;
		try {
			// setup
			final PeerDHT[] peers = UtilsDHT2.createNodes(100, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			final Number160 key = Number160.createHash("test");
			final Data data1 = new Data("test1");
			data1.ttlSeconds(3);
			final Data data2 = new Data("test2");
			data2.ttlSeconds(3);

			// add every second a two values
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					while (running.get()) {
						peers[10].add(key).data(data1).start().awaitUninterruptibly();
						peers[10].add(key).data(data2).start().awaitUninterruptibly();
						try {
	                        Thread.sleep(1000);
                        } catch (InterruptedException e) {
	                        e.printStackTrace();
                        }
					}
				}
			});
			t.start();
			// wait until the first data is stored.
			Thread.sleep(1000);
			for (int i = 0; i < 30; i++) {
				FutureGet fget = peers[20 + i].get(key).all().start();
				fget.awaitUninterruptibly();
				Assert.assertEquals(2, fget.dataMap().size());
				Thread.sleep(1000);
			}
		} finally {
			running.set(false);
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	/**
	 * This will probably fail on your machine since you have to have eth0
	 * configured. This testcase is suited for running on the tomp2p.net server
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBindings() throws Exception {
		final Random rnd = new Random(42L);
		Peer p1 = null;
		Peer p2 = null;
		try {
			// setup (step 1)
			Bindings b = new Bindings();
			p1 = new PeerBuilder(new Number160(rnd)).ports(4001).bindings(b).start();
			p2 = new PeerBuilder(new Number160(rnd)).ports(4002).bindings(b).start();
			FutureBootstrap fb = p2.bootstrap().peerAddress(p1.peerAddress()).start();
			fb.awaitUninterruptibly();
			Assert.assertEquals(true, fb.isSuccess());
			System.out.println("testBindings done");
		} finally {
			if (p1 != null) {
				p1.shutdown().await();
			}
			if (p2 != null) {
				p2.shutdown().await();
			}

		}
	}

	@Test
	public void testTooManyOpenFilesInSystem() throws Exception {
		Peer master = null;
		Peer slave = null;
		try {
			// since we have two peers, we need to reduce the connections -> we
			// will have 300 * 2 (peer connection)
			// plus 100 * 2 * 2. The last multiplication is due to discover,
			// where the recipient creates a connection
			// with its own limit. Since the limit is 1024 and we stop at 1000
			// only for the connection, we may run into
			// too many open files
			PeerBuilder masterMaker = new PeerBuilder(new Number160(rnd)).ports(4001);
			master = masterMaker.enableMaintenance(false).start();
			PeerBuilder slaveMaker = new PeerBuilder(new Number160(rnd)).ports(4002);
			slave = slaveMaker.enableMaintenance(false).start();

			System.out.println("peers up and running");

			slave.rawDataReply(new RawDataReply() {
				@Override
				public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean last) throws Exception {
					final byte[] b1 = new byte[10000];
					int i = requestBuffer.buffer().getInt(0);
					ByteBuf buf = Unpooled.wrappedBuffer(b1);
					buf.setInt(0, i);
					return new Buffer(buf);
				}
			});
			List<BaseFuture> list1 = new ArrayList<BaseFuture>();
			List<BaseFuture> list2 = new ArrayList<BaseFuture>();
			List<FuturePeerConnection> list3 = new ArrayList<FuturePeerConnection>();
			for (int i = 0; i < 125; i++) {
				final byte[] b = new byte[10000];
				FuturePeerConnection pc = master.createPeerConnection(slave.peerAddress());
				list1.add(master.sendDirect(pc).dataBuffer(new DataBuffer(Unpooled.wrappedBuffer(b))).start());
				list3.add(pc);
				// pc.close();
			}
			for (int i = 0; i < 20000; i++) {
				list2.add(master.discover().peerAddress(slave.peerAddress()).start());
				final byte[] b = new byte[10000];
				byte[] me = Utils.intToByteArray(i);
				System.arraycopy(me, 0, b, 0, 4);
				list2.add(master.sendDirect(slave.peerAddress()).dataBuffer(new DataBuffer(Unpooled.wrappedBuffer(b)))
				        .start());
			}
			for (BaseFuture bf : list1) {
				bf.awaitUninterruptibly();
				bf.awaitListenersUninterruptibly();
				if (bf.isFailed()) {
					System.out.println("WTF " + bf.failedReason());
				} else {
					System.out.print(",");
				}
				Assert.assertEquals(true, bf.isSuccess());
			}
			for (FuturePeerConnection pc : list3) {
				pc.close().awaitUninterruptibly();
				pc.close().awaitListenersUninterruptibly();
			}
			for (BaseFuture bf : list2) {
				bf.awaitUninterruptibly();
				bf.awaitListenersUninterruptibly();
				if (bf.isFailed()) {
					System.out.println("WTF " + bf.failedReason());
				} else {
					System.out.print(".");
				}
				Assert.assertEquals(true, bf.isSuccess());
			}
			System.out.println("done!!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("done!1!");
			if (master != null) {
				master.shutdown().await();
			}
			if (slave != null) {
				slave.shutdown().await();
			}
		}
	}

	

	@Test
	public void testThreads() throws Exception {
		PeerDHT master = null;
		PeerDHT slave = null;
		try {

			DefaultEventExecutorGroup eventExecutorGroup = new DefaultEventExecutorGroup(250);
			ChannelClientConfiguration ccc1 = PeerBuilder.createDefaultChannelClientConfiguration();
			ccc1.pipelineFilter(new PeerBuilder.EventExecutorGroupFilter(eventExecutorGroup));

			ChannelServerConfiguration ccs1 = PeerBuilder.createDefaultChannelServerConfiguration();
			ccs1.pipelineFilter(new PeerBuilder.EventExecutorGroupFilter(eventExecutorGroup));

			master = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4001).channelClientConfiguration(ccc1)
			        .channelServerConfiguration(ccs1).start()).start();
			slave = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4002).channelClientConfiguration(ccc1)
			        .channelServerConfiguration(ccs1).start()).start();

			master.peer().bootstrap().peerAddress(slave.peerAddress()).start().awaitUninterruptibly();
			slave.peer().bootstrap().peerAddress(master.peerAddress()).start().awaitUninterruptibly();

			System.out.println("peers up and running");
			
			final int count = 100;
			final CountDownLatch latch = new CountDownLatch(count);
			final AtomicBoolean correct = new AtomicBoolean(true);

			for (int i = 0; i < count; i++) {
				FuturePut futurePut = master.put(Number160.ONE).data(new Data("test")).start();
				futurePut.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						Thread.sleep(1000);
						latch.countDown();
						System.out.println("block in "+Thread.currentThread().getName());
						if(!Thread.currentThread().getName().contains("EventExecutorGroup")) {
							correct.set(false);
						}
					}
				});
			}
			latch.await(10, TimeUnit.SECONDS);
			Assert.assertTrue(correct.get());

		} finally {
			System.out.println("done!1!");
			if (master != null) {
				master.shutdown().await();
			}
			if (slave != null) {
				slave.shutdown().await();
			}
			System.out.println("done!2!");
		}
	}
	
	
	
	@Test
	public void removeFromToTest3() throws IOException, ClassNotFoundException {
		PeerDHT p1 = null;
		PeerDHT p2 = null;
		try {

			p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(5000).start()).start();
			p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).start()).start();

			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
			p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
			Number160 lKey = Number160.createHash("location");
			Number160 dKey = Number160.createHash("domain");
			Number160 cKey = Number160.createHash("content");
			String data = "test";
			p2.put(lKey).data(cKey, new Data(data)).domainKey(dKey).start().awaitUninterruptibly();
			FutureRemove futureRemove = p1.remove(lKey).domainKey(dKey).contentKey(cKey).start();
			futureRemove.awaitUninterruptibly();
			// check with a normal digest
			FutureDigest futureDigest = p1.digest(lKey).contentKey(cKey).domainKey(dKey).start();
			futureDigest.awaitUninterruptibly();
			Assert.assertTrue(futureDigest.digest().keyDigest().isEmpty());
			// check with a from/to digest
			futureDigest = p1.digest(lKey).from(new Number640(lKey, dKey, cKey, Number160.ZERO))
			        .to(new Number640(lKey, dKey, cKey, Number160.MAX_VALUE)).start();
			futureDigest.awaitUninterruptibly();
			Assert.assertTrue(futureDigest.digest().keyDigest().isEmpty());
		} finally {
			if (p1 != null) {
				p1.shutdown().awaitUninterruptibly();
			}
			if (p2 != null) {
				p2.shutdown().awaitUninterruptibly();
			}
		}
	}
	

	@Test
	public void removeFromToTest4() throws IOException, ClassNotFoundException {
		PeerDHT p1 = null;
		PeerDHT p2 = null;
		try {
			p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(5000).start()).start();
			p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).start()).start();
			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
			p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
			Number160 lKey = Number160.createHash("location");
			Number160 dKey = Number160.createHash("domain");
			Number160 cKey = Number160.createHash("content");
			String data = "test";
			p2.put(lKey).data(cKey, new Data(data)).domainKey(dKey).start().awaitUninterruptibly();
			FutureRemove futureRemove = p1.remove(lKey).from(new Number640(lKey, dKey, cKey, Number160.ZERO))
			        .to(new Number640(lKey, dKey, cKey, Number160.MAX_VALUE)).start();
			futureRemove.awaitUninterruptibly();
			FutureDigest futureDigest = p1.digest(lKey).from(new Number640(lKey, dKey, cKey, Number160.ZERO))
			        .to(new Number640(lKey, dKey, cKey, Number160.MAX_VALUE)).start();
			futureDigest.awaitUninterruptibly();
			// should be empty
			Assert.assertTrue(futureDigest.digest().keyDigest().isEmpty());
		} finally {
			if (p1 != null) {
				p1.shutdown().awaitUninterruptibly();
			}
			if (p2 != null) {
				p2.shutdown().awaitUninterruptibly();
			}
		}
	}
	
	@Test
	public void testShutdown() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			Random rnd = new Random();
			final int nrPeers = 10;
			final int port = 4001;
			// Peer[] peers = Utils2.createNodes(nrPeers, rnd, port);
			// Peer[] peers =
			// createAndAttachNodesWithReplicationShortId(nrPeers, port);
			// Peer[] peers = createNodes(nrPeers, rnd, port, null, true);
			PeerDHT[] peers = createNodesWithShortId(nrPeers, rnd, port, null);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);
			// do testing

			final int peerTest = 3;
			peers[peerTest].put(Number160.createHash(1000)).data(new Data("Test")).start().awaitUninterruptibly();

			for (int i = 0; i < nrPeers; i++) {
				for (Data d : peers[i].storageLayer().get().values())
					System.out.println("peer[" + i + "]: " + d.object().toString() + " ");
			}

			FutureDone<Void> futureShutdown = peers[peerTest].peer().announceShutdown().start();
			futureShutdown.awaitUninterruptibly();
			// we need to wait a bit, since the quit RPC is a fire and forget
			// and we return immediately
			Thread.sleep(2000);
			peers[peerTest].shutdown().awaitUninterruptibly();
			System.out.println("peer " + peerTest + " is shutdown");
			for (int i = 0; i < nrPeers; i++) {
				for (Data d : peers[i].storageLayer().get().values())
					System.out.println("peer[" + i + "]: " + d.object().toString() + " ");
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutPreparePutConfirmGet() throws Exception {
		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(10, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);

			// put data with a prepare flag
			Data data = new Data("test").prepareFlag();
			Number160 locationKey = Number160.createHash("location");
			Number160 domainKey = Number160.createHash("domain");
			Number160 contentKey = Number160.createHash("content");
			Number160 versionKey = Number160.createHash("version");
			FuturePut fput = peers[rnd.nextInt(10)].put(locationKey).data(contentKey, data)
					.domainKey(domainKey).versionKey(versionKey).start();
			fput.awaitUninterruptibly();
			fput.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());

			// get shouldn't see provisional put
			FutureGet fget = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
					.contentKey(contentKey).versionKey(versionKey).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isEmpty());

			// confirm prepared put
			Data tmp = new Data();
			FuturePut fputConfirm = peers[rnd.nextInt(10)].put(locationKey).data(contentKey, tmp)
					.domainKey(domainKey).versionKey(versionKey).putConfirm().start();
			fputConfirm.awaitUninterruptibly();
			fputConfirm.futureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fputConfirm.isSuccess());

			// get should see confirmed put
			fget = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey).contentKey(contentKey)
					.versionKey(versionKey).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(data.object(), fget.data().object());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	/**
	 * .../2b../4b-5b../7b..................................
	 * .....................................................
	 * 0-1-2a-3-4a-5a-6-7a..................................
	 * 
	 * result should be 2b, 5b, 7a, 7b
	 */
	@Test
	public void testGetLatestVersion1() throws Exception {
		// create test data
		NavigableMap<Number160, Data> sortedMap = new TreeMap<Number160, Data>();

		String content0 = generateRandomString();
		Number160 vKey0 = generateVersionKey(0, content0);
		Data data0 = new Data(content0);
		sortedMap.put(vKey0, data0);

		String content1 = generateRandomString();
		Number160 vKey1 = generateVersionKey(1, content1);
		Data data1 = new Data(content1);
		data1.addBasedOn(vKey0);
		sortedMap.put(vKey1, data1);

		String content2a = generateRandomString();
		Number160 vKey2a = generateVersionKey(2, content2a);
		Data data2a = new Data(content2a);
		data2a.addBasedOn(vKey1);
		sortedMap.put(vKey2a, data2a);

		String content2b = generateRandomString();
		Number160 vKey2b = generateVersionKey(2, content2b);
		Data data2b = new Data(content2b);
		data2b.addBasedOn(vKey1);
		sortedMap.put(vKey2b, data2b);

		String content3 = generateRandomString();
		Number160 vKey3 = generateVersionKey(3, content3);
		Data data3 = new Data(content3);
		data3.addBasedOn(vKey2a);
		sortedMap.put(vKey3, data3);

		String content4a = generateRandomString();
		Number160 vKey4a = generateVersionKey(4, content4a);
		Data data4a = new Data(content4a);
		data4a.addBasedOn(vKey3);
		sortedMap.put(vKey4a, data4a);

		String content4b = generateRandomString();
		Number160 vKey4b = generateVersionKey(4, content4b);
		Data data4b = new Data(content4b);
		data4b.addBasedOn(vKey3);
		sortedMap.put(vKey4b, data4b);

		String content5a = generateRandomString();
		Number160 vKey5a = generateVersionKey(5, content5a);
		Data data5a = new Data(content5a);
		data5a.addBasedOn(vKey4a);
		sortedMap.put(vKey5a, data5a);

		String content5b = generateRandomString();
		Number160 vKey5b = generateVersionKey(5, content5b);
		Data data5b = new Data(content5b);
		data5b.addBasedOn(vKey4b);
		sortedMap.put(vKey5b, data5b);

		String content6 = generateRandomString();
		Number160 vKey6 = generateVersionKey(6, content6);
		Data data6 = new Data(content6);
		data6.addBasedOn(vKey5a);
		sortedMap.put(vKey6, data6);

		String content7a = generateRandomString();
		Number160 vKey7a = generateVersionKey(7, content7a);
		Data data7a = new Data(content7a);
		data7a.addBasedOn(vKey6);
		sortedMap.put(vKey7a, data7a);

		String content7b = generateRandomString();
		Number160 vKey7b = generateVersionKey(7, content7b);
		Data data7b = new Data(content7b);
		data7b.addBasedOn(vKey6);
		sortedMap.put(vKey7b, data7b);

		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(10, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);

			// put test data
			Number160 locationKey = Number160.createHash("location");
			Number160 domainKey = Number160.createHash("domain");
			Number160 contentKey = Number160.createHash("content");
			for (Number160 vKey : sortedMap.keySet()) {
				FuturePut fput = peers[rnd.nextInt(10)].put(locationKey)
						.data(contentKey, sortedMap.get(vKey)).domainKey(domainKey).versionKey(vKey)
						.start();
				fput.awaitUninterruptibly();
				fput.futureRequests().awaitUninterruptibly();
				Assert.assertEquals(true, fput.isSuccess());
			}

			// get latest versions
			FutureGet fget = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
					.contentKey(contentKey).getLatest().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());

			// check result
			Map<Number640, Data> dataMap = fget.dataMap();
			Assert.assertEquals(4, dataMap.size());
			Number480 key480 = new Number480(locationKey, domainKey, contentKey);

			Number640 key2b = new Number640(key480, vKey2b);
			Assert.assertTrue(dataMap.containsKey(key2b));
			Assert.assertEquals(data2b.object(), dataMap.get(key2b).object());

			Number640 key5b = new Number640(key480, vKey5b);
			Assert.assertTrue(dataMap.containsKey(key5b));
			Assert.assertEquals(data5b.object(), dataMap.get(key5b).object());

			Number640 key7a = new Number640(key480, vKey7a);
			Assert.assertTrue(dataMap.containsKey(key7a));
			Assert.assertEquals(data7a.object(), dataMap.get(key7a).object());

			Number640 key7b = new Number640(key480, vKey7b);
			Assert.assertTrue(dataMap.containsKey(key7b));
			Assert.assertEquals(data7b.object(), dataMap.get(key7b).object());

			// get latest versions with digest
			FutureGet fgetWithDigest = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
					.contentKey(contentKey).getLatest().withDigest().start();
			fgetWithDigest.awaitUninterruptibly();
			Assert.assertTrue(fgetWithDigest.isSuccess());

			// check digest result
			DigestResult digestResult = fgetWithDigest.digest();
			Assert.assertEquals(12, digestResult.keyDigest().size());
			for (Number160 vKey : sortedMap.keySet()) {
				Number640 key = new Number640(locationKey, domainKey, contentKey, vKey);
				Assert.assertTrue(digestResult.keyDigest().containsKey(key));
				Assert.assertEquals(sortedMap.get(vKey).basedOnSet().size(), digestResult.keyDigest()
						.get(key).size());
				for (Number160 bKey : sortedMap.get(vKey).basedOnSet()) {
					Assert.assertTrue(digestResult.keyDigest().get(key).contains(bKey));
				}
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	/**
	 * .........../5c.......................................
	 * .....................................................
	 * .../2b../4b-5b./6b...................................
	 * .....................................................
	 * 0-1-2a-3-4a-5a.-6a-7.................................
	 * 
	 * result should be 2b, 5b, 5c, 6b, 7
	 */
	@Test
	public void testGetLatestVersion2() throws Exception {
		NavigableMap<Number160, Data> sortedMap = new TreeMap<Number160, Data>();

		String content0 = generateRandomString();
		Number160 vKey0;
		vKey0 = generateVersionKey(0, content0);
		Data data0 = new Data(content0);
		sortedMap.put(vKey0, data0);

		String content1 = generateRandomString();
		Number160 vKey1 = generateVersionKey(1, content1);
		Data data1 = new Data(content1);
		data1.addBasedOn(vKey0);
		sortedMap.put(vKey1, data1);

		String content2a = generateRandomString();
		Number160 vKey2a = generateVersionKey(2, content2a);
		Data data2a = new Data(content2a);
		data2a.addBasedOn(vKey1);
		sortedMap.put(vKey2a, data2a);

		String content2b = generateRandomString();
		Number160 vKey2b = generateVersionKey(2, content2b);
		Data data2b = new Data(content2b);
		data2b.addBasedOn(vKey1);
		sortedMap.put(vKey2b, data2b);

		String content3 = generateRandomString();
		Number160 vKey3 = generateVersionKey(3, content3);
		Data data3 = new Data(content3);
		data3.addBasedOn(vKey2a);
		sortedMap.put(vKey3, data3);

		String content4a = generateRandomString();
		Number160 vKey4a = generateVersionKey(4, content4a);
		Data data4a = new Data(content4a);
		data4a.addBasedOn(vKey3);
		sortedMap.put(vKey4a, data4a);

		String content4b = generateRandomString();
		Number160 vKey4b = generateVersionKey(4, content4b);
		Data data4b = new Data(content4b);
		data4b.addBasedOn(vKey3);
		sortedMap.put(vKey4b, data4b);

		String content5a = generateRandomString();
		Number160 vKey5a = generateVersionKey(5, content5a);
		Data data5a = new Data(content5a);
		data5a.addBasedOn(vKey4a);
		sortedMap.put(vKey5a, data5a);

		String content5b = generateRandomString();
		Number160 vKey5b = generateVersionKey(5, content5b);
		Data data5b = new Data(content5b);
		data5b.addBasedOn(vKey4b);
		sortedMap.put(vKey5b, data5b);

		String content5c = generateRandomString();
		Number160 vKey5c = generateVersionKey(5, content5c);
		Data data5c = new Data(content5c);
		data5c.addBasedOn(vKey4b);
		sortedMap.put(vKey5c, data5c);

		String content6a = generateRandomString();
		Number160 vKey6a = generateVersionKey(6, content6a);
		Data data6a = new Data(content6a);
		data6a.addBasedOn(vKey5a);
		sortedMap.put(vKey6a, data6a);

		String content6b = generateRandomString();
		Number160 vKey6b = generateVersionKey(6, content6b);
		Data data6b = new Data(content6b);
		data6b.addBasedOn(vKey5a);
		sortedMap.put(vKey6b, data6b);

		String content7 = generateRandomString();
		Number160 vKey7 = generateVersionKey(7, content7);
		Data data7 = new Data(content7);
		data7.addBasedOn(vKey6a);
		sortedMap.put(vKey7, data7);

		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(10, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);

			// put test data
			Number160 locationKey = Number160.createHash("location");
			Number160 domainKey = Number160.createHash("domain");
			Number160 contentKey = Number160.createHash("content");
			for (Number160 vKey : sortedMap.keySet()) {
				FuturePut fput = peers[rnd.nextInt(10)].put(locationKey)
						.data(contentKey, sortedMap.get(vKey)).domainKey(domainKey).versionKey(vKey)
						.start();
				fput.awaitUninterruptibly();
				fput.futureRequests().awaitUninterruptibly();
				Assert.assertEquals(true, fput.isSuccess());
			}

			// get latest versions
			FutureGet fget = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
					.contentKey(contentKey).getLatest().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());

			// check result
			Map<Number640, Data> dataMap = fget.dataMap();
			Assert.assertEquals(5, dataMap.size());
			Number480 key480 = new Number480(locationKey, domainKey, contentKey);

			Number640 key2b = new Number640(key480, vKey2b);
			Assert.assertTrue(dataMap.containsKey(key2b));
			Assert.assertEquals(data2b.object(), dataMap.get(key2b).object());

			Number640 key5b = new Number640(key480, vKey5b);
			Assert.assertTrue(dataMap.containsKey(key5b));
			Assert.assertEquals(data5b.object(), dataMap.get(key5b).object());

			Number640 key5c = new Number640(key480, vKey5c);
			Assert.assertTrue(dataMap.containsKey(key5c));
			Assert.assertEquals(data5c.object(), dataMap.get(key5c).object());

			Number640 key6b = new Number640(key480, vKey6b);
			Assert.assertTrue(dataMap.containsKey(key6b));
			Assert.assertEquals(data6b.object(), dataMap.get(key6b).object());

			Number640 key7 = new Number640(key480, vKey7);
			Assert.assertTrue(dataMap.containsKey(key7));
			Assert.assertEquals(data7.object(), dataMap.get(key7).object());

			// get latest versions with digest
			FutureGet fgetWithDigest = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
					.contentKey(contentKey).getLatest().withDigest().start();
			fgetWithDigest.awaitUninterruptibly();
			Assert.assertTrue(fgetWithDigest.isSuccess());

			// check digest result
			DigestResult digestResult = fgetWithDigest.digest();
			Assert.assertEquals(13, digestResult.keyDigest().size());
			for (Number160 vKey : sortedMap.keySet()) {
				Number640 key = new Number640(locationKey, domainKey, contentKey, vKey);
				Assert.assertTrue(digestResult.keyDigest().containsKey(key));
				Assert.assertEquals(sortedMap.get(vKey).basedOnSet().size(), digestResult.keyDigest()
						.get(key).size());
				for (Number160 bKey : sortedMap.get(vKey).basedOnSet()) {
					Assert.assertTrue(digestResult.keyDigest().get(key).contains(bKey));
				}
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	/**
	 * 0-1-2
	 * 
	 * result should return version 2
	 */
	@Test
	public void testGetLatestVersion3() throws Exception {
		NavigableMap<Number160, Data> sortedMap = new TreeMap<Number160, Data>();

		String content0 = generateRandomString();
		Number160 vKey0;
		vKey0 = generateVersionKey(0, content0);
		Data data0 = new Data(content0);
		sortedMap.put(vKey0, data0);

		String content1 = generateRandomString();
		Number160 vKey1 = generateVersionKey(1, content1);
		Data data1 = new Data(content1);
		data1.addBasedOn(vKey0);
		sortedMap.put(vKey1, data1);

		String content2 = generateRandomString();
		Number160 vKey2 = generateVersionKey(2, content2);
		Data data2 = new Data(content2);
		data2.addBasedOn(vKey1);
		sortedMap.put(vKey2, data2);

		PeerDHT master = null;
		try {
			// setup
			PeerDHT[] peers = UtilsDHT2.createNodes(10, rnd, 4001);
			master = peers[0];
			UtilsDHT2.perfectRouting(peers);

			// put test data
			Number160 locationKey = Number160.createHash("location");
			Number160 domainKey = Number160.createHash("domain");
			Number160 contentKey = Number160.createHash("content");
			for (Number160 vKey : sortedMap.keySet()) {
				FuturePut fput = peers[rnd.nextInt(10)].put(locationKey)
						.data(contentKey, sortedMap.get(vKey)).domainKey(domainKey).versionKey(vKey)
						.start();
				fput.awaitUninterruptibly();
				fput.futureRequests().awaitUninterruptibly();
				Assert.assertEquals(true, fput.isSuccess());
			}

			// get latest versions
			FutureGet fget = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
					.contentKey(contentKey).getLatest().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());

			// check result
			Map<Number640, Data> dataMap = fget.dataMap();
			Assert.assertEquals(1, dataMap.size());
			Number480 key480 = new Number480(locationKey, domainKey, contentKey);

			Number640 key2 = new Number640(key480, vKey2);
			Assert.assertTrue(dataMap.containsKey(key2));
			Assert.assertEquals(data2.object(), dataMap.get(key2).object());

			// get latest versions with digest
			FutureGet fgetWithDigest = peers[rnd.nextInt(10)].get(locationKey).domainKey(domainKey)
					.contentKey(contentKey).getLatest().withDigest().start();
			fgetWithDigest.awaitUninterruptibly();
			Assert.assertTrue(fgetWithDigest.isSuccess());

			// check digest result
			DigestResult digestResult = fgetWithDigest.digest();
			Assert.assertEquals(3, digestResult.keyDigest().size());
			for (Number160 vKey : sortedMap.keySet()) {
				Number640 key = new Number640(locationKey, domainKey, contentKey, vKey);
				Assert.assertTrue(digestResult.keyDigest().containsKey(key));
				Assert.assertEquals(sortedMap.get(vKey).basedOnSet().size(), digestResult.keyDigest()
						.get(key).size());
				for (Number160 bKey : sortedMap.get(vKey).basedOnSet()) {
					Assert.assertTrue(digestResult.keyDigest().get(key).contains(bKey));
				}
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	@Test
	public void testShutdown3Peers() throws IOException, ClassNotFoundException {
	    //create 3 peers, store data, shut one peer down, joins -> data should be there
	    PeerMapConfiguration pmc = new PeerMapConfiguration(Number160.createHash( "1" ) ).peerNoVerification();
        PeerMap pm = new PeerMap(pmc);
	    PeerDHT peer1 = new PeerBuilderDHT( new PeerBuilder( Number160.createHash( "1" ) ).peerMap( pm ).ports( 3000 ).start() ).start();
	    PeerDHT peer2 = new PeerBuilderDHT( new PeerBuilder( Number160.createHash( "2" ) ).ports( 3001 ).start() ).start();
	    PeerDHT peer3 = new PeerBuilderDHT( new PeerBuilder( Number160.createHash( "3" ) ).ports( 3002 ).start() ).start();
	    
	    BaseFuture fb1 = peer2.peer().bootstrap().peerAddress( peer1.peerAddress() ).start().awaitUninterruptibly();
	    BaseFuture fb2 = peer3.peer().bootstrap().peerAddress( peer1.peerAddress() ).start().awaitUninterruptibly();
	    
	    Assert.assertTrue( fb1.isSuccess() );
	    Assert.assertTrue( fb2.isSuccess() );
	    
	    FuturePut fp = peer2.put( Number160.ONE ).object( "test" ).start().awaitUninterruptibly();
	    Assert.assertTrue( fp.isSuccess() );
	    
	    peer2.shutdown().awaitUninterruptibly();
	    peer2 = new PeerBuilderDHT( new PeerBuilder( Number160.createHash( "2" ) ).ports( 3001 ).start() ).start();
	    BaseFuture fb3 = peer2.peer().bootstrap().peerAddress( peer1.peerAddress() ).start().awaitUninterruptibly();
        Assert.assertTrue( fb3.isSuccess() );
        
        FutureGet fg1 = peer2.get( Number160.ONE ).start().awaitUninterruptibly();
        Assert.assertTrue( fg1.isSuccess() );
        Assert.assertEquals( "test", fg1.data().object() );
	    
	    peer3.shutdown().awaitUninterruptibly();
        peer3 = new PeerBuilderDHT( new PeerBuilder( Number160.createHash( "3" ) ).ports( 3002 ).start() ).start();
        BaseFuture fb4 = peer3.peer().bootstrap().peerAddress( peer1.peerAddress() ).start().awaitUninterruptibly();
        Assert.assertTrue( fb4.isSuccess() );
	    
        FutureGet fg2 = peer3.get( Number160.ONE ).start().awaitUninterruptibly();
        Assert.assertTrue( fg2.isSuccess() );
        Assert.assertEquals( "test", fg2.data().object() );   
	}

	private static String generateRandomString() {
		return UUID.randomUUID().toString();
	}

	private static Number160 generateVersionKey(long basedOnCounter, Serializable object) throws IOException {
		// get a MD5 hash of the object itself
		byte[] hash = generateMD5Hash(serializeObject(object));
		return new Number160(basedOnCounter, new Number160(Arrays.copyOf(hash, Number160.BYTE_ARRAY_SIZE)));
	}

	private static byte[] generateMD5Hash(byte[] data) {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
		}
		md.reset();
		md.update(data, 0, data.length);
		return md.digest();
	}

	private static byte[] serializeObject(Serializable object) throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		byte[] result = null;

		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			result = baos.toByteArray();
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (oos != null)
					oos.close();
				if (baos != null)
					baos.close();
			} catch (IOException e) {
				throw e;
			}
		}
		return result;
	}

	public static PeerDHT[] createNodesWithShortId(int nrOfPeers, Random rnd, int port, AutomaticFuture automaticFuture) throws Exception {
		if (nrOfPeers < 1) {
			throw new IllegalArgumentException("Cannot create less than 1 peer");
		}
		final Peer master;
		PeerDHT[] peers = new PeerDHT[nrOfPeers];
		if (automaticFuture != null) {
			master = new PeerBuilder(new Number160(1111))
			        .ports(port).start().addAutomaticFuture(automaticFuture);
			peers[0] = new PeerBuilderDHT(master).start();
		} else {
			master = new PeerBuilder(new Number160(1111)).ports(port)
			        .start();
			peers[0] = new PeerBuilderDHT(master).start();
		}

		for (int i = 1; i < nrOfPeers; i++) {
			if (automaticFuture != null) {
				Peer peer = new PeerBuilder(new Number160(i))
				        .masterPeer(master).start().addAutomaticFuture(automaticFuture);
				peers[i] = new PeerBuilderDHT(peer).start();
			} else {
				Peer peer = new PeerBuilder(new Number160(i))
				        .masterPeer(master).start();
				peers[i] = new PeerBuilderDHT(peer).start();
			}
		}
		System.out.println("peers created.");
		return peers;
	}

	private void send2(final PeerDHT p1, final PeerDHT p2, final ByteBuf toStore1, final int count) throws IOException {
		if (count == 0) {
			return;
		}
		DataBuffer b = new DataBuffer(toStore1);
		FutureDirect fd = p1.peer().sendDirect(p2.peerAddress()).dataBuffer(b).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {
			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				if (future.isFailed()) {
					// System.out.println(future.getFailedReason());
					send2(p1, p2, toStore1, count - 1);
				}
			}
		});
	}

	private void send1(final PeerDHT p1, final PeerDHT p2, final byte[] toStore1, final int count) throws IOException {
		if (count == 0) {
			return;
		}
		FutureDirect fd = p1.peer().sendDirect(p2.peerAddress()).object(toStore1).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {
			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				if (future.isFailed()) {
					//System.out.println(future.getFailedReason());
					send1(p1, p2, toStore1, count - 1);
				}
			}
		});
	}

	private void testForArray(PeerDHT peer, Number160 locationKey, boolean find) {
		Collection<Number160> tmp = new ArrayList<Number160>();
		tmp.add(new Number160(5));
		Number640 min = new Number640(locationKey, Number160.createHash("test"), Number160.ZERO, Number160.ZERO);
		Number640 max = new Number640(locationKey, Number160.createHash("test"), Number160.MAX_VALUE,
		        Number160.MAX_VALUE);
		Map<Number640, Data> test = peer.storageLayer().get(min, max, -1, true);
		if (find) {
			Assert.assertEquals(1, test.size());
			Assert.assertEquals(
			        44444,
			        test.get(
			                new Number640(new Number320(locationKey, Number160.createHash("test")), new Number160(5),
			                        Number160.ZERO)).length());
		} else {
			Assert.assertEquals(0, test.size());
		}
		for(Map.Entry<Number640, Data> entry: test.entrySet()) {
			entry.getValue().release();
		}
	}
}
