package net.tomp2p.p2p;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelServerConficuration;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDigest;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureGet;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.futures.FutureRemove;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureSend;
import net.tomp2p.futures.FutureShutdown;
import net.tomp2p.message.Buffer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.peers.PeerStatusListener.FailReason;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDHT {
	final private static Random rnd = new Random(42L);
	private static final Logger LOG = LoggerFactory.getLogger(TestDHT.class);

	@Test
	public void testPutTwo() throws Exception {
		Peer master = null;
		try {
			Peer[] peers = Utils2.createNodes(10, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			final Data data1 = new Data(new byte[1]);
			data1.ttlSeconds(3);
			FuturePut futurePut = master.put(Number160.createHash("test")).setData(data1).start();
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
		Peer master = null;
		try {
			Peer[] peers = Utils2.createNodes(10, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			final Data data1 = new Data(new byte[1]);
			data1.ttlSeconds(3);
			FuturePut futurePut = master.put(Number160.createHash("test")).setVersionKey(Number160.MAX_VALUE)
			        .setData(data1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertEquals(true, futurePut.isSuccess());
			//
			Map<Number640, Data> map = peers[0].getPeerBean().storage().get();
			Assert.assertEquals(Number160.MAX_VALUE, map.entrySet().iterator().next().getKey().getVersionKey());
			LOG.error("done");
		} finally {
			if (master != null) {
				master.shutdown().awaitListenersUninterruptibly();
			}
		}
	}

	@Test
	public void testPutPerforomance() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);

			for (int i = 0; i < 500; i++) {
				long start = System.currentTimeMillis();
				FuturePut fp = peers[444].put(Number160.createHash("1")).setData(new Data("test")).start();
				fp.awaitUninterruptibly();
				fp.getFutureRequests().awaitUninterruptibly();
				for (FutureResponse fr : fp.getFutureRequests().getCompleted()) {
					LOG.error(fr + " / " + fr.getRequest());
				}

				long stop = System.currentTimeMillis();
				System.err.println("Test " + fp.getFailedReason() + " / " + (stop - start) + "ms");
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
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fp = peers[444].put(peers[30].getPeerID())
			        .setData(Number160.createHash("test"), new Number160(5), data).setRequestP2PConfiguration(pc)
			        .setRoutingConfiguration(rc).start();

			fp.awaitUninterruptibly();
			fp.getFutureRequests().awaitUninterruptibly();
			System.err.println("Test " + fp.getFailedReason());
			Assert.assertEquals(true, fp.isSuccess());
			peers[30].getPeerBean().peerMap();
			// search top 3
			TreeMap<PeerAddress, Integer> tmp = new TreeMap<PeerAddress, Integer>(PeerMap.createComparator(peers[30]
			        .getPeerID()));
			int i = 0;
			for (Peer node : peers) {
				tmp.put(node.getPeerAddress(), i);
				i++;
			}
			Entry<PeerAddress, Integer> e = tmp.pollFirstEntry();
			System.err.println("1 (" + e.getValue() + ")" + e.getKey());
			Assert.assertEquals(peers[e.getValue()].getPeerAddress(), peers[30].getPeerAddress());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.err.println("2 (" + e.getValue() + ")" + e.getKey());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.err.println("3 " + e.getKey());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			System.err.println("4 " + e.getKey());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGetAlone() throws Exception {
		Peer master = null;
		try {
			master = new PeerMaker(new Number160(rnd)).ports(4001).makeAndListen();
			FuturePut fdht = master.put(Number160.ONE).setData(new Data("hallo")).start();
			fdht.awaitUninterruptibly();
			fdht.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			FutureGet fdht2 = master.get(Number160.ONE).start();
			fdht2.awaitUninterruptibly();
			System.err.println(fdht2.getFailedReason());
			Assert.assertEquals(true, fdht2.isSuccess());
			Data tmp = fdht2.getData();
			Assert.assertEquals("hallo", tmp.object().toString());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutTimeout() throws Exception {
		Peer master = null;
		try {
			master = new PeerMaker(new Number160(rnd)).ports(4001).storageIntervalMillis(1).makeAndListen();
			Data data = new Data("hallo");
			data.ttlSeconds(1);
			FuturePut fdht = master.put(Number160.ONE).setData(data).start();
			fdht.awaitUninterruptibly();
			fdht.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			Thread.sleep(3000);
			FutureGet fdht2 = master.get(Number160.ONE).start();
			fdht2.awaitUninterruptibly();
			System.err.println(fdht2.getFailedReason());
			Assert.assertEquals(false, fdht2.isSuccess());
			Data tmp = fdht2.getData();
			Assert.assertNull(tmp);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPut2() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(500, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(0, 0, 1);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(1, 0, 0);
			FuturePut fdht = peers[444].put(peers[30].getPeerID()).setData(new Number160(5), data)
			        .setDomainKey(Number160.createHash("test")).setRoutingConfiguration(rc)
			        .setRequestP2PConfiguration(pc).start();
			fdht.awaitUninterruptibly();
			fdht.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fdht.isSuccess());
			peers[30].getPeerBean().peerMap();
			// search top 3
			TreeMap<PeerAddress, Integer> tmp = new TreeMap<PeerAddress, Integer>(PeerMap.createComparator(peers[30]
			        .getPeerID()));
			int i = 0;
			for (Peer node : peers) {
				tmp.put(node.getPeerAddress(), i);
				i++;
			}
			Entry<PeerAddress, Integer> e = tmp.pollFirstEntry();
			Assert.assertEquals(peers[e.getValue()].getPeerAddress(), peers[30].getPeerAddress());
			testForArray(peers[e.getValue()], peers[30].getPeerID(), true);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
			//
			e = tmp.pollFirstEntry();
			testForArray(peers[e.getValue()], peers[30].getPeerID(), false);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGet() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);
			FuturePut fput = peers[444].put(peers[30].getPeerID()).setData(new Number160(5), data)
			        .setDomainKey(Number160.createHash("test")).setRoutingConfiguration(rc)
			        .setRequestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(0, 0, 10, 1);
			pc = new RequestP2PConfiguration(1, 0, 0);

			FutureGet fget = peers[555].get(peers[30].getPeerID()).setDomainKey(Number160.createHash("test"))
			        .setContentKey(new Number160(5)).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(1, fget.getRawData().size());
			Assert.assertEquals(true, fget.isMinReached());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutConvert() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);
			Map<Number160, Data> tmp = new HashMap<Number160, Data>();
			tmp.put(new Number160(5), data);
			FuturePut fput = peers[444].put(peers[30].getPeerID()).setDataMapContent(tmp)
			        .setDomainKey(Number160.createHash("test")).setRoutingConfiguration(rc)
			        .setRequestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			System.err.println(fput.getFailedReason());
			Assert.assertEquals(true, fput.isSuccess());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGet2() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(1000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
			Data data = new Data(new byte[44444]);

			FuturePut fput = peers[444].put(peers[30].getPeerID()).setData(new Number160(5), data)
			        .setDomainKey(Number160.createHash("test")).setRoutingConfiguration(rc)
			        .setRequestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);

			FutureGet fget = peers[555].get(peers[30].getPeerID()).setDomainKey(Number160.createHash("test"))
			        .setContentKey(new Number160(5)).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(3, fget.getRawData().size());
			Assert.assertEquals(false, fget.isMinReached());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGet3() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = peers[444].put(peers[30].getPeerID()).setData(new Number160(5), data)
			        .setDomainKey(Number160.createHash("test")).setRoutingConfiguration(rc)
			        .setRequestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(1, 0, 10, 1);
			pc = new RequestP2PConfiguration(1, 0, 0);
			for (int i = 0; i < 1000; i++) {
				FutureGet fget = peers[100 + i].get(peers[30].getPeerID()).setDomainKey(Number160.createHash("test"))
				        .setContentKey(new Number160(5)).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc)
				        .start();
				fget.awaitUninterruptibly();
				Assert.assertEquals(true, fget.isSuccess());
				Assert.assertEquals(1, fget.getRawData().size());
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
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = peers[444].put(peers[30].getPeerID()).setDomainKey(Number160.createHash("test"))
			        .setData(new Number160(5), data).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);
			FutureRemove frem = peers[222].remove(peers[30].getPeerID()).setDomainKey(Number160.createHash("test"))
			        .contentKey(new Number160(5)).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			Assert.assertEquals(3, frem.getRawKeys().size());

			FutureGet fget = peers[555].get(peers[30].getPeerID()).setDomainKey(Number160.createHash("test"))
			        .setContentKey(new Number160(5)).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(false, fget.isSuccess());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testPutGetRemove2() throws Exception {
		Peer master = null;
		try {
			// rnd.setSeed(253406013991563L);
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = peers[444].put(peers[30].getPeerID()).setData(new Number160(5), data)
			        .setDomainKey(Number160.createHash("test")).setRoutingConfiguration(rc)
			        .setRequestP2PConfiguration(pc).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			System.err.println("remove");
			rc = new RoutingConfiguration(4, 0, 10, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);

			FutureRemove frem = peers[222].remove(peers[30].getPeerID()).setReturnResults()
			        .setDomainKey(Number160.createHash("test")).contentKey(new Number160(5))
			        .setRoutingConfiguration(rc).setRequestP2PConfiguration(pc).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			Assert.assertEquals(3, frem.getRawData().size());
			System.err.println("get");
			rc = new RoutingConfiguration(4, 0, 0, 1);
			pc = new RequestP2PConfiguration(4, 0, 0);

			FutureGet fget = peers[555].get(peers[30].getPeerID()).setDomainKey(Number160.createHash("test"))
			        .setContentKey(new Number160(5)).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(false, fget.isSuccess());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testDirect() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(1000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			final AtomicInteger ai = new AtomicInteger(0);
			for (int i = 0; i < peers.length; i++) {
				peers[i].setObjectDataReply(new ObjectDataReply() {
					@Override
					public Object reply(PeerAddress sender, Object request) throws Exception {
						ai.incrementAndGet();
						return "ja";
					}
				});
			}
			// do testing
			FutureSend fdir = peers[400].send(new Number160(rnd)).setObject("hallo").start();
			fdir.awaitUninterruptibly();
			System.err.println(fdir.getFailedReason());
			Assert.assertEquals(true, fdir.isSuccess());
			Assert.assertEquals(true, ai.get() >= 3 && ai.get() <= 6);
			System.err.println("called: " + ai.get());
			Assert.assertEquals("ja", fdir.getObject());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testAddListGet() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo1";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			FuturePut fput = peers[30].add(nr).setData(data1).setList(true).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].add(nr).setData(data2).setList(true).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			FutureGet fget = peers[77].get(nr).setAll().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
			// majority voting with getDataMap is not possible since we create
			// random content key on the recipient
			Assert.assertEquals(2, fget.getRawData().values().iterator().next().values().size());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testAddGet() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			Data data1 = new Data(toStore1.getBytes());
			Data data2 = new Data(toStore2.getBytes());
			FuturePut fput = peers[30].add(nr).setData(data1).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].add(nr).setData(data2).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			FutureGet fget = peers[77].get(nr).setAll().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
	
	@Test
	public void removeTestLoop() throws IOException, ClassNotFoundException {
		for(int i=0;i<100;i++) {
			System.err.println("removeTestLoop() call nr "+i);
			removeTest();
		}
	}

	@Test
	public void removeTest() throws IOException, ClassNotFoundException {
		Peer p1 = new PeerMaker(Number160.createHash(1)).setEnableIndirectReplication(false).ports(5000).makeAndListen();
		Peer p2 = new PeerMaker(Number160.createHash(2)).setEnableIndirectReplication(false).masterPeer(p1)
		        .makeAndListen();

		p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();

		String locationKey = "location";
		String contentKey = "content";

		String data = "testme";
		// data.generateVersionKey();

		p2.put(Number160.createHash(locationKey)).setData(Number160.createHash(contentKey), new Data(data))
		        .setVersionKey(Number160.ONE).start().awaitUninterruptibly();

		FutureRemove futureRemove = p1.remove(Number160.createHash(locationKey)).setDomainKey(Number160.ZERO)
		        .contentKey(Number160.createHash(contentKey)).setVersionKey(Number160.ONE).start();
		futureRemove.awaitUninterruptibly();

		FutureDigest futureDigest = p1.digest(Number160.createHash(locationKey)).setDomainKey(Number160.ZERO)
		        .setContentKey(Number160.createHash(contentKey)).setVersionKey(Number160.ONE).start();
		futureDigest.awaitUninterruptibly();

		Assert.assertTrue(futureDigest.getDigest().keyDigest().isEmpty());

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}

	@Test
	public void testDigest1() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
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
			FuturePut fput = peers[30].put(nr).setData(key1, data1).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].put(nr).setData(key2, data2).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			fput = peers[51].put(nr).setData(key3, data3).start();
			fput.awaitUninterruptibly();
			System.out.println("added: " + toStore3 + " (" + fput.isSuccess() + ")");
			Number640 from = new Number640(nr, Number160.ZERO, Number160.ZERO, Number160.ZERO);
			Number640 to = new Number640(nr, Number160.MAX_VALUE, Number160.MAX_VALUE, Number160.MAX_VALUE);
			FutureDigest fget = peers[77].digest(nr).from(from).to(to).returnNr(1).start();
			fget.awaitUninterruptibly();
			System.err.println(fget.getFailedReason());
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(1, fget.getDigest().keyDigest().size());
			Assert.assertEquals(key1, fget.getDigest().keyDigest().keySet().iterator().next().getContentKey());
			fget = peers[67].digest(nr).from(from).to(to).returnNr(1).descending().start();
			fget.awaitUninterruptibly();
			System.err.println(fget.getFailedReason());
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(1, fget.getDigest().keyDigest().size());
			Assert.assertEquals(key3, fget.getDigest().keyDigest().keySet().iterator().next().getContentKey());
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testDigest2() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);

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
			FuturePut fput = peers[30].put(lKey).setData(ckey, data1, versionKey1).start();
			fput.awaitUninterruptibly();
			fput = peers[50].put(lKey).setData(ckey, data2, versionKey2).start();
			fput.awaitUninterruptibly();
			fput = peers[51].put(lKey).setData(ckey, data3, versionKey3).start();
			fput.awaitUninterruptibly();

			// get digest
			FutureDigest fget = peers[77].digest(lKey).setAll().start();
			fget.awaitUninterruptibly();
			DigestResult dr = fget.getDigest();
			NavigableMap<Number640, Set<Number160>> map = dr.keyDigest();
			
			// verify fetched digest
			Entry<Number640, Set<Number160>> e1 = map.pollFirstEntry();
			Assert.assertEquals(Number160.ONE, e1.getValue().iterator().next());
			Assert.assertEquals(new Number640(lKey, Number160.ZERO, ckey, versionKey1), e1.getKey());
			Entry<Number640, Set<Number160>> e2 = map.pollFirstEntry();
			Assert.assertEquals(versionKey1, e2.getValue().iterator().next());
			Assert.assertEquals(new Number640(lKey, Number160.ZERO, ckey, versionKey2), e2.getKey());
			Entry<Number640, Set<Number160>> e3 = map.pollFirstEntry();
			Assert.assertEquals(versionKey2, e3.getValue().iterator().next());
			Assert.assertEquals(new Number640(lKey, Number160.ZERO, ckey, versionKey2), e3.getKey());

		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testData() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			ByteBuf c = Unpooled.buffer();
			c.writeInt(77);
			Buffer b = new Buffer(c);
			peers[50].setRawDataReply(new RawDataReply() {
				@Override
				public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) {
					System.err.println(requestBuffer.buffer().readInt());
					ByteBuf c = Unpooled.buffer();
					c.writeInt(88);
					Buffer ret = new Buffer(c);
					return ret;
				}
			});
			FutureDirect fd = master.sendDirect(peers[50].getPeerAddress()).setBuffer(b).start();
			fd.await();
			if (fd.getBuffer() == null) {
				System.err.println("damm");
				Assert.fail();
			}
			int read = fd.getBuffer().buffer().readInt();
			Assert.assertEquals(88, read);
			System.err.println("done");
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
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			ByteBuf c = Unpooled.buffer();
			c.writeInt(77);
			Buffer b = new Buffer(c);
			peers[50].setRawDataReply(new RawDataReply() {
				@Override
				public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) {
					System.err.println("got it");
					return requestBuffer;
				}
			});
			FutureDirect fd = master.sendDirect(peers[50].getPeerAddress()).setBuffer(b).start();
			fd.await();
			System.err.println("done1");
			Assert.assertEquals(true, fd.isSuccess());
			Assert.assertNull(fd.getBuffer());
			// int read = fd.getBuffer().readInt();
			// Assert.assertEquals(88, read);
			System.err.println("done2");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testObjectLoop() throws Exception {
		for (int i = 0; i < 1000; i++) {
			System.err.println("nr: " + i);
			testObject();
		}
	}

	@Test
	public void testObject() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			String toStore1 = "hallo1";
			String toStore2 = "hallo2";
			Data data1 = new Data(toStore1);
			Data data2 = new Data(toStore2);
			System.err.println("begin add : ");
			FuturePut fput = peers[30].add(nr).setData(data1).start();
			fput.awaitUninterruptibly();
			System.err.println("stop added: " + toStore1 + " (" + fput.isSuccess() + ")");
			fput = peers[50].add(nr).setData(data2).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			System.err.println("added: " + toStore2 + " (" + fput.isSuccess() + ")");
			FutureGet fget = peers[77].get(nr).setAll().start();
			fget.awaitUninterruptibly();
			fget.getFutureRequests().awaitUninterruptibly();
			if (!fget.isSuccess())
				System.err.println(fget.getFailedReason());
			Assert.assertEquals(true, fget.isSuccess());
			Assert.assertEquals(2, fget.getDataMap().size());
			System.err.println("got it");
		} finally {
			if (master != null) {
				master.shutdown().awaitListenersUninterruptibly();
			}
		}
	}

	@Test
	public void testMaintenanceInit() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRoutingIndirect(peers);
			// do testing

			PeerStatatistic peerStatatistic = master.getPeerBean().peerMap()
			        .nextForMaintenance(new ArrayList<PeerAddress>());
			Assert.assertNotEquals(master.getPeerAddress(), peerStatatistic.getPeerAddress());
			Thread.sleep(10000);
			System.err.println("DONE");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	@Ignore
	// test is not working
	public void testAddGetPermits() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Number160 nr = new Number160(rnd);
			List<FuturePut> list = new ArrayList<FuturePut>();
			for (int i = 0; i < peers.length; i++) {
				String toStore1 = "hallo" + i;
				Data data1 = new Data(toStore1.getBytes());
				FuturePut fput = peers[i].add(nr).setData(data1).start();
				list.add(fput);
			}
			for (FuturePut futureDHT : list) {
				futureDHT.awaitUninterruptibly();
				Assert.assertEquals(true, futureDHT.isSuccess());
			}
			System.err.println("DONE");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testhalt() throws Exception {
		Peer master1 = null;
		Peer master2 = null;
		Peer master3 = null;
		try {
			master1 = new PeerMaker(new Number160(rnd)).p2pId(1).ports(4001).makeAndListen();
			master2 = new PeerMaker(new Number160(rnd)).p2pId(1).ports(4002).makeAndListen();
			master3 = new PeerMaker(new Number160(rnd)).p2pId(1).ports(4003).makeAndListen();
			// perfect routing
			master1.getPeerBean().peerMap().peerFound(master2.getPeerAddress(), null);
			master1.getPeerBean().peerMap().peerFound(master3.getPeerAddress(), null);
			master2.getPeerBean().peerMap().peerFound(master1.getPeerAddress(), null);
			master2.getPeerBean().peerMap().peerFound(master3.getPeerAddress(), null);
			master3.getPeerBean().peerMap().peerFound(master1.getPeerAddress(), null);
			master3.getPeerBean().peerMap().peerFound(master2.getPeerAddress(), null);
			Number160 id = master2.getPeerID();
			Data data = new Data(new byte[44444]);
			RoutingConfiguration rc = new RoutingConfiguration(2, 10, 2);
			RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);

			FuturePut fput = master1.put(id).setData(new Number160(5), data).setDomainKey(Number160.createHash("test"))
			        .setRequestP2PConfiguration(pc).setRoutingConfiguration(rc).start();
			fput.awaitUninterruptibly();
			fput.getFutureRequests().awaitUninterruptibly();
			// Collection<Number160> tmp = new ArrayList<Number160>();
			// tmp.add(new Number160(5));

			//
			Assert.assertEquals(true, fput.isSuccess());
			// search top 3
			master2.shutdown().await();

			//

			FutureGet fget = master1.get(id).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc)
			        .setDomainKey(Number160.createHash("test")).setContentKey(new Number160(5)).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(true, fget.isSuccess());

			master1.getPeerBean().peerMap().peerFailed(master2.getPeerAddress(), FailReason.Shutdown);
			master3.getPeerBean().peerMap().peerFailed(master2.getPeerAddress(), FailReason.Shutdown);
			master2 = new PeerMaker(new Number160(rnd)).p2pId(1).ports(4002).makeAndListen();
			master1.getPeerBean().peerMap().peerFound(master2.getPeerAddress(), null);
			master3.getPeerBean().peerMap().peerFound(master2.getPeerAddress(), null);
			master2.getPeerBean().peerMap().peerFound(master1.getPeerAddress(), null);
			master2.getPeerBean().peerMap().peerFound(master3.getPeerAddress(), null);

			System.err.println("no more exceptions here!!");

			fget = master1.get(id).setRoutingConfiguration(rc).setRequestP2PConfiguration(pc)
			        .setDomainKey(Number160.createHash("test")).setContentKey(new Number160(5)).start();
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
			p1 = new PeerMaker(new Number160(rnd)).ports(4001).makeAndListen();
			p2 = new PeerMaker(new Number160(rnd)).ports(4002).makeAndListen();
			// attach reply handler
			p2.setObjectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					System.out.println("request [" + request + "]");
					return "world";
				}
			});
			FutureDirect futureData = p1.sendDirect(p2.getPeerAddress()).setObject("hello").start();
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
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(500, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			for (int i = 0; i < peers.length; i++) {
				System.err.println("node " + i);
				peers[i].setObjectDataReply(new ObjectDataReply() {
					@Override
					public Object reply(PeerAddress sender, Object request) throws Exception {
						return request;
					}
				});
				peers[i].setRawDataReply(new RawDataReply() {
					@Override
					public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception {
						return requestBuffer;
					}
				});
			}
			// do testing
			System.err.println("round start");
			Random rnd = new Random(42L);
			byte[] toStore1 = new byte[10 * 1024];
			for (int j = 0; j < 5; j++) {
				System.err.println("round " + j);
				for (int i = 0; i < peers.length - 1; i++) {
					send1(peers[rnd.nextInt(peers.length)], peers[rnd.nextInt(peers.length)], toStore1, 100);
					send2(peers[rnd.nextInt(peers.length)], peers[rnd.nextInt(peers.length)],
					        Unpooled.wrappedBuffer(toStore1), 100);
					System.err.println("round1 " + i);
				}
			}
			System.err.println("DONE");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testKeys() throws Exception {
		final Random rnd = new Random(42L);
		Peer p1 = null;
		Peer p2 = null;
		try {
			Number160 n1 = new Number160(rnd);
			Data d1 = new Data("hello");
			Data d2 = new Data("world!");
			// setup (step 1)
			p1 = new PeerMaker(new Number160(rnd)).ports(4001).makeAndListen();
			FuturePut fput = p1.add(n1).setData(d1).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			p2 = new PeerMaker(new Number160(rnd)).ports(4002).makeAndListen();
			p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();
			// test (step 2)
			fput = p1.add(n1).setData(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			FutureGet fget = p2.get(n1).setAll().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(2, fget.getDataMap().size());
			// test (step 3)
			FutureRemove frem = p1.remove(n1).contentKey(d2.hash()).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			fget = p2.get(n1).setAll().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(1, fget.getDataMap().size());
			// test (step 4)
			fput = p1.add(n1).setData(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			fget = p2.get(n1).setAll().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(2, fget.getDataMap().size());
			// test (remove all)
			frem = p1.remove(n1).contentKey(d1.hash()).start();
			frem.awaitUninterruptibly();
			frem = p1.remove(n1).contentKey(d2.hash()).start();
			frem.awaitUninterruptibly();
			fget = p2.get(n1).setAll().start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(0, fget.getDataMap().size());
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
		Peer p1 = null;
		Peer p2 = null;
		try {
			Number160 n1 = new Number160(rnd);
			Number160 n2 = new Number160(rnd);
			Data d1 = new Data("hello");
			Data d2 = new Data("world!");
			// setup (step 1)
			p1 = new PeerMaker(new Number160(rnd)).ports(4001).makeAndListen();
			FuturePut fput = p1.put(n1).setData(d1).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			p2 = new PeerMaker(new Number160(rnd)).ports(4002).makeAndListen();
			p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();
			// test (step 2)
			fput = p1.put(n2).setData(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			FutureGet fget = p2.get(n2).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(1, fget.getDataMap().size());
			// test (step 3)
			FutureRemove frem = p1.remove(n2).start();
			frem.awaitUninterruptibly();
			Assert.assertEquals(true, frem.isSuccess());
			fget = p2.get(n2).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(0, fget.getDataMap().size());
			// test (step 4)
			fput = p1.put(n2).setData(d2).start();
			fput.awaitUninterruptibly();
			Assert.assertEquals(true, fput.isSuccess());
			fget = p2.get(n2).start();
			fget.awaitUninterruptibly();
			Assert.assertEquals(1, fget.getDataMap().size());
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
		Peer master = null;
		try {
			// setup
			final Peer[] peers = Utils2.createNodes(100, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
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
						peers[10].add(key).setData(data1).start().awaitUninterruptibly();
						peers[10].add(key).setData(data2).start().awaitUninterruptibly();
						Timings.sleepUninterruptibly(1000);
					}
				}
			});
			t.start();
			// wait until the first data is stored.
			Timings.sleep(1000);
			for (int i = 0; i < 30; i++) {
				FutureGet fget = peers[20 + i].get(key).setAll().start();
				fget.awaitUninterruptibly();
				Assert.assertEquals(2, fget.getDataMap().size());
				Timings.sleep(1000);
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
			Bindings b = new Bindings().addInterface("lo");
			p1 = new PeerMaker(new Number160(rnd)).ports(4001).externalBindings(b).makeAndListen();
			p2 = new PeerMaker(new Number160(rnd)).ports(4002).externalBindings(b).makeAndListen();
			FutureBootstrap fb = p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start();
			fb.awaitUninterruptibly();
			Assert.assertEquals(true, fb.isSuccess());
		} finally {
			if (p1 != null) {
				p1.shutdown().await();
			}
			if (p2 != null) {
				p2.shutdown().await();
			}

		}
	}

	// TODO: make this work
	@Test
	public void testBroadcast() throws Exception {
		Peer master = null;
		try {
			// setup
			Peer[] peers = Utils2.createNodes(1000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			master.broadcast(Number160.createHash("blub")).start();
			DefaultBroadcastHandler d = (DefaultBroadcastHandler) master.getBroadcastRPC().broadcastHandler();
			int counter = 0;
			while (d.getBroadcastCounter() < 900) {
				Thread.sleep(200);
				counter++;
				if (counter > 100) {
					System.err.println("did not broadcast to 1000 peers, but to " + d.getBroadcastCounter());
					Assert.fail("did not broadcast to 1000 peers, but to " + d.getBroadcastCounter());
				}
			}
			System.err.println("DONE");
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	/**
	 * Test the quit messages if they set a peer as offline.
	 * 
	 * @throws Exception .
	 */
	@Test
	public void testQuit() throws Exception {
		Peer master = null;
		try {
			// setup
			final int nrPeers = 200;
			final int port = 4001;
			Peer[] peers = Utils2.createNodes(nrPeers, rnd, port);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			final int peerTest = 10;
			System.err.println("counter: " + countOnline(peers, peers[peerTest].getPeerAddress()));
			FutureShutdown futureShutdown = peers[peerTest].announceShutdown().start();
			futureShutdown.awaitUninterruptibly();
			// we need to wait a bit, since the quit RPC is a fire and forget
			// and we return immediately
			Thread.sleep(2000);
			int counter = countOnline(peers, peers[peerTest].getPeerAddress());
			System.err.println("counter: " + counter);
			Assert.assertEquals(180, nrPeers - 20);
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	// TODO: make this work
	@Test
	@Ignore
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
			PeerMaker masterMaker = new PeerMaker(new Number160(rnd)).ports(4001);
			master = masterMaker.setEnableMaintenance(false).makeAndListen();
			PeerMaker slaveMaker = new PeerMaker(new Number160(rnd)).ports(4002);
			slave = slaveMaker.setEnableMaintenance(false).makeAndListen();

			System.err.println("peers up and running");

			slave.setRawDataReply(new RawDataReply() {
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
				FuturePeerConnection pc = master.createPeerConnection(slave.getPeerAddress());
				list1.add(master.sendDirect(pc).setBuffer(new Buffer(Unpooled.wrappedBuffer(b))).start());
				list3.add(pc);
				// pc.close();
			}
			for (int i = 0; i < 20000; i++) {
				list2.add(master.discover().peerAddress(slave.getPeerAddress()).start());
				final byte[] b = new byte[10000];
				byte[] me = Utils.intToByteArray(i);
				System.arraycopy(me, 0, b, 0, 4);
				list2.add(master.sendDirect(slave.getPeerAddress()).setBuffer(new Buffer(Unpooled.wrappedBuffer(b)))
				        .start());
			}
			for (BaseFuture bf : list1) {
				bf.awaitListenersUninterruptibly();
				if (bf.isFailed()) {
					System.err.println("WTF " + bf.getFailedReason());
				} else {
					System.err.print(",");
				}
				Assert.assertEquals(true, bf.isSuccess());
			}
			for (FuturePeerConnection pc : list3) {
				pc.close().awaitListenersUninterruptibly();
			}
			for (BaseFuture bf : list2) {
				bf.awaitListenersUninterruptibly();
				if (bf.isFailed()) {
					System.err.println("WTF " + bf.getFailedReason());
				} else {
					System.err.print(".");
				}
				Assert.assertEquals(true, bf.isSuccess());
			}
			System.err.println("done!!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.err.println("done!1!");
			if (master != null) {
				master.shutdown().await();
			}
			if (slave != null) {
				slave.shutdown().await();
			}
		}
	}

	@Test
	public void testShutdown() throws Exception {
		Peer master = null;
		try {
			// setup
			Random rnd = new Random();
			final int nrPeers = 10;
			final int port = 4001;
			// Peer[] peers = Utils2.createNodes(nrPeers, rnd, port);
			// Peer[] peers =
			// createAndAttachNodesWithReplicationShortId(nrPeers, port);
			// Peer[] peers = createNodes(nrPeers, rnd, port, null, true);
			Peer[] peers = createNodesWithShortId(nrPeers, rnd, port, null, true);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing

			final int peerTest = 3;
			peers[peerTest].put(Number160.createHash(1000)).setData(new Data("Test")).start().awaitUninterruptibly();

			for (int i = 0; i < nrPeers; i++) {
				for (Data d : peers[i].getPeerBean().storage().get().values())
					System.out.println("peer[" + i + "]: " + d.object().toString() + " ");
			}

			FutureShutdown futureShutdown = peers[peerTest].announceShutdown().start();
			futureShutdown.awaitUninterruptibly();
			// we need to wait a bit, since the quit RPC is a fire and forget
			// and we return immediately
			Thread.sleep(2000);
			peers[peerTest].shutdown().awaitUninterruptibly();
			System.out.println("peer " + peerTest + " is shutdown");
			for (int i = 0; i < nrPeers; i++) {
				for (Data d : peers[i].getPeerBean().storage().get().values())
					System.out.println("peer[" + i + "]: " + d.object().toString() + " ");
			}
		} finally {
			if (master != null) {
				master.shutdown().await();
			}
		}
	}

	@Test
	public void testThreads() throws Exception {
		Peer master = null;
		Peer slave = null;
		try {

			DefaultEventExecutorGroup eventExecutorGroup = new DefaultEventExecutorGroup(250);
			ChannelClientConfiguration ccc1 = PeerMaker.createDefaultChannelClientConfiguration();
			ccc1.pipelineFilter(new PeerMaker.EventExecutorGroupFilter(eventExecutorGroup));

			ChannelServerConficuration ccs1 = PeerMaker.createDefaultChannelServerConfiguration();
			ccs1.pipelineFilter(new PeerMaker.EventExecutorGroupFilter(eventExecutorGroup));

			master = new PeerMaker(new Number160(rnd)).ports(4001).channelClientConfiguration(ccc1)
			        .channelServerConfiguration(ccs1).makeAndListen();
			slave = new PeerMaker(new Number160(rnd)).ports(4002).channelClientConfiguration(ccc1)
			        .channelServerConfiguration(ccs1).makeAndListen();

			master.bootstrap().setPeerAddress(slave.getPeerAddress()).start().awaitUninterruptibly();
			slave.bootstrap().setPeerAddress(master.getPeerAddress()).start().awaitUninterruptibly();

			System.err.println("peers up and running");
			
			final int count = 100;
			final CountDownLatch latch = new CountDownLatch(count);
			final AtomicBoolean correct = new AtomicBoolean(true);

			for (int i = 0; i < count; i++) {
				FuturePut futurePut = master.put(Number160.ONE).setData(new Data("test")).start();
				futurePut.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						Thread.sleep(1000);
						latch.countDown();
						System.err.println("block in "+Thread.currentThread().getName());
						if(!Thread.currentThread().getName().contains("EventExecutorGroup")) {
							correct.set(false);
						}
					}
				});
			}
			latch.await(10, TimeUnit.SECONDS);
			Assert.assertTrue(correct.get());

		} finally {
			System.err.println("done!1!");
			if (master != null) {
				master.shutdown().await();
			}
			if (slave != null) {
				slave.shutdown().await();
			}
		}
	}
	
	@Test
	public void removeFromToTest3() throws IOException, ClassNotFoundException {
		Peer p1 = new PeerMaker(Number160.createHash(1)).setEnableIndirectReplication(true).ports(5000).makeAndListen();
		Peer p2 = new PeerMaker(Number160.createHash(2)).setEnableIndirectReplication(true).masterPeer(p1)
		        .makeAndListen();
		p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();
		p1.bootstrap().setPeerAddress(p2.getPeerAddress()).start().awaitUninterruptibly();
		Number160 lKey = Number160.createHash("location");
		Number160 dKey = Number160.createHash("domain");
		Number160 cKey = Number160.createHash("content");
		String data = "test";
		p2.put(lKey).setData(cKey, new Data(data)).setDomainKey(dKey).start().awaitUninterruptibly();
		FutureRemove futureRemove = p1.remove(lKey).setDomainKey(dKey).contentKey(cKey).start();
		futureRemove.awaitUninterruptibly();
		// check with a normal digest
		FutureDigest futureDigest = p1.digest(lKey).setContentKey(cKey).setDomainKey(dKey).start();
		futureDigest.awaitUninterruptibly();
		Assert.assertTrue(futureDigest.getDigest().keyDigest().isEmpty());
		// check with a from/to digest
		futureDigest = p1.digest(lKey).from(new Number640(lKey, dKey, cKey, Number160.ZERO))
		        .to(new Number640(lKey, dKey, cKey, Number160.MAX_VALUE)).start();
		futureDigest.awaitUninterruptibly();
		Assert.assertTrue(futureDigest.getDigest().keyDigest().isEmpty());
		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}
	

	@Test
	public void removeFromToTest4() throws IOException, ClassNotFoundException {
		Peer p1 = new PeerMaker(Number160.createHash(1)).setEnableIndirectReplication(true).ports(5000).makeAndListen();
		Peer p2 = new PeerMaker(Number160.createHash(2)).setEnableIndirectReplication(true).masterPeer(p1)
		        .makeAndListen();
		p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();
		p1.bootstrap().setPeerAddress(p2.getPeerAddress()).start().awaitUninterruptibly();
		Number160 lKey = Number160.createHash("location");
		Number160 dKey = Number160.createHash("domain");
		Number160 cKey = Number160.createHash("content");
		String data = "test";
		p2.put(lKey).setData(cKey, new Data(data)).setDomainKey(dKey).start().awaitUninterruptibly();
		FutureRemove futureRemove = p1.remove(lKey).from(new Number640(lKey, dKey, cKey, Number160.ZERO))
		        .to(new Number640(lKey, dKey, cKey, Number160.MAX_VALUE)).start();
		futureRemove.awaitUninterruptibly();
		FutureDigest futureDigest = p1.digest(lKey).from(new Number640(lKey, dKey, cKey, Number160.ZERO))
		        .to(new Number640(lKey, dKey, cKey, Number160.MAX_VALUE)).start();
		futureDigest.awaitUninterruptibly();
		// should be empty
		Assert.assertTrue(futureDigest.getDigest().keyDigest().isEmpty());
		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}

	public static Peer[] createNodesWithShortId(int nrOfPeers, Random rnd, int port, AutomaticFuture automaticFuture,
	        boolean replication) throws Exception {
		if (nrOfPeers < 1) {
			throw new IllegalArgumentException("Cannot create less than 1 peer");
		}
		Peer[] peers = new Peer[nrOfPeers];
		if (automaticFuture != null) {
			peers[0] = new PeerMaker(new Number160(1111)).setEnableIndirectReplication(replication)
			        .addAutomaticFuture(automaticFuture).ports(port).makeAndListen();
		} else {
			peers[0] = new PeerMaker(new Number160(1111)).setEnableIndirectReplication(replication).ports(port)
			        .makeAndListen();
		}

		for (int i = 1; i < nrOfPeers; i++) {
			if (automaticFuture != null) {
				peers[i] = new PeerMaker(new Number160(i)).setEnableIndirectReplication(replication)
				        .addAutomaticFuture(automaticFuture).masterPeer(peers[0]).makeAndListen();
			} else {
				peers[i] = new PeerMaker(new Number160(i)).setEnableIndirectReplication(replication)
				        .masterPeer(peers[0]).makeAndListen();
			}
		}
		System.err.println("peers created.");
		return peers;
	}

	private static int countOnline(Peer[] peers, PeerAddress peerAddress) {
		int counter = 0;
		for (Peer peer : peers) {
			if (peer.getPeerBean().peerMap().contains(peerAddress)) {
				counter++;
			}
		}
		return counter;
	}

	private void send2(final Peer p1, final Peer p2, final ByteBuf toStore1, final int count) throws IOException {
		if (count == 0) {
			return;
		}
		Buffer b = new Buffer(toStore1);
		FutureDirect fd = p1.sendDirect(p2.getPeerAddress()).setBuffer(b).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {
			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				if (future.isFailed()) {
					// System.err.println(future.getFailedReason());
					send2(p1, p2, toStore1, count - 1);
				}
			}
		});
	}

	private void send1(final Peer p1, final Peer p2, final byte[] toStore1, final int count) throws IOException {
		if (count == 0) {
			return;
		}
		FutureDirect fd = p1.sendDirect(p2.getPeerAddress()).setObject(toStore1).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {
			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				if (future.isFailed()) {
					//System.err.println(future.getFailedReason());
					send1(p1, p2, toStore1, count - 1);
				}
			}
		});
	}

	private void testForArray(Peer peer, Number160 locationKey, boolean find) {
		Collection<Number160> tmp = new ArrayList<Number160>();
		tmp.add(new Number160(5));
		Number640 min = new Number640(locationKey, Number160.createHash("test"), Number160.ZERO, Number160.ZERO);
		Number640 max = new Number640(locationKey, Number160.createHash("test"), Number160.MAX_VALUE,
		        Number160.MAX_VALUE);
		Map<Number640, Data> test = peer.getPeerBean().storage().get(min, max, -1, true);
		if (find) {
			Assert.assertEquals(1, test.size());
			Assert.assertEquals(
			        44444,
			        test.get(
			                new Number640(new Number320(locationKey, Number160.createHash("test")), new Number160(5),
			                        Number160.ZERO)).length());
		} else
			Assert.assertEquals(0, test.size());
	}
}
