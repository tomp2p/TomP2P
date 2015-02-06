package net.tomp2p.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DataBuffer;
import net.tomp2p.synchronization.Checksum;
import net.tomp2p.synchronization.Instruction;
import net.tomp2p.synchronization.PeerSync;
import net.tomp2p.synchronization.RSync;
import net.tomp2p.synchronization.SyncBuilder;
import net.tomp2p.synchronization.SyncStat;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronizationTest {

	private final static Random random = new Random(42);
	private static final Logger LOG = LoggerFactory.getLogger(SynchronizationTest.class);

	@Test
	public void testAdler() {
		Random rnd = new Random(42);
		for (int i = 0; i < 100; i++) {
			byte[] test = new byte[1000+i];
			rnd.nextBytes(test);
			int start = rnd.nextInt(test.length);
			int len = rnd.nextInt(test.length - start);			
			RSync.RollingChecksum a = new RSync.RollingChecksum();
			a.update(test, start, len);
			int val = a.value();
			a.updateRolling(test);
			val = a.value();
			a.reset();
			a.update(test, start+1, len);
			int val2 = a.value();
			Assert.assertEquals(val, val2);
		}
	}

	@Test
	public void testGetMD5() throws IOException, NoSuchAlgorithmException {
		String block = "The quick brown fox jumps over the lazy dog";
		String expected = "9e107d9d372bb6826bd81d3542a419d6";
		BigInteger bi = new BigInteger(1, Utils.makeMD5Hash(block.getBytes()));
		assertArrayEquals(expected.getBytes(), bi.toString(16).getBytes());
	}

	@Test
	public void testGetChecksums() throws NoSuchAlgorithmException, IOException {
		Random rnd = new Random(42);
		for (int i = 0; i < 100; i++) {
			byte[] test = new byte[1000*i];
			rnd.nextBytes(test);
			List<Checksum> list = RSync.checksums(test, 100);
			double t =  test.length / 100d;
			Assert.assertEquals(list.size()	, (int) Math.floor(t));
		}
	}

	@Test
	public void testGetReconstructedValueStatic0() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		int size = 6;
		byte[] oldValue = "ZurichGenevaLuganoAAA".getBytes();
		byte[] newValue = "AzurichGenevaLuganoAbbLuganoAAA".getBytes();
		List<Checksum> checksums = RSync.checksums(oldValue, size);
		List<Instruction> instructions = RSync.instructions(newValue, checksums, size);
		DataBuffer reconstructedValue = RSync.reconstruct(oldValue, instructions, size);
		Assert.assertArrayEquals(newValue, reconstructedValue.bytes());
	}
	
	@Test
	public void testGetReconstructedValueStatic1() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		int size = 6;
		byte[] oldValue = "ZurichGenevaLuganoAA".getBytes();
		byte[] newValue = "AzurichGenevaLuganoAbbLuganoAAA".getBytes();
		List<Checksum> checksums = RSync.checksums(oldValue, size);
		List<Instruction> instructions = RSync.instructions(newValue, checksums, size);
		DataBuffer reconstructedValue = RSync.reconstruct(oldValue, instructions, size);
		Assert.assertArrayEquals(newValue, reconstructedValue.bytes());
	}
	
	@Test
	public void testGetReconstructedValueStatic2() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		int size = 6;
		byte[] oldValue = "ZurichGenevaLuganoAAA".getBytes();
		byte[] newValue = "AzurichGenevaLuganoAbbLuganoAA".getBytes();
		List<Checksum> checksums = RSync.checksums(oldValue, size);
		List<Instruction> instructions = RSync.instructions(newValue, checksums, size);
		DataBuffer reconstructedValue = RSync.reconstruct(oldValue, instructions, size);
		Assert.assertArrayEquals(newValue, reconstructedValue.bytes());
	}

	@Test
	public void testGetReconstructedValueStatic3() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		int size = 5;
		byte[] newValue = "Test1Test2Test3Test4".getBytes();
		byte[] oldValue = "test0Test2test0Test4".getBytes();
		List<Checksum> checksums = RSync.checksums(oldValue, size);
		List<Instruction> instructions = RSync.instructions(newValue, checksums, size);

		Assert.assertEquals(4, instructions.size());
		DataBuffer reconstructedValue = RSync.reconstruct(oldValue, instructions, size);
		Assert.assertArrayEquals(newValue, reconstructedValue.bytes());
	}

	@Test
	public void testGetReconstructedValueDynamic() throws IOException {
		for (int i = 0; i < 1000; i++) {
			testGetReconstructedValueDynamic0(i);
		}
	}

	private void testGetReconstructedValueDynamic0(int counter) throws IOException {

		int k = 20 + counter;
		// character types number, m different characters are used to construct content
		int m = 3; 

		// number of changes, so l characters of content will be changed
		int l = random.nextInt(21); 
		int size = random.nextInt(20) + 1;

		System.out.print("character types: " + m + " - ");
		for (int i = 0; i < 3; i++) {
			System.out.print(" " + (char) (i + 65));
		}
		LOG.debug("changes: " + l);
		LOG.debug("content size: " + k);
		LOG.debug("block size: " + size);

		String oldValue = "";
		StringBuilder sb = new StringBuilder(k);
		for (int i = 0; i < k; i++) {
			int temp = random.nextInt(m);
			sb.append((char) (temp + 65));
		}
		oldValue = sb.toString();
		LOG.debug("oldvalue.length=" + oldValue.length());
		LOG.debug("old value: " + oldValue);

		String newValue = oldValue;
		for (int i = 0; i < l; i++) {
			int temp = random.nextInt(k);
			StringBuilder sb1 = new StringBuilder(newValue);
			sb1.setCharAt(temp, 'X');
			newValue = sb1.toString();
		}
		LOG.debug("new value: " + newValue);

		List<Checksum> checksums = RSync.checksums(oldValue.getBytes(), size);
		List<Instruction> instructions = RSync.instructions(newValue.getBytes(), checksums, size);
		LOG.debug("checksums(" + checksums.size() + "): " + checksums);
		LOG.debug("instructions(" + instructions.size() + "): " + instructions);

		DataBuffer reconstructedValue = RSync.reconstruct(oldValue.getBytes(), instructions, size);

		Assert.assertArrayEquals(newValue.getBytes(), reconstructedValue.bytes());
	}

	@Test
	public void testInfoMessageSAME() throws IOException, InterruptedException {

		PeerDHT sender = null;
		PeerDHT receiver = null;
		try {
			final AtomicReference<Type> ref = new AtomicReference<Type>(Type.UNKNOWN_ID);
			final AtomicReference<DataMap> ref2 = new AtomicReference<DataMap>();

			//final ReplicationSync syncSender = new ReplicationSync(5);
			sender = new PeerBuilderDHT(new PeerBuilder(new Number160(1)).ports(4001).start()).start();
			//final ReplicationSync syncReceiver = new ReplicationSync(5);
			receiver = new PeerBuilderDHT(new PeerBuilder(new Number160(2)).ports(4002).start()).start();
			final PeerSync senderSync = new PeerSync(sender, 5);
			new PeerSync(receiver, 5);

			final Number160 locationKey = new Number160(100);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			final String value = "Test";

			NavigableMap<Number640, Data> map = new TreeMap<Number640, Data>();
			final DataMap dataMap = new DataMap(map);
			map.put(new Number640(locationKey, domainKey, contentKey, Number160.ZERO), new Data("Test"));

			sender.put(locationKey).data(new Data(value)).start().awaitUninterruptibly();
			receiver.put(locationKey).data(new Data(value)).start().awaitUninterruptibly();

			sender.peer().bootstrap().peerAddress(receiver.peerAddress()).start().awaitUninterruptibly();

			FutureChannelCreator futureChannelCreator = sender.peer().connectionBean().reservation().create(0, 1);

			final CountDownLatch latch = new CountDownLatch(1);
			final PeerAddress receiverAddress = receiver.peerAddress();

			futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(final FutureChannelCreator future2) throws Exception {
					if (future2.isSuccess()) {
						SyncBuilder synchronizationBuilder = new SyncBuilder(senderSync, receiverAddress, 5);
						synchronizationBuilder.dataMap(dataMap);
						final FutureResponse futureResponse = senderSync.syncRPC().infoMessage(
						        receiverAddress, synchronizationBuilder, future2.channelCreator());
						futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
							@Override
							public void operationComplete(FutureResponse future) throws Exception {
								System.err.println(future.failedReason());
								ref.set(future.responseMessage().type());
								ref2.set(future.responseMessage().dataMap(0));
								Utils.addReleaseListener(future2.channelCreator(), futureResponse);
								latch.countDown();
							}
						});
					}
				}
			});
			latch.await();
			assertEquals(Type.OK, ref.get());
			assertEquals(1, ref2.get().size());
			assertEquals(0, ref2.get().dataMap().values().iterator().next().length());
			assertEquals(true, ref2.get().dataMap().values().iterator().next().isFlag1());
		} finally {
			if (sender != null) {
				sender.shutdown().await();
			}
			if (receiver != null) {
				receiver.shutdown().await();
			}
		}
	}

	@Test
	public void testInfoMessageNO() throws IOException, InterruptedException {

		PeerDHT sender = null;
		PeerDHT receiver = null;
		try {
			final AtomicReference<DataMap> ref = new AtomicReference<DataMap>();

			sender = new PeerBuilderDHT(new PeerBuilder(new Number160(3)).ports(4003).start()).start();
			receiver = new PeerBuilderDHT(new PeerBuilder(new Number160(4)).ports(4004).start()).start();
			final PeerSync senderSync = new PeerSync(sender, 5);
			new PeerSync(receiver, 5);

			final Number160 locationKey = new Number160(200);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			final String value = "Test";

			NavigableMap<Number640, Data> map = new TreeMap<Number640, Data>();
			final DataMap dataMap = new DataMap(map);
			map.put(new Number640(locationKey, domainKey, contentKey, Number160.ZERO), new Data("Test"));

			sender.put(locationKey).data(new Data(value)).start().awaitUninterruptibly();

			sender.peer().bootstrap().peerAddress(receiver.peerAddress()).start().awaitUninterruptibly();
			final CountDownLatch latch = new CountDownLatch(1);
			final PeerAddress receiverAddress = receiver.peerAddress();
			FutureChannelCreator futureChannelCreator = sender.peer().connectionBean().reservation().create(0, 1);
			futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(final FutureChannelCreator future2) throws Exception {
					if (future2.isSuccess()) {
						SyncBuilder synchronizationBuilder = new SyncBuilder(senderSync, receiverAddress, 5);
						synchronizationBuilder.dataMap(dataMap);
						final FutureResponse futureResponse = senderSync.syncRPC().infoMessage(
						        receiverAddress, synchronizationBuilder, future2.channelCreator());
						futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
							@Override
							public void operationComplete(FutureResponse future) throws Exception {
								ref.set(future.responseMessage().dataMap(0));
								Utils.addReleaseListener(future2.channelCreator(), futureResponse);
								latch.countDown();
							}
						});
					}
				}
			});

			latch.await();
			assertEquals(1, ref.get().size());
			assertEquals(0, ref.get().dataMap().values().iterator().next().length());
			assertEquals(true, ref.get().dataMap().values().iterator().next().isFlag2());
		} finally {
			if (sender != null) {
				sender.shutdown().await();
			}
			if (receiver != null) {
				receiver.shutdown().await();
			}
		}
	}

	@Test
	public void testInfoMessageNOTSAME() throws IOException, InterruptedException {

		PeerDHT sender = null;
		PeerDHT receiver = null;
		try {
			final AtomicReference<DataMap> ref = new AtomicReference<DataMap>();

			sender = new PeerBuilderDHT(new PeerBuilder(new Number160(3)).ports(4003).start()).start();

			receiver = new PeerBuilderDHT(new PeerBuilder(new Number160(4)).ports(4004).start()).start();

			final PeerSync senderSync = new PeerSync(sender, 5);
			new PeerSync(receiver, 5);

			final Number160 locationKey = new Number160(300);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;

			final String value = "Test";
			final String value1 = "Test1";

			sender.put(locationKey).data(new Data(value)).start().awaitUninterruptibly();
			receiver.put(locationKey).data(new Data(value1)).start().awaitUninterruptibly();

			NavigableMap<Number640, Data> map = new TreeMap<Number640, Data>();
			final DataMap dataMap = new DataMap(map);
			map.put(new Number640(locationKey, domainKey, contentKey, Number160.ZERO), new Data("Test"));

			sender.peer().bootstrap().peerAddress(receiver.peerAddress()).start().awaitUninterruptibly();

			final CountDownLatch latch = new CountDownLatch(1);
			final PeerAddress receiverAddress = receiver.peerAddress();

			FutureChannelCreator futureChannelCreator = sender.peer().connectionBean().reservation().create(0, 1);
			futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(final FutureChannelCreator future2) throws Exception {
					if (future2.isSuccess()) {
						SyncBuilder synchronizationBuilder = new SyncBuilder(senderSync, receiverAddress, 5);
						synchronizationBuilder.dataMap(dataMap);
						final FutureResponse futureResponse = senderSync.syncRPC().infoMessage(receiverAddress,
						        synchronizationBuilder, future2.channelCreator());
						futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
							@Override
							public void operationComplete(FutureResponse future) throws Exception {
								ref.set(future.responseMessage().dataMap(0));
								Utils.addReleaseListener(future2.channelCreator(), futureResponse);
								latch.countDown();
							}
						});
					}
				}
			});

			latch.await();
			assertEquals(1, ref.get().size());
			assertEquals(100, ref.get().dataMap().values().iterator().next().toBytes().length);
			assertEquals(false, ref.get().dataMap().values().iterator().next().isFlag1());
			assertEquals(false, ref.get().dataMap().values().iterator().next().isFlag2());

		} finally {
			if (sender != null) {
				sender.shutdown().await();
			}
			if (receiver != null) {
				receiver.shutdown().await();
			}
		}
	}

	@Test
	public void testSyncMessageDiff() throws IOException, InterruptedException, ClassNotFoundException {
		PeerDHT sender = null;
		PeerDHT receiver = null;
		try {
			sender = new PeerBuilderDHT(new PeerBuilder(new Number160(3)).ports(4003).start()).start();
			receiver = new PeerBuilderDHT(new PeerBuilder(new Number160(4)).ports(4004).start()).start();

			final PeerSync senderSync = new PeerSync(sender, 5);
			new PeerSync(receiver, 5);

			final Number160 locationKey = new Number160(500);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
			final String newValue = "Test1Test2Test3Test4";
			final String oldValue = "test0Test2test0Test4";

			Data test1 = new Data(newValue.getBytes());
			Data test2 = new Data(oldValue.getBytes());

			sender.put(locationKey).data(test1).start().awaitUninterruptibly();
			receiver.put(locationKey).data(test2).start().awaitUninterruptibly();

			FutureDone<SyncStat> future = senderSync.synchronize(receiver.peerAddress()).key(key)
			        .start();
			future.awaitUninterruptibly();

			System.err.println(future.object().toString());
			Data data = receiver.storageLayer()
			        .get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
			byte[] reconstructedValue = data.toBytes();

			assertArrayEquals(newValue.getBytes(), reconstructedValue);
			Assert.assertEquals(20, ((SyncStat)future.object()).dataOrig());
			Assert.assertEquals(26, ((SyncStat)future.object()).dataCopy());
		} finally {
			if (sender != null) {
				sender.shutdown().awaitUninterruptibly();
			}
			if (receiver != null) {
				receiver.shutdown().awaitUninterruptibly();
			}
		}
	}
	
	@Test
	public void testSyncMessageDiff2() throws IOException, InterruptedException, ClassNotFoundException {
		PeerDHT sender = null;
		PeerDHT receiver = null;
		try {
			sender = new PeerBuilderDHT(new PeerBuilder(new Number160(3)).ports(4003).start()).start();
			receiver = new PeerBuilderDHT(new PeerBuilder(new Number160(4)).ports(4004).start()).start();

			final PeerSync senderSync = new PeerSync(sender, 32);
			new PeerSync(receiver, 32);
			

			final Number160 locationKey = new Number160(500);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
			final String newValue = "TomP2P 5 is around the corner with several new additions. One of the larger changes is the support for relays as described here. Check out the latest alpha version.";
			final String oldValue = "TomP2P 5 is around the corner with several new additions! One of the larger changes is the support for relays as described here. Check out the latest alpha version.";

			Data test1 = new Data(newValue.getBytes());
			Data test2 = new Data(oldValue.getBytes());

			sender.put(locationKey).data(test1).start().awaitUninterruptibly();
			receiver.put(locationKey).data(test2).start().awaitUninterruptibly();

			FutureDone<SyncStat> future = senderSync.synchronize(receiver.peerAddress()).key(key)
			        .start();
			future.awaitUninterruptibly();

			System.err.println(future.object().toString());
			Data data = receiver.storageLayer()
			        .get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
			byte[] reconstructedValue = data.toBytes();

			assertArrayEquals(newValue.getBytes(), reconstructedValue);
			Assert.assertEquals(164, ((SyncStat)future.object()).dataOrig());
			Assert.assertEquals(56, ((SyncStat)future.object()).dataCopy());
		} finally {
			if (sender != null) {
				sender.shutdown().awaitUninterruptibly();
			}
			if (receiver != null) {
				receiver.shutdown().awaitUninterruptibly();
			}
		}
	}

	@Test
	public void testSyncMessageSame() throws IOException, InterruptedException, ClassNotFoundException {
		PeerDHT sender = null;
		PeerDHT receiver = null;
		try {
			sender = new PeerBuilderDHT(new PeerBuilder(new Number160(3)).ports(4003).start()).start();
			receiver = new PeerBuilderDHT(new PeerBuilder(new Number160(4)).ports(4004).start()).start();
			
			final PeerSync senderSync = new PeerSync(sender, 5);
			new PeerSync(receiver, 5);

			final Number160 locationKey = new Number160(500);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
			final String newValue = "Test1Test2Test3Test4";
			final String oldValue = "Test1Test2Test3Test4";

			Data test1 = new Data(newValue.getBytes());
			Data test2 = new Data(oldValue.getBytes());

			sender.put(locationKey).data(test1).start().awaitUninterruptibly();
			receiver.put(locationKey).data(test2).start().awaitUninterruptibly();

			FutureDone<SyncStat> future = senderSync.synchronize(receiver.peerAddress()).key(key)
			        .start();
			future.awaitUninterruptibly();

			System.err.println(future.object().toString());
			Data data = receiver.storageLayer()
			        .get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
			byte[] reconstructedValue = data.toBytes();

			assertArrayEquals(newValue.getBytes(), reconstructedValue);
			
			Assert.assertEquals(0, ((SyncStat)future.object()).dataOrig());
			Assert.assertEquals(0, ((SyncStat)future.object()).dataCopy());
		} finally {
			if (sender != null) {
				sender.shutdown().awaitUninterruptibly();
			}
			if (receiver != null) {
				receiver.shutdown().awaitUninterruptibly();
			}
		}
	}

	@Test
	public void testSyncMessageCopy() throws IOException, InterruptedException, ClassNotFoundException {
		PeerDHT sender = null;
		PeerDHT receiver = null;
		try {
			sender = new PeerBuilderDHT(new PeerBuilder(new Number160(3)).ports(4003).start()).start();
			receiver = new PeerBuilderDHT(new PeerBuilder(new Number160(4)).ports(4004).start()).start();

			final PeerSync senderSync = new PeerSync(sender, 5);
			new PeerSync(receiver, 5);

			final Number160 locationKey = new Number160(600);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
			final String newValue = "Test1Test2Test3Test4";

			Data test1 = new Data(newValue.getBytes());

			sender.put(locationKey).data(test1).start().awaitUninterruptibly();

			FutureDone<SyncStat> future = senderSync.synchronize(receiver.peerAddress()).key(key)
			        .start();
			future.awaitUninterruptibly();

			System.err.println(future.object().toString());

			Data data = receiver.storageLayer()
			        .get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
			byte[] reconstructedValue = data.toBytes();
			assertArrayEquals(newValue.getBytes(), reconstructedValue);
			Assert.assertEquals(20, ((SyncStat)future.object()).dataOrig());
			Assert.assertEquals(20, ((SyncStat)future.object()).dataCopy());
		} finally {
			if (sender != null) {
				sender.shutdown().awaitUninterruptibly();
			}
			if (receiver != null) {
				receiver.shutdown().awaitUninterruptibly();
			}
		}
	}
}
