package net.tomp2p.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DataBuffer;
import net.tomp2p.synchronization.RollingChecksum;
import net.tomp2p.synchronization.Checksum;
import net.tomp2p.synchronization.Instruction;
import net.tomp2p.synchronization.PeerSync;
import net.tomp2p.synchronization.SyncSender;
import net.tomp2p.synchronization.Synchronization;
import net.tomp2p.synchronization.SynchronizationDirectBuilder;
import net.tomp2p.synchronization.SynchronizationStatistics;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class SynchronizationTest {

	private final static Random random = new Random(42);

	@Test
	public void testAdler() {
		Random rnd = new Random(42);
		for (int i = 0; i < 100; i++) {
			byte[] test = new byte[1000+i];
			rnd.nextBytes(test);
			int start = rnd.nextInt(test.length);
			int len = rnd.nextInt(test.length - start);			
			RollingChecksum a = new RollingChecksum();
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
			List<Checksum> list = Synchronization.checksums(test, 100);
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
		List<Checksum> checksums = Synchronization.checksums(oldValue, size);
		List<Instruction> instructions = Synchronization.instructions(newValue, checksums, size);
		DataBuffer reconstructedValue = Synchronization.reconstruct(oldValue, instructions, size);
		Assert.assertArrayEquals(newValue, reconstructedValue.bytes());
	}
	
	@Test
	public void testGetReconstructedValueStatic1() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		int size = 6;
		byte[] oldValue = "ZurichGenevaLuganoAA".getBytes();
		byte[] newValue = "AzurichGenevaLuganoAbbLuganoAAA".getBytes();
		List<Checksum> checksums = Synchronization.checksums(oldValue, size);
		List<Instruction> instructions = Synchronization.instructions(newValue, checksums, size);
		DataBuffer reconstructedValue = Synchronization.reconstruct(oldValue, instructions, size);
		Assert.assertArrayEquals(newValue, reconstructedValue.bytes());
	}
	
	@Test
	public void testGetReconstructedValueStatic2() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		int size = 6;
		byte[] oldValue = "ZurichGenevaLuganoAAA".getBytes();
		byte[] newValue = "AzurichGenevaLuganoAbbLuganoAA".getBytes();
		List<Checksum> checksums = Synchronization.checksums(oldValue, size);
		List<Instruction> instructions = Synchronization.instructions(newValue, checksums, size);
		DataBuffer reconstructedValue = Synchronization.reconstruct(oldValue, instructions, size);
		Assert.assertArrayEquals(newValue, reconstructedValue.bytes());
	}

	@Test
	public void testGetReconstructedValueStatic3() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		int size = 5;
		byte[] newValue = "Test1Test2Test3Test4".getBytes();
		byte[] oldValue = "test0Test2test0Test4".getBytes();
		List<Checksum> checksums = Synchronization.checksums(oldValue, size);
		List<Instruction> instructions = Synchronization.instructions(newValue, checksums, size);

		Assert.assertEquals(4, instructions.size());
		DataBuffer reconstructedValue = Synchronization.reconstruct(oldValue, instructions, size);
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
		System.out.println();
		System.out.println("changes: " + l);
		System.out.println("content size: " + k);
		System.out.println("block size: " + size);

		String oldValue = "";
		StringBuilder sb = new StringBuilder(k);
		for (int i = 0; i < k; i++) {
			int temp = random.nextInt(m);
			sb.append((char) (temp + 65));
		}
		oldValue = sb.toString();
		System.out.println("oldvalue.length=" + oldValue.length());
		System.out.println("old value: " + oldValue);

		String newValue = oldValue;
		for (int i = 0; i < l; i++) {
			int temp = random.nextInt(k);
			StringBuilder sb1 = new StringBuilder(newValue);
			sb1.setCharAt(temp, 'X');
			newValue = sb1.toString();
		}
		System.out.println("new value: " + newValue);

		List<Checksum> checksums = Synchronization.checksums(oldValue.getBytes(), size);
		List<Instruction> instructions = Synchronization.instructions(newValue.getBytes(), checksums, size);
		System.out.println("checksums(" + checksums.size() + "): " + checksums);
		System.out.println("instructions(" + instructions.size() + "): " + instructions);

		DataBuffer reconstructedValue = Synchronization.reconstruct(oldValue.getBytes(), instructions, size);

		Assert.assertArrayEquals(newValue.getBytes(), reconstructedValue.bytes());
	}

	@Test
	public void testInfoMessageSAME() throws IOException, InterruptedException {

		final AtomicReference<Type> ref = new AtomicReference<Type>(Type.UNKNOWN_ID);
		final AtomicReference<DataMap> ref2 = new AtomicReference<DataMap>();

		final PeerSync senderSync = new PeerSync();
		final SyncSender syncSender = new SyncSender(senderSync);
		final Peer sender = new PeerMaker(new Number160(1)).ports(4001).replicationSender(syncSender).makeAndListen();
		final PeerSync receiverSync = new PeerSync();
		final SyncSender syncReceiver = new SyncSender(receiverSync);
		final Peer receiver = new PeerMaker(new Number160(2)).ports(4002).replicationSender(syncReceiver)
		        .makeAndListen();

		final Number160 locationKey = new Number160(100);
		final Number160 domainKey = Number160.ZERO;
		final Number160 contentKey = Number160.ZERO;
		final String value = "Test";

		HashMap<Number640, Data> map = new HashMap<Number640, Data>();
		final DataMap dataMap = new DataMap(map);
		map.put(new Number640(locationKey, domainKey, contentKey, Number160.ZERO), new Data("Test"));

		sender.put(locationKey).setData(new Data(value)).start().awaitUninterruptibly();
		receiver.put(locationKey).setData(new Data(value)).start().awaitUninterruptibly();

		sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();

		FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future2) throws Exception {
				if (future2.isSuccess()) {
					SynchronizationDirectBuilder synchronizationBuilder = new SynchronizationDirectBuilder(senderSync,
					        receiver.getPeerAddress());
					synchronizationBuilder.dataMap(dataMap);
					final FutureResponse futureResponse = senderSync.getSynchronizationRPC().infoMessage(
					        receiver.getPeerAddress(), synchronizationBuilder, future2.getChannelCreator());
					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
						public void operationComplete(FutureResponse future) throws Exception {
							System.err.println(future.getFailedReason());
							ref.set(future.getResponse().getType());
							ref2.set(future.getResponse().getDataMap(0));
							Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
						}
					});
				}
			}
		});
		Thread.sleep(500);
		assertEquals(Type.OK, ref.get());
		assertEquals(1, ref2.get().size());
		assertEquals(0, ref2.get().dataMap().values().iterator().next().toBytes()[0]);
	}

	@Test
	public void testInfoMessageNO() throws IOException, InterruptedException {

		final AtomicReference<DataMap> ref = new AtomicReference<DataMap>();

		final PeerSync senderSync = new PeerSync();
		final SyncSender syncSender = new SyncSender(senderSync);
		final Peer sender = new PeerMaker(new Number160(3)).ports(4003).replicationSender(syncSender).makeAndListen();
		final PeerSync receiverSync = new PeerSync();
		final SyncSender syncReceiver = new SyncSender(receiverSync);
		final Peer receiver = new PeerMaker(new Number160(4)).ports(4004).replicationSender(syncReceiver)
		        .makeAndListen();

		final Number160 locationKey = new Number160(200);
		final Number160 domainKey = Number160.ZERO;
		final Number160 contentKey = Number160.ZERO;
		final String value = "Test";

		HashMap<Number640, Data> map = new HashMap<Number640, Data>();
		final DataMap dataMap = new DataMap(map);
		map.put(new Number640(locationKey, domainKey, contentKey, Number160.ZERO), new Data("Test"));

		sender.put(locationKey).setData(new Data(value)).start().awaitUninterruptibly();

		sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();

		FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future2) throws Exception {
				if (future2.isSuccess()) {
					SynchronizationDirectBuilder synchronizationBuilder = new SynchronizationDirectBuilder(senderSync,
					        receiver.getPeerAddress());
					synchronizationBuilder.dataMap(dataMap);
					final FutureResponse futureResponse = senderSync.getSynchronizationRPC().infoMessage(
					        receiver.getPeerAddress(), synchronizationBuilder, future2.getChannelCreator());
					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
						public void operationComplete(FutureResponse future) throws Exception {
							ref.set(future.getResponse().getDataMap(0));
							Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
						}
					});
				}
			}
		});

		Thread.sleep(500);
		assertEquals(1, ref.get().size());
		assertEquals(1, ref.get().dataMap().values().iterator().next().toBytes()[0]);
	}

	@Test
	public void testInfoMessageNOTSAME() throws IOException, InterruptedException {

		final AtomicReference<DataMap> ref = new AtomicReference<DataMap>();

		final PeerSync senderSync = new PeerSync();
		final SyncSender syncSender = new SyncSender(senderSync);
		final Peer sender = new PeerMaker(new Number160(5)).ports(4005).replicationSender(syncSender).makeAndListen();
		final PeerSync receiverSync = new PeerSync();
		final SyncSender syncReceiver = new SyncSender(receiverSync);
		final Peer receiver = new PeerMaker(new Number160(6)).ports(4006).replicationSender(syncReceiver)
		        .makeAndListen();

		final Number160 locationKey = new Number160(300);
		final Number160 domainKey = Number160.ZERO;
		final Number160 contentKey = Number160.ZERO;

		final String value = "Test";
		final String value1 = "Test1";

		sender.put(locationKey).setData(new Data(value)).start().awaitUninterruptibly();
		receiver.put(locationKey).setData(new Data(value1)).start().awaitUninterruptibly();

		HashMap<Number640, Data> map = new HashMap<Number640, Data>();
		final DataMap dataMap = new DataMap(map);
		map.put(new Number640(locationKey, domainKey, contentKey, Number160.ZERO), new Data("Test"));

		sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();

		FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future2) throws Exception {
				if (future2.isSuccess()) {
					SynchronizationDirectBuilder synchronizationBuilder = new SynchronizationDirectBuilder(senderSync,
					        receiver.getPeerAddress());
					synchronizationBuilder.dataMap(dataMap);
					final FutureResponse futureResponse = senderSync.getSynchronizationRPC().infoMessage(
					        receiver.getPeerAddress(), synchronizationBuilder, future2.getChannelCreator());
					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
						public void operationComplete(FutureResponse future) throws Exception {
							ref.set(future.getResponse().getDataMap(0));
							Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
						}
					});
				}
			}
		});

		Thread.sleep(500);
		assertEquals(1, ref.get().size());
		assertEquals(0, ref.get().dataMap().values().iterator().next().toBytes()[0]);
	}

	@Test
	public void testSyncMessageDiff() throws IOException, InterruptedException, ClassNotFoundException {
		Peer sender = null;
		Peer receiver = null;
		try {
			final PeerSync senderSync = new PeerSync();
			final SyncSender syncSender = new SyncSender(senderSync);
			sender = new PeerMaker(new Number160(9)).ports(4009).replicationSender(syncSender).makeAndListen();
			final PeerSync receiverSync = new PeerSync();
			final SyncSender syncReceiver = new SyncSender(receiverSync);
			receiver = new PeerMaker(new Number160(10)).ports(4010).replicationSender(syncReceiver).makeAndListen();

			final Number160 locationKey = new Number160(500);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
			final String newValue = "Test1Test2Test3Test4";
			final String oldValue = "test0Test2test0Test4";

			Data test1 = new Data(newValue.getBytes());
			Data test2 = new Data(oldValue.getBytes());

			sender.put(locationKey).setData(test1).start().awaitUninterruptibly();
			receiver.put(locationKey).setData(test2).start().awaitUninterruptibly();

			FutureDone<SynchronizationStatistics> future = senderSync.synchronize(receiver.getPeerAddress()).key(key)
			        .start();
			future.awaitUninterruptibly();

			System.err.println(future.getObject().toString());
			Data data = receiver.getPeerBean().storage()
			        .get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
			byte[] reconstructedValue = data.toBytes();

			assertArrayEquals(newValue.getBytes(), reconstructedValue);
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
		Peer sender = null;
		Peer receiver = null;
		try {
			final PeerSync senderSync = new PeerSync();
			final SyncSender syncSender = new SyncSender(senderSync);
			sender = new PeerMaker(new Number160(9)).ports(4009).replicationSender(syncSender).makeAndListen();
			final PeerSync receiverSync = new PeerSync();
			final SyncSender syncReceiver = new SyncSender(receiverSync);
			receiver = new PeerMaker(new Number160(10)).ports(4010).replicationSender(syncReceiver).makeAndListen();

			final Number160 locationKey = new Number160(500);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
			final String newValue = "Test1Test2Test3Test4";
			final String oldValue = "Test1Test2Test3Test4";

			Data test1 = new Data(newValue.getBytes());
			Data test2 = new Data(oldValue.getBytes());

			sender.put(locationKey).setData(test1).start().awaitUninterruptibly();
			receiver.put(locationKey).setData(test2).start().awaitUninterruptibly();

			FutureDone<SynchronizationStatistics> future = senderSync.synchronize(receiver.getPeerAddress()).key(key)
			        .start();
			future.awaitUninterruptibly();

			System.err.println(future.getObject().toString());
			Data data = receiver.getPeerBean().storage()
			        .get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
			byte[] reconstructedValue = data.toBytes();

			assertArrayEquals(newValue.getBytes(), reconstructedValue);
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
		Peer sender = null;
		Peer receiver = null;
		try {
			final PeerSync senderSync = new PeerSync();
			final SyncSender syncSender = new SyncSender(senderSync);
			sender = new PeerMaker(new Number160(9)).ports(4009).replicationSender(syncSender).makeAndListen();
			final PeerSync receiverSync = new PeerSync();
			final SyncSender syncReceiver = new SyncSender(receiverSync);
			receiver = new PeerMaker(new Number160(10)).ports(4010).replicationSender(syncReceiver).makeAndListen();

			final Number160 locationKey = new Number160(600);
			final Number160 domainKey = Number160.ZERO;
			final Number160 contentKey = Number160.ZERO;
			Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
			final String newValue = "Test1Test2Test3Test4";

			Data test1 = new Data(newValue.getBytes());

			sender.put(locationKey).setData(test1).start().awaitUninterruptibly();

			FutureDone<SynchronizationStatistics> future = senderSync.synchronize(receiver.getPeerAddress()).key(key)
			        .start();
			future.awaitUninterruptibly();

			System.err.println(future.getObject().toString());

			Data data = receiver.getPeerBean().storage()
			        .get(new Number640(locationKey, domainKey, contentKey, Number160.ZERO));
			byte[] reconstructedValue = data.toBytes();
			assertArrayEquals(newValue.getBytes(), reconstructedValue);
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
	public void testIntToByteArrayToInt() {
		for (int i = 0; i < 1000000; i++) {
			testByteArrayToInt0();
		}
	}

	private void testByteArrayToInt0() {
		Random random = new Random();
		int value = random.nextInt(32767);

		assertEquals(value, Synchronization.byteArrayToInt(Synchronization.intToByteArray(value)));
	}

	@Test
	public void testEncodeDecodeChecksums() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
		String value = "asdfkjasfalskjfasdfkljasaslkfjasdflaksjdfasdklfjasa";
		int size = 10;
		ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
		byte[] bytes = Synchronization.encodeChecksumList(checksums);

		assertEquals(checksums, Synchronization.decodeChecksumList(bytes));
	}

	@Test
	public void testEncodeDecodeChecksums2() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
		String value = "Test1Test2Test3Test4";
		int size = 5;
		ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
		byte[] bytes = Synchronization.encodeChecksumList(checksums);

		assertEquals(checksums, Synchronization.decodeChecksumList(bytes));
	}

	@Test
	public void testEncodeDecodeInstructions() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
		String value = "asdfkjasfalskjfasdfkljasaslkfjasdflaksjdfasdklfjasa";
		String newValue = "asdfkjasfalskjfasdfkljasasasdflkjsdfkjjdfasdklfjasa";
		int size = 10;
		ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
		ArrayList<Instruction> instructions = Synchronization.getInstructions(newValue.getBytes(), checksums, size);

		Number160 key = new Number160(12345);
		byte[] bytes = Synchronization.encodeInstructionList(instructions, key);

		assertEquals(instructions, Synchronization.decodeInstructionList(bytes));
	}

	@Test
	public void testEncodeDecodeNumber160() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
		String value = "asdfkjasfalskjfasdfkljasaslkfjasdflaksjdfasdklfjasa";
		String newValue = "asdfkjasfalskjfasdfkljasasasdflkjsdfkjjdfasdklfjasa";
		int size = 10;
		ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
		ArrayList<Instruction> instructions = Synchronization.getInstructions(newValue.getBytes(), checksums, size);

		Number160 key = new Number160(12345);
		byte[] bytes = Synchronization.encodeInstructionList(instructions, key);

		assertEquals(key, Synchronization.decodeHash(bytes));
	}
}
