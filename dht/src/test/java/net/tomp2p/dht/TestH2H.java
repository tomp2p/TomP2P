package net.tomp2p.dht;

import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestH2H {
	
	private static final DSASignatureFactory factory = new DSASignatureFactory();
	
	@Test
	public void testPut() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InvalidKeyException,
	        SignatureException {
		StringBuilder sb = new StringBuilder("2b51b720-7ae2-11e3-981f-0800200c9a66");
		for (int i = 0; i < 10; i++) {
			testPut(sb.toString());
			sb.append("2b51b720-7ae2-11e3-981f-0800200c9a66");
		}
	}

	@Test
	public void testPut0() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InvalidKeyException,
	        SignatureException {
		testPut("2b51b720-7ae2-11e3-981f-0800200c9a662b51b720-7ae2-11e3-981f-0800200c9a66");
	}

	private void testPut(String s1) throws IOException, ClassNotFoundException, NoSuchAlgorithmException,
	        InvalidKeyException, SignatureException {
		PeerDHT p1 = null;
		PeerDHT p2 = null;
		try {

			KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

			KeyPair keyPairPeer1 = gen.generateKeyPair();
			p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1).start()).start();
			KeyPair keyPairPeer2 = gen.generateKeyPair();
			p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2).start()).start();

			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
			p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
			KeyPair keyPair = gen.generateKeyPair();

			String locationKey = "location";
			Number160 lKey = Number160.createHash(locationKey);
			String domainKey = "domain";
			Number160 dKey = Number160.createHash(domainKey);
			String contentKey = "content";
			Number160 cKey = Number160.createHash(contentKey);
			String versionKey = "version";
			Number160 vKey = Number160.createHash(versionKey);
			String basedOnKey = "based on";
			Number160 bKey = Number160.createHash(basedOnKey);

			H2HTestData testData = new H2HTestData(s1);

			Data data = new Data(testData);
			data.ttlSeconds(10000);
			data.addBasedOn(bKey);
			data.protectEntryNow(keyPair, factory);
			FuturePut futurePut1 = p1.put(lKey).data(cKey, data).domainKey(dKey).versionKey(vKey)
			        .keyPair(keyPair).start();
			futurePut1.awaitUninterruptibly();
			Assert.assertTrue(futurePut1.isSuccess());

		} catch (Throwable t) {
			Assert.fail("no reason to fail");
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
	public void testRemove1() throws NoSuchAlgorithmException, IOException, InvalidKeyException, SignatureException,
	        ClassNotFoundException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1).start()).start();
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2).start()).start();

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
		KeyPair keyPair1 = gen.generateKeyPair();
		KeyPair keyPair2 = gen.generateKeyPair();
		String locationKey = "location";
		Number160 lKey = Number160.createHash(locationKey);
		String contentKey = "content";
		Number160 cKey = Number160.createHash(contentKey);

		String testData1 = "data1";
		Data data = new Data(testData1).protectEntryNow(keyPair1, factory);

		// put trough peer 1 with key pair
		// -------------------------------------------------------

		FuturePut futurePut1 = p1.put(lKey).data(cKey, data).keyPair(keyPair1).start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());

		FutureGet futureGet1a = p1.get(lKey).contentKey(cKey).start();
		futureGet1a.awaitUninterruptibly();
		Assert.assertTrue(futureGet1a.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1a.data().object());

		FutureGet futureGet1b = p2.get(lKey).contentKey(cKey).start();
		futureGet1b.awaitUninterruptibly();
		Assert.assertTrue(futureGet1b.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1b.data().object());

		// try to remove without key pair
		// -------------------------------------------------------

		FutureRemove futureRemove1a = p1.remove(lKey).contentKey(cKey).start();
		futureRemove1a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1a.isSuccess());

		FutureGet futureGet2a = p1.get(lKey).contentKey(cKey).start();
		futureGet2a.awaitUninterruptibly();
		Assert.assertTrue(futureGet2a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2a.data().object());

		FutureRemove futureRemove1b = p2.remove(lKey).contentKey(cKey).start();
		futureRemove1b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1b.isSuccess());

		FutureGet futureGet2b = p2.get(lKey).contentKey(cKey).start();
		futureGet2b.awaitUninterruptibly();
		Assert.assertTrue(futureGet2b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2b.data().object());
		// try to remove with wrong key pair
		// ---------------------------------------------------

		FutureRemove futureRemove2a = p1.remove(lKey).contentKey(cKey).keyPair(keyPair2).start();
		futureRemove2a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2a.isSuccess());

		FutureGet futureGet3a = p1.get(lKey).contentKey(cKey).start();
		futureGet3a.awaitUninterruptibly();
		Assert.assertTrue(futureGet3a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3a.data().object());

		FutureRemove futureRemove2b = p2.remove(lKey).contentKey(cKey).start();
		futureRemove2b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2b.isSuccess());

		FutureGet futureGet3b = p2.get(lKey).contentKey(cKey).start();
		futureGet3b.awaitUninterruptibly();
		Assert.assertTrue(futureGet3b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3b.data().object());

		// remove with correct key pair
		// ---------------------------------------------------------

		FutureRemove futureRemove4 = p1.remove(lKey).contentKey(cKey).keyPair(keyPair1).start();
		futureRemove4.awaitUninterruptibly();
		Assert.assertTrue(futureRemove4.isSuccess());

		FutureGet futureGet4a = p2.get(lKey).contentKey(cKey).start();
		futureGet4a.awaitUninterruptibly();
		Assert.assertFalse(futureGet4a.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4a.data());
		FutureGet futureGet4b = p2.get(lKey).contentKey(cKey).start();
		futureGet4b.awaitUninterruptibly();
		Assert.assertFalse(futureGet4b.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4b.data());

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}

	@Test
	public void testRemoveFromTo() throws NoSuchAlgorithmException, IOException, InvalidKeyException,
	        SignatureException, ClassNotFoundException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1).start()).start();
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2).start()).start();

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

		KeyPair key1 = gen.generateKeyPair();
		KeyPair key2 = gen.generateKeyPair();

		String locationKey = "location";
		Number160 lKey = Number160.createHash(locationKey);
		//String domainKey = "domain";
		//Number160 dKey = Number160.createHash(domainKey);
		String contentKey = "content";
		Number160 cKey = Number160.createHash(contentKey);

		String testData1 = "data1";
		Data data = new Data(testData1).protectEntryNow(key1, factory);

		// put trough peer 1 with key pair
		// -------------------------------------------------------

		FuturePut futurePut1 = p1.put(lKey).data(cKey, data).keyPair(key1).start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());

		FutureGet futureGet1a = p1.get(lKey).contentKey(cKey).start();
		futureGet1a.awaitUninterruptibly();
		Assert.assertTrue(futureGet1a.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1a.data().object());

		FutureGet futureGet1b = p2.get(lKey).contentKey(cKey).start();
		futureGet1b.awaitUninterruptibly();
		Assert.assertTrue(futureGet1b.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1b.data().object());

		// try to remove without key pair using from/to
		// -----------------------------------------

		FutureRemove futureRemove1a = p1.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).start();
		futureRemove1a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1a.isSuccess());

		FutureGet futureGet2a = p1.get(lKey).contentKey(cKey).start();
		futureGet2a.awaitUninterruptibly();
		Assert.assertTrue(futureGet2a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2a.data().object());

		FutureRemove futureRemove1b = p2.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).start();
		futureRemove1b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1b.isSuccess());

		FutureGet futureGet2b = p2.get(lKey).contentKey(cKey).start();
		futureGet2b.awaitUninterruptibly();
		Assert.assertTrue(futureGet2b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2b.data().object());

		// remove with wrong key pair
		// -----------------------------------------------------------

		FutureRemove futureRemove2a = p1.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).keyPair(key2).start();
		futureRemove2a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2a.isSuccess());
		FutureGet futureGet3a = p2.get(lKey).contentKey(cKey).start();
		futureGet3a.awaitUninterruptibly();
		Assert.assertTrue(futureGet3a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3a.data().object());
		FutureRemove futureRemove2b = p2.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).keyPair(key2).start();
		futureRemove2b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2b.isSuccess());

		FutureGet futureGet3b = p2.get(lKey).contentKey(cKey).start();
		futureGet3b.awaitUninterruptibly();
		Assert.assertTrue(futureGet3b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3b.data().object());
		// remove with correct key pair
		// -----------------------------------------------------------

		FutureRemove futureRemove4 = p1.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).keyPair(key1).start();
		futureRemove4.awaitUninterruptibly();
		Assert.assertTrue(futureRemove4.isSuccess());
		FutureGet futureGet4a = p2.get(lKey).contentKey(cKey).start();
		futureGet4a.awaitUninterruptibly();
		Assert.assertFalse(futureGet4a.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4a.data());

		FutureGet futureGet4b = p2.get(lKey).contentKey(cKey).start();
		futureGet4b.awaitUninterruptibly();
		Assert.assertFalse(futureGet4b.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4b.data());

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}

	@Test
	public void getFromToTest1() throws IOException, ClassNotFoundException {
		PeerDHT p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(4838).start()).start();
		PeerDHT p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).start()).start();

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();

		String locationKey = "location";
		String contentKey = "content";

		List<H2HTestData> content = new ArrayList<H2HTestData>();
		int numberOfContent = 3;
		for (int i = 0; i < numberOfContent; i++) {
			H2HTestData data = new H2HTestData(UUID.randomUUID().toString());
			data.generateVersionKey();
			if (i > 0) {
				data.setBasedOnKey(content.get(i - 1).getVersionKey());
			}
			content.add(data);

			p2.put(Number160.createHash(locationKey)).data(Number160.createHash(contentKey), new Data(data))
					.versionKey(data.getVersionKey()).start().awaitUninterruptibly().awaitListenersUninterruptibly();
		}

		FutureGet future = p1
				.get(Number160.createHash(locationKey))
				.from(new Number640(Number160.createHash(locationKey), Number160.ZERO, Number160.createHash(contentKey),
						Number160.ZERO))
				.to(new Number640(Number160.createHash(locationKey), Number160.ZERO, Number160.createHash(contentKey),
						Number160.MAX_VALUE)).descending().returnNr(1).start();
		future.awaitUninterruptibly();
		future.awaitListenersUninterruptibly();

		Assert.assertEquals(content.get(numberOfContent - 1).getTestString(),
				((H2HTestData) future.data().object()).getTestString());

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}
}

class H2HTestData extends NetworkContent {

	private static final long serialVersionUID = -4190279666159015217L;
	private final String testString;

	public H2HTestData(String testContent) {
		this.testString = testContent;
	}

	@Override
	public int getTimeToLive() {
		return 10000;
	}

	public String getTestString() {
		return testString;
	}

}

abstract class NetworkContent implements Serializable {

	private static final long serialVersionUID = 1L;

	private Number160 versionKey = Number160.ZERO;

	private Number160 basedOnKey = Number160.ZERO;

	public abstract int getTimeToLive();

	public Number160 getVersionKey() {
		return versionKey;
	}

	public void setVersionKey(Number160 versionKey) {
		this.versionKey = versionKey;
	}

	public Number160 getBasedOnKey() {
		return basedOnKey;
	}

	public void setBasedOnKey(Number160 versionKey) {
		this.basedOnKey = versionKey;
	}
	
	public void generateVersionKey() {
		// re-attach version keys
		basedOnKey = versionKey;
		// increase counter
		long counter = basedOnKey.timestamp() + 1;
		// create new version key based on increased counter and hash
		versionKey = new Number160(counter, new Number160(this.hashCode()).number96());
	}
}
