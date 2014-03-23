package net.tomp2p.p2p;

import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

import net.tomp2p.futures.FutureGet;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.futures.FutureRemove;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestH2H {
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
		Peer p1 = null;
		Peer p2 = null;
		try {

			KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

			KeyPair keyPairPeer1 = gen.generateKeyPair();
			p1 = new PeerMaker(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1)
			        .setEnableIndirectReplication(true).makeAndListen();
			KeyPair keyPairPeer2 = gen.generateKeyPair();
			p2 = new PeerMaker(Number160.createHash(2)).masterPeer(p1).keyPair(keyPairPeer2)
			        .setEnableIndirectReplication(true).makeAndListen();

			p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();
			p1.bootstrap().setPeerAddress(p2.getPeerAddress()).start().awaitUninterruptibly();
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
			data.basedOn(bKey);
			data.setProtectedEntry().sign(keyPair);
			FuturePut futurePut1 = p1.put(lKey).setData(cKey, data).setDomainKey(dKey).setVersionKey(vKey)
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
		Peer p1 = new PeerMaker(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1)
		        .setEnableIndirectReplication(true).makeAndListen();
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		Peer p2 = new PeerMaker(Number160.createHash(2)).masterPeer(p1).keyPair(keyPairPeer2)
		        .setEnableIndirectReplication(true).makeAndListen();

		p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();
		p1.bootstrap().setPeerAddress(p2.getPeerAddress()).start().awaitUninterruptibly();
		KeyPair keyPair1 = gen.generateKeyPair();
		KeyPair keyPair2 = gen.generateKeyPair();
		String locationKey = "location";
		Number160 lKey = Number160.createHash(locationKey);
		String contentKey = "content";
		Number160 cKey = Number160.createHash(contentKey);

		String testData1 = "data1";
		Data data = new Data(testData1).setProtectedEntry().sign(keyPair1);

		// put trough peer 1 with key pair
		// -------------------------------------------------------

		FuturePut futurePut1 = p1.put(lKey).setData(cKey, data).keyPair(keyPair1).start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());

		FutureGet futureGet1a = p1.get(lKey).setContentKey(cKey).start();
		futureGet1a.awaitUninterruptibly();
		Assert.assertTrue(futureGet1a.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1a.getData().object());

		FutureGet futureGet1b = p2.get(lKey).setContentKey(cKey).start();
		futureGet1b.awaitUninterruptibly();
		Assert.assertTrue(futureGet1b.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1b.getData().object());

		// try to remove without key pair
		// -------------------------------------------------------

		FutureRemove futureRemove1a = p1.remove(lKey).contentKey(cKey).start();
		futureRemove1a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1a.isSuccess());

		FutureGet futureGet2a = p1.get(lKey).setContentKey(cKey).start();
		futureGet2a.awaitUninterruptibly();
		Assert.assertTrue(futureGet2a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2a.getData().object());

		FutureRemove futureRemove1b = p2.remove(lKey).contentKey(cKey).start();
		futureRemove1b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1b.isSuccess());

		FutureGet futureGet2b = p2.get(lKey).setContentKey(cKey).start();
		futureGet2b.awaitUninterruptibly();
		Assert.assertTrue(futureGet2b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2b.getData().object());
		// try to remove with wrong key pair
		// ---------------------------------------------------

		FutureRemove futureRemove2a = p1.remove(lKey).contentKey(cKey).keyPair(keyPair2).start();
		futureRemove2a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2a.isSuccess());

		FutureGet futureGet3a = p1.get(lKey).setContentKey(cKey).start();
		futureGet3a.awaitUninterruptibly();
		Assert.assertTrue(futureGet3a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3a.getData().object());

		FutureRemove futureRemove2b = p2.remove(lKey).contentKey(cKey).start();
		futureRemove2b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2b.isSuccess());

		FutureGet futureGet3b = p2.get(lKey).setContentKey(cKey).start();
		futureGet3b.awaitUninterruptibly();
		Assert.assertTrue(futureGet3b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3b.getData().object());

		// remove with correct key pair
		// ---------------------------------------------------------

		FutureRemove futureRemove4 = p1.remove(lKey).contentKey(cKey).keyPair(keyPair1).start();
		futureRemove4.awaitUninterruptibly();
		Assert.assertTrue(futureRemove4.isSuccess());

		FutureGet futureGet4a = p2.get(lKey).setContentKey(cKey).start();
		futureGet4a.awaitUninterruptibly();
		Assert.assertFalse(futureGet4a.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4a.getData());
		FutureGet futureGet4b = p2.get(lKey).setContentKey(cKey).start();
		futureGet4b.awaitUninterruptibly();
		Assert.assertFalse(futureGet4b.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4b.getData());

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}

	@Test
	public void testRemoveFromTo() throws NoSuchAlgorithmException, IOException, InvalidKeyException,
	        SignatureException, ClassNotFoundException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		Peer p1 = new PeerMaker(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1)
		        .setEnableIndirectReplication(true).makeAndListen();
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		Peer p2 = new PeerMaker(Number160.createHash(2)).masterPeer(p1).keyPair(keyPairPeer2)
		        .setEnableIndirectReplication(true).makeAndListen();

		p2.bootstrap().setPeerAddress(p1.getPeerAddress()).start().awaitUninterruptibly();
		p1.bootstrap().setPeerAddress(p2.getPeerAddress()).start().awaitUninterruptibly();

		KeyPair key1 = gen.generateKeyPair();
		KeyPair key2 = gen.generateKeyPair();

		String locationKey = "location";
		Number160 lKey = Number160.createHash(locationKey);
		//String domainKey = "domain";
		//Number160 dKey = Number160.createHash(domainKey);
		String contentKey = "content";
		Number160 cKey = Number160.createHash(contentKey);

		String testData1 = "data1";
		Data data = new Data(testData1).setProtectedEntry().sign(key1);

		// put trough peer 1 with key pair
		// -------------------------------------------------------

		FuturePut futurePut1 = p1.put(lKey).setData(cKey, data).keyPair(key1).start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());

		FutureGet futureGet1a = p1.get(lKey).setContentKey(cKey).start();
		futureGet1a.awaitUninterruptibly();
		Assert.assertTrue(futureGet1a.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1a.getData().object());

		FutureGet futureGet1b = p2.get(lKey).setContentKey(cKey).start();
		futureGet1b.awaitUninterruptibly();
		Assert.assertTrue(futureGet1b.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1b.getData().object());

		// try to remove without key pair using from/to
		// -----------------------------------------

		FutureRemove futureRemove1a = p1.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).start();
		futureRemove1a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1a.isSuccess());

		FutureGet futureGet2a = p1.get(lKey).setContentKey(cKey).start();
		futureGet2a.awaitUninterruptibly();
		Assert.assertTrue(futureGet2a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2a.getData().object());

		FutureRemove futureRemove1b = p2.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).start();
		futureRemove1b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove1b.isSuccess());

		FutureGet futureGet2b = p2.get(lKey).setContentKey(cKey).start();
		futureGet2b.awaitUninterruptibly();
		Assert.assertTrue(futureGet2b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet2b.getData().object());

		// remove with wrong key pair
		// -----------------------------------------------------------

		FutureRemove futureRemove2a = p1.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).keyPair(key2).start();
		futureRemove2a.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2a.isSuccess());
		FutureGet futureGet3a = p2.get(lKey).setContentKey(cKey).start();
		futureGet3a.awaitUninterruptibly();
		Assert.assertTrue(futureGet3a.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3a.getData().object());
		FutureRemove futureRemove2b = p2.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).keyPair(key2).start();
		futureRemove2b.awaitUninterruptibly();
		Assert.assertFalse(futureRemove2b.isSuccess());

		FutureGet futureGet3b = p2.get(lKey).setContentKey(cKey).start();
		futureGet3b.awaitUninterruptibly();
		Assert.assertTrue(futureGet3b.isSuccess());
		// should have been not modified
		Assert.assertEquals(testData1, (String) futureGet3b.getData().object());
		// remove with correct key pair
		// -----------------------------------------------------------

		FutureRemove futureRemove4 = p1.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).keyPair(key1).start();
		futureRemove4.awaitUninterruptibly();
		Assert.assertTrue(futureRemove4.isSuccess());
		FutureGet futureGet4a = p2.get(lKey).setContentKey(cKey).start();
		futureGet4a.awaitUninterruptibly();
		Assert.assertFalse(futureGet4a.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4a.getData());

		FutureGet futureGet4b = p2.get(lKey).setContentKey(cKey).start();
		futureGet4b.awaitUninterruptibly();
		Assert.assertFalse(futureGet4b.isSuccess());
		// should have been removed
		Assert.assertNull(futureGet4b.getData());

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
}
