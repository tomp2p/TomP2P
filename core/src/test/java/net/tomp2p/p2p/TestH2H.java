package net.tomp2p.p2p;

import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

import net.tomp2p.futures.FuturePut;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestH2H {
	@Test
	public void testPut() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InvalidKeyException,
	        SignatureException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		Peer p1 = new PeerMaker(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1)
		        .setEnableIndirectReplication(true).makeAndListen();
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		Peer p2 = new PeerMaker(Number160.createHash(2)).masterPeer(p1).keyPair(keyPairPeer2)
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

		H2HTestData testData = new H2HTestData("2b51b720-7ae2-11e3-981f-0800200c9a66");

		Data data = new Data(testData);
		data.ttlSeconds(10000);
		data.basedOn(bKey);
		data.setProtectedEntry().sign(keyPair);
		FuturePut futurePut1 = p1.put(lKey).setData(cKey, data).setDomainKey(dKey).setVersionKey(vKey).keyPair(keyPair)
		        .start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());

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
