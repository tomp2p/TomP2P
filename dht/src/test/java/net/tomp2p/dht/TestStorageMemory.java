package net.tomp2p.dht;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestStorageMemory {

	@Test
	// copied from Hive2Hive
	public void testMaxVersionLimit() throws IOException, ClassNotFoundException, NoSuchAlgorithmException,
			InvalidKeyException, SignatureException, InterruptedException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		// create peers which accept only two versions
		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(1)).ports(5000).keyPair(keyPairPeer1).start())
				.storage(new StorageMemory(1000, 2)).start();
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerBuilderDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
				.start()).storage(new StorageMemory(1000, 2)).start();

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

		KeyPair keyPair1 = gen.generateKeyPair();

		Number160 lKey = Number160.createHash("location");
		Number160 dKey = Number160.createHash("domain");
		Number160 cKey = Number160.createHash("content");

		try {
			// put first version
			FuturePut futurePut = p1.put(lKey).domainKey(dKey).data(cKey, new Data("version1").protectEntry(keyPair1))
					.versionKey(new Number160(0)).keyPair(keyPair1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertTrue(futurePut.isSuccess());
			// put second version
			futurePut = p1.put(lKey).domainKey(dKey).data(cKey, new Data("version2").protectEntry(keyPair1))
					.versionKey(new Number160(1)).keyPair(keyPair1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertTrue(futurePut.isSuccess());
			// put third version
			futurePut = p1.put(lKey).domainKey(dKey).data(cKey, new Data("version3").protectEntry(keyPair1))
					.versionKey(new Number160(2)).keyPair(keyPair1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertTrue(futurePut.isSuccess());

			// wait for maintenance to kick in
			Thread.sleep(1500);
			
			// first version should be not available
			FutureGet futureGet = p1.get(lKey).domainKey(dKey).contentKey(cKey).versionKey(new Number160(0)).start();
			futureGet.awaitUninterruptibly();
			Assert.assertTrue(futureGet.isSuccess());
			Assert.assertNull(futureGet.data());
		} finally {
			p1.shutdown().awaitUninterruptibly();
			p2.shutdown().awaitUninterruptibly();
		}
	}
}
