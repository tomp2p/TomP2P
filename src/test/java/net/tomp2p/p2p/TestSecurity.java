package net.tomp2p.p2p;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationRemove;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric.ProtectionEnable;
import net.tomp2p.storage.StorageGeneric.ProtectionMode;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestSecurity
{
	@Test
	public void testPublicKeyReceived() throws Exception
	{
		final Random rnd = new Random(43L);
		Peer master = null;
		Peer slave1 = null;
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		KeyPair pair2 = gen.generateKeyPair();
		// make master
		try
		{
			master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).buildAndListen();
			// make slave
			slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).buildAndListen();
			final AtomicBoolean gotPK = new AtomicBoolean(false);
			// set storage to test PK
			slave1.getPeerBean().setStorage(new StorageMemory()
			{
				@Override
				public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, 
						Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection)
				{
					System.err.println("P is " + publicKey);
					gotPK.set(publicKey != null);
					System.err.println("PK is " + gotPK);
					return super.put(locationKey,domainKey, contentKey, newData, publicKey, putIfAbsent, domainProtection);
				}
			});
			// perfect routing
			boolean peerInMap1 = master.getPeerBean().getPeerMap()
					.peerFound(slave1.getPeerAddress(), null);
			boolean peerInMap2 = slave1.getPeerBean().getPeerMap()
					.peerFound(master.getPeerAddress(), null);
			Assert.assertEquals(true, peerInMap1);
			Assert.assertEquals(true, peerInMap2);
			//
			Number160 locationKey = new Number160(50);
			ConfigurationStore cs1 = Configurations.defaultStoreConfiguration();
			// cs1.setRequestP2PConfiguration(new RequestP2PConfiguration(1, 1,
			// 1));
			cs1.setRequestP2PConfiguration(new RequestP2PConfiguration(1, 1, 0));
			cs1.setSignMessage(true);
			master.put(locationKey, new Data(new byte[100000]), cs1).awaitUninterruptibly();
			// master.put(locationKey, new Data("test"),
			// cs1).awaitUninterruptibly();
			Assert.assertEquals(true, gotPK.get());
			// without PK, this test should fail.
			cs1.setSignMessage(false);
			master.put(locationKey, new Data("test1"), cs1).awaitUninterruptibly();
			Assert.assertEquals(false, gotPK.get());
		}
		finally
		{
			master.shutdown();
			slave1.shutdown();
		}
	}

	@Test
	public void testPublicKeyReceivedDomain() throws Exception
	{
		final Random rnd = new Random(43L);
		Peer master = null;
		try
		{
			KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
			KeyPair pair1 = gen.generateKeyPair();
			// make master
			master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).buildAndListen();
			// make slave
			final AtomicBoolean gotPK = new AtomicBoolean(false);
			// set storage to test PK
			master.getPeerBean().setStorage(new StorageMemory()
			{
				@Override
				public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, 
						Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection)
				{
					gotPK.set(publicKey != null);
					System.err.println("PK is " + gotPK);
					return super.put(locationKey,domainKey,contentKey , newData, publicKey, putIfAbsent, domainProtection);
				}
			});
			//
			Number160 locationKey = new Number160(50);
			ConfigurationStore cs1 = Configurations.defaultStoreConfiguration();
			cs1.setSignMessage(true);
			cs1.setContentKey(Number160.ONE);
			cs1.setDomain(Number160.ONE);
			cs1.setRequestP2PConfiguration(new RequestP2PConfiguration(1, 1, 0));
			master.put(locationKey, new Data(new byte[2000]), cs1).awaitUninterruptibly();
			Assert.assertEquals(true, gotPK.get());
			// without PK
			cs1.setSignMessage(false);
			master.put(locationKey, new Data("test1"), cs1).awaitUninterruptibly();
			Assert.assertEquals(false, gotPK.get());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testProtection() throws Exception
	{
		final Random rnd = new Random(43L);
		Peer master = null;
		Peer slave1 = null;
		Peer slave2 = null;
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		KeyPair pair2 = gen.generateKeyPair();
		KeyPair pair3 = gen.generateKeyPair();
		System.err.println("PPK1 " + pair1.getPublic());
		System.err.println("PPK2 " + pair2.getPublic());
		System.err.println("PPK3 " + pair3.getPublic());
		try
		{
			master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).buildAndListen();
			master.getPeerBean()
					.getStorage()
					.setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY,
							ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY);
			slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).buildAndListen();
			slave1.getPeerBean()
					.getStorage()
					.setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY,
							ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY);
			slave2 = new PeerMaker(new Number160(rnd)).setKeyPair(pair3).setMasterPeer(master).buildAndListen();
			slave2.getPeerBean()
					.getStorage()
					.setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY,
							ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY);
			// perfect routing
			master.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
			master.getPeerBean().getPeerMap().peerFound(slave2.getPeerAddress(), null);
			//
			slave1.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
			slave1.getPeerBean().getPeerMap().peerFound(slave2.getPeerAddress(), null);
			//
			slave2.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
			slave2.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
			Number160 locationKey = new Number160(50);
			ConfigurationStore cs1 = Configurations.defaultStoreConfiguration();
			cs1.setProtectDomain(true);
			cs1.setDomain(Utils.makeSHAHash(pair3.getPublic().getEncoded()));
			cs1.setContentKey(new Number160(10));
			FutureDHT fdht1 = master.put(locationKey, new Data("test1"), cs1);
			fdht1.awaitUninterruptibly();
			Assert.assertEquals(true, fdht1.isSuccess());
			// try to insert in same domain from different peer
			cs1.setContentKey(new Number160(11));
			FutureDHT fdht2 = slave1.put(locationKey, new Data("tes2"), cs1);
			fdht2.awaitUninterruptibly();
			Assert.assertEquals(false, fdht2.isSuccess());
			// insert from same peer but with public key protection
			cs1.setContentKey(new Number160(12));
			FutureDHT fdht3 = slave2.put(locationKey, new Data("tes2"), cs1);
			fdht3.awaitUninterruptibly();
			Assert.assertEquals(true, fdht3.isSuccess());
			//
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			// get at least 3 results, because we want to test the domain
			// removel feature
			cg.setRequestP2PConfiguration(new RequestP2PConfiguration(3, 3, 3));
			cg.setDomain(Utils.makeSHAHash(pair3.getPublic().getEncoded()));
			FutureDHT fdht4 = slave1.getAll(locationKey, cg);
			fdht4.awaitUninterruptibly();
			Assert.assertEquals(true, fdht4.isSuccess());
			Assert.assertEquals(2, fdht4.getData().size());
		}
		finally
		{
			master.shutdown();
			slave1.shutdown();
			slave2.shutdown();
		}
	}

	@Test
	public void testProtectionWithRemove() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		Peer slave1 = null;
		Peer slave2 = null;
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		KeyPair pair2 = gen.generateKeyPair();
		KeyPair pair3 = gen.generateKeyPair();
		System.err.println("PPK1 " + pair1.getPublic());
		System.err.println("PPK2 " + pair2.getPublic());
		System.err.println("PPK3 " + pair3.getPublic());
		try
		{
			master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).buildAndListen();
			master.getPeerBean()
					.getStorage()
					.setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY,
							ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY);
			slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).buildAndListen();
			slave1.getPeerBean()
					.getStorage()
					.setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY,
							ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY);
			slave2 = new PeerMaker(new Number160(rnd)).setKeyPair(pair3).setMasterPeer(master).buildAndListen();
			slave2.getPeerBean()
					.getStorage()
					.setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY,
							ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY);
			// perfect routing
			master.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
			master.getPeerBean().getPeerMap().peerFound(slave2.getPeerAddress(), null);
			//
			slave1.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
			slave1.getPeerBean().getPeerMap().peerFound(slave2.getPeerAddress(), null);
			//
			slave2.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
			slave2.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
			Number160 locationKey = new Number160(50);
			ConfigurationStore cs1 = Configurations.defaultStoreConfiguration();
			cs1.setProtectDomain(true);
			cs1.setDomain(Utils.makeSHAHash(pair1.getPublic().getEncoded()));
			FutureDHT fdht1 = master.put(locationKey, new Data("test1"), cs1);
			fdht1.awaitUninterruptibly();
			// remove from different peer, should fail
			ConfigurationRemove cr = Configurations.defaultRemoveConfiguration();
			cr.setDomain(Utils.makeSHAHash(pair1.getPublic().getEncoded()));
			cr.setSignMessage(true);
			FutureDHT fdht2 = slave1.remove(locationKey, cr);
			fdht2.awaitUninterruptibly();
			Assert.assertEquals(0, fdht2.getKeys().size());
			// this should work
			FutureDHT fdht3 = master.remove(locationKey, cr);
			fdht3.awaitUninterruptibly();
			Assert.assertEquals(1, fdht3.getKeys().size());
		}
		finally
		{
			master.shutdown();
			slave1.shutdown();
			slave2.shutdown();
		}
	}

	@Test
	public void testProtectionDomain() throws Exception
	{
		final Random rnd = new Random(43L);
		Peer master = null;
		Peer slave1 = null;
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair pair1 = gen.generateKeyPair();
		KeyPair pair2 = gen.generateKeyPair();
		// make master
		try
		{
			master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).buildAndListen();
			// make slave
			slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).buildAndListen();
			master.getPeerBean().setStorage(new StorageMemory()
			{
				public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, 
						Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection)
				{
					//System.out.println("store1");
					return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent, domainProtection);
				}
			});
			slave1.getPeerBean().setStorage(new StorageMemory()
			{
				@Override
				public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, 
						Data newData, PublicKey publicKey, boolean putIfAbsent, boolean domainProtection)
				{
					//System.out.println("store2");
					return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent, domainProtection);
				}
			});
			// perfect routing
			boolean peerInMap1 = master.getPeerBean().getPeerMap()
					.peerFound(slave1.getPeerAddress(), null);
			boolean peerInMap2 = slave1.getPeerBean().getPeerMap()
					.peerFound(master.getPeerAddress(), null);
			Assert.assertEquals(true, peerInMap1);
			Assert.assertEquals(true, peerInMap2);
			ConfigurationStore cs1 = Configurations.defaultStoreConfiguration();
			// since we have to peers, we store on both, otherwise this test may
			// sometimes work, sometimes not.
			cs1.setRequestP2PConfiguration(new RequestP2PConfiguration(1, 1, 1));
			cs1.setContentKey(Number160.createHash("content1"));
			cs1.setDomain(Number160.createHash("domain1"));
			cs1.setProtectDomain(true);
			Number160 locationKey = Number160.createHash("loctaion");
			FutureDHT futureDHT = master.put(locationKey, new Data("test1"), cs1);
			futureDHT.awaitUninterruptibly();
			Assert.assertEquals(true, futureDHT.isSuccess());
			// now the slave stores with different in the same domain. This
			// should not work
			cs1.setContentKey(Number160.createHash("content2"));
			futureDHT = slave1.put(locationKey, new Data("test2"), cs1);
			futureDHT.awaitUninterruptibly();
			System.err.println(futureDHT.getFailedReason());
			Assert.assertEquals(false, futureDHT.isSuccess());
		}
		finally
		{
			master.shutdown();
			slave1.shutdown();
		}
	}
}
