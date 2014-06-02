package net.tomp2p.dht;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Cipher;

import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelServerConficuration;
import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.connection.RSASignatureFactory;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.dht.StorageLayer.ProtectionEnable;
import net.tomp2p.dht.StorageLayer.ProtectionMode;
import net.tomp2p.message.RSASignatureCodec;
import net.tomp2p.message.SignatureCodec;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestSecurity {
    final private static Random rnd = new Random(42L);
    private static final DSASignatureFactory factory = new DSASignatureFactory();

    @Test
    public void testPublicKeyReceived() throws Exception {
        final Random rnd = new Random(43L);
        PeerDHT master = null;
        PeerDHT slave1 = null;
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        // make master
        try {
        	final AtomicBoolean gotPK = new AtomicBoolean(false);
            // set storage to test PK
        	StorageLayer sl = new StorageLayer(new StorageMemory()) {
            	@Override
            	public Enum<?> put(Number640 key, Data newData, PublicKey publicKey, boolean putIfAbsent,
            			boolean domainProtection) {
            		System.err.println("P is " + publicKey);
                    gotPK.set(publicKey != null);
                    System.err.println("PK is " + gotPK);
            		return super.put(key, newData, publicKey, putIfAbsent, domainProtection);
            	}
            };
        	
            master = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair1).ports(4001).start());
            // make slave
            slave1 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair2).masterPeer(master.peer()).start(), sl);
            
            // perfect routing
            boolean peerInMap1 = master.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            boolean peerInMap2 = slave1.peerBean().peerMap().peerFound(master.peerAddress(), null);
            Assert.assertEquals(true, peerInMap1);
            Assert.assertEquals(true, peerInMap2);
            //
            Number160 locationKey = new Number160(50);

            RequestP2PConfiguration rc = new RequestP2PConfiguration(1, 1, 0);
            master.put(locationKey).setData(new Data(new byte[100000])).requestP2PConfiguration(rc).sign()
                    .start().awaitUninterruptibly();
            // master.put(locationKey, new Data("test"),
            // cs1).awaitUninterruptibly();
            Assert.assertEquals(true, gotPK.get());
            // without PK, this test should fail.
            master.put(locationKey).setData(new Data("test1")).requestP2PConfiguration(rc).start()
                    .awaitUninterruptibly();
            Assert.assertEquals(false, gotPK.get());
        } finally {
            master.shutdown();
            slave1.shutdown();
        }
    }

    @Test
    public void testPublicKeyReceivedDomain() throws Exception {
        final Random rnd = new Random(43L);
        PeerDHT master = null;
        try {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
            KeyPair pair1 = gen.generateKeyPair();
            
            final AtomicBoolean gotPK = new AtomicBoolean(false);
            // set storage to test PK
            StorageLayer sl =new StorageLayer(new StorageMemory()) {
            	@Override
            	public Enum<?> put(Number640 key, Data newData, PublicKey publicKey, boolean putIfAbsent,
            			boolean domainProtection) {
                    gotPK.set(publicKey != null);
                    System.err.println("PK is " + gotPK);
                    return super.put(key, newData, publicKey, putIfAbsent, domainProtection);
                }
            };
            
            // make master
            master = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair1).ports(4001).start(), sl);
            
            //
            Number160 locationKey = new Number160(50);
            RequestP2PConfiguration rc = new RequestP2PConfiguration(1, 1, 0);
            master.put(locationKey).setData(Number160.ONE, new Data(new byte[2000])).requestP2PConfiguration(rc)
                    .domainKey(Number160.ONE).sign().start().awaitUninterruptibly();
            Assert.assertEquals(true, gotPK.get());
            // without PK
            master.put(locationKey).setData(Number160.ONE, new Data("test1")).requestP2PConfiguration(rc)
                    .domainKey(Number160.ONE).start().awaitUninterruptibly();
            Assert.assertEquals(false, gotPK.get());
        } catch (Throwable t) {
        	Assert.fail(t.getMessage());
        	t.printStackTrace();
        } finally {
            master.shutdown();
        }
    }

    @Test
    public void testProtection() throws Exception {
        final Random rnd = new Random(43L);
        PeerDHT master = null;
        PeerDHT slave1 = null;
        PeerDHT slave2 = null;
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        KeyPair pair3 = gen.generateKeyPair();
        System.err.println("PPK1 " + pair1.getPublic());
        System.err.println("PPK2 " + pair2.getPublic());
        System.err.println("PPK3 " + pair3.getPublic());
        try {
            master = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair1).ports(4001).start());
            master
                    .storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave1 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair2).masterPeer(master.peer()).start());
            slave1.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave2 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair3).masterPeer(master.peer()).start());
            slave2.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            // perfect routing
            master.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            master.peerBean().peerMap().peerFound(slave2.peerAddress(), null);
            //
            slave1.peerBean().peerMap().peerFound(master.peerAddress(), null);
            slave1.peerBean().peerMap().peerFound(slave2.peerAddress(), null);
            //
            slave2.peerBean().peerMap().peerFound(master.peerAddress(), null);
            slave2.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            Number160 locationKey = new Number160(50);
            FuturePut fdht1 = master.put(locationKey).setData(new Number160(10), new Data("test1"))
                    .domainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).setProtectDomain().start();
            fdht1.awaitUninterruptibly();
            Assert.assertEquals(true, fdht1.isSuccess());
            // try to insert in same domain from different peer
            FuturePut fdht2 = slave1.put(locationKey).setData(new Number160(11), new Data("tes2"))
                    .domainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).setProtectDomain().start();
            fdht2.awaitUninterruptibly();
            Assert.assertEquals(false, fdht2.isSuccess());
            // insert from same peer but with public key protection
            FuturePut fdht3 = slave2.put(locationKey).setData(new Number160(12), new Data("tes2"))
                    .domainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).setProtectDomain().start();
            fdht3.awaitUninterruptibly();
            Assert.assertEquals(true, fdht3.isSuccess());
            //
            // get at least 3 results, because we want to test the domain
            // removel feature
            RequestP2PConfiguration rc = new RequestP2PConfiguration(3, 3, 3);
            FutureGet fdht4 = slave1.get(locationKey).setAll().requestP2PConfiguration(rc)
                    .domainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).start();
            fdht4.awaitUninterruptibly();
            Assert.assertEquals(true, fdht4.isSuccess());
            Assert.assertEquals(2, fdht4.getDataMap().size());
        } finally {
            master.shutdown();
            slave1.shutdown();
            slave2.shutdown();
        }
    }

    @Test
    public void testProtectionWithRemove() throws Exception {
        final Random rnd = new Random(42L);
        PeerDHT master = null;
        PeerDHT slave1 = null;
        PeerDHT slave2 = null;
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        KeyPair pair3 = gen.generateKeyPair();
        System.err.println("PPK1 " + pair1.getPublic());
        System.err.println("PPK2 " + pair2.getPublic());
        System.err.println("PPK3 " + pair3.getPublic());
        try {
            master = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair1).ports(4001).start());
            master.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave1 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair2).masterPeer(master.peer()).start());
            slave1.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave2 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair3).masterPeer(master.peer()).start());
            slave2.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            // perfect routing
            master.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            master.peerBean().peerMap().peerFound(slave2.peerAddress(), null);
            //
            slave1.peerBean().peerMap().peerFound(master.peerAddress(), null);
            slave1.peerBean().peerMap().peerFound(slave2.peerAddress(), null);
            //
            slave2.peerBean().peerMap().peerFound(master.peerAddress(), null);
            slave2.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            Number160 locationKey = new Number160(50);
            FuturePut fdht1 = master.put(locationKey).setData(new Data("test1"))
                    .domainKey(Utils.makeSHAHash(pair1.getPublic().getEncoded())).setProtectDomain().start();
            fdht1.awaitUninterruptibly();
            // remove from different peer, should fail
            FutureRemove fdht2 = slave1.remove(locationKey)
                    .domainKey(Utils.makeSHAHash(pair1.getPublic().getEncoded())).sign().start();
            fdht2.awaitUninterruptibly();
            Assert.assertFalse(fdht2.isSuccess());
            // this should work
            FutureRemove fdht3 = master.remove(locationKey)
                    .domainKey(Utils.makeSHAHash(pair1.getPublic().getEncoded())).sign().start();
            fdht3.awaitUninterruptibly();
            Assert.assertTrue(fdht3.isSuccess());
        } finally {
            master.shutdown();
            slave1.shutdown();
            slave2.shutdown();
        }
    }

    @Test
    public void testProtectionDomain() throws Exception {
        final Random rnd = new Random(43L);
        PeerDHT master = null;
        PeerDHT slave1 = null;
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        // make master
        try {
        	
        	StorageLayer slm =new StorageLayer(new StorageMemory()) {
            	@Override
            	public Enum<?> put(Number640 key, Data newData, PublicKey publicKey, boolean putIfAbsent,
            			boolean domainProtection) {
                    // System.out.println("store1");
            		return super.put(key, newData, publicKey, putIfAbsent, domainProtection);
                }
            };
            StorageLayer sls =new StorageLayer(new StorageMemory()) {
            	@Override
            	public Enum<?> put(Number640 key, Data newData, PublicKey publicKey, boolean putIfAbsent,
            			boolean domainProtection) {
                    // System.out.println("store2");
            		return super.put(key, newData, publicKey, putIfAbsent, domainProtection);
                }
            };
        	
            master = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair1).ports(4001).start(), slm);
            // make slave
            slave1 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair2).masterPeer(master.peer()).start(), sls);
            
            // perfect routing
            boolean peerInMap1 = master.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            boolean peerInMap2 = slave1.peerBean().peerMap().peerFound(master.peerAddress(), null);
            Assert.assertEquals(true, peerInMap1);
            Assert.assertEquals(true, peerInMap2);

            // since we have to peers, we store on both, otherwise this test may
            // sometimes work, sometimes not.
            RequestP2PConfiguration rc = new RequestP2PConfiguration(1, 1, 1);
            Number160 locationKey = Number160.createHash("loctaion");
            FuturePut futureDHT = master.put(locationKey).setData(Number160.createHash("content1"), new Data("test1"))
                    .domainKey(Number160.createHash("domain1")).setProtectDomain().requestP2PConfiguration(rc)
                    .start();
            futureDHT.awaitUninterruptibly();
            Assert.assertEquals(true, futureDHT.isSuccess());
            // now the slave stores with different in the same domain. This
            // should not work
            futureDHT = slave1.put(locationKey).setData(Number160.createHash("content2"), new Data("test2"))
                    .domainKey(Number160.createHash("domain1")).setProtectDomain().requestP2PConfiguration(rc)
                    .start();
            futureDHT.awaitUninterruptibly();
            System.err.println(futureDHT.failedReason());
            Assert.assertEquals(false, futureDHT.isSuccess());
        } finally {
            master.shutdown();
            slave1.shutdown();
        }
    }

    @Test
    public void testSecurePutGet1() throws Exception {
        PeerDHT master = null;
        PeerDHT slave1 = null;
        PeerDHT slave2 = null;
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        KeyPair pair3 = gen.generateKeyPair();
        System.err.println("PPK1 " + pair1.getPublic());
        System.err.println("PPK2 " + pair2.getPublic());
        System.err.println("PPK3 " + pair3.getPublic());
        try {

            // make slave
            master = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair1).ports(4001).start());
            master.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave1 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair2).masterPeer(master.peer()).start());
            slave1.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave2 = new PeerDHT(new PeerBuilder(new Number160(rnd)).keyPair(pair3).masterPeer(master.peer()).start());
            slave2.storageLayer()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            // perfect routing
            master.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            master.peerBean().peerMap().peerFound(slave2.peerAddress(), null);
            //
            slave1.peerBean().peerMap().peerFound(master.peerAddress(), null);
            slave1.peerBean().peerMap().peerFound(slave2.peerAddress(), null);
            //
            slave2.peerBean().peerMap().peerFound(master.peerAddress(), null);
            slave2.peerBean().peerMap().peerFound(slave1.peerAddress(), null);
            Number160 locationKey = new Number160(50);

            Data data1 = new Data("test1");
            data1.setProtectedEntry();
            FuturePut fdht1 = master.put(locationKey).setData(data1).sign().start();
            fdht1.awaitUninterruptibly();
            fdht1.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(true, fdht1.isSuccess());
            // store again
            Data data2 = new Data("test1");
            data2.setProtectedEntry();
            FuturePut fdht2 = slave1.put(locationKey).setData(data2).sign().start();
            fdht2.awaitUninterruptibly();
            fdht2.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(0, fdht2.getResult().size());
            Assert.assertEquals(false, fdht2.isSuccess());
            // Utils.sleep(1000000);
            // try to remove it, will fail since we do not sign
            FutureRemove fdht3 = slave2.remove(locationKey).start();
            fdht3.awaitUninterruptibly();
            // false, since we have domain protection yet
            Assert.assertEquals(false, fdht3.isSuccess());
            // try to put another thing
            Data data3 = new Data("test2");
            data3.setProtectedEntry();
            FuturePut fdht4 = master.put(locationKey).sign().setData(new Number160(33), data3).start();
            fdht4.awaitUninterruptibly();
            fdht4.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(true, fdht4.isSuccess());
            // get it
            FutureGet fdht7 = slave2.get(locationKey).setAll().start();
            fdht7.awaitUninterruptibly();
            Assert.assertEquals(2, fdht7.getDataMap().size());
            Assert.assertEquals(true, fdht7.isSuccess());
            // if(true)
            // System.exit(0);
            // try to remove for real, all
            FutureRemove fdht5 = master.remove(locationKey).sign().setAll().sign().start();
            fdht5.awaitUninterruptibly();
            System.err.println(fdht5.failedReason());
            Assert.assertEquals(true, fdht5.isSuccess());
            // get all, they should be removed now
            FutureGet fdht6 = slave2.get(locationKey).setAll().start();
            fdht6.awaitUninterruptibly();
            Assert.assertEquals(0, fdht6.getDataMap().size());
            Assert.assertEquals(false, fdht6.isSuccess());
            // put there the data again...
            FuturePut fdht8 = slave1.put(locationKey)
                    .setData(Utils.makeSHAHash(pair1.getPublic().getEncoded()), new Data("test1")).sign().start();
            fdht8.awaitUninterruptibly();
            fdht8.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(true, fdht8.isSuccess());
            // overwrite
            Data data4 = new Data("test1");
            data4.setProtectedEntry();
            FuturePut fdht9 = master.put(locationKey).setData(Utils.makeSHAHash(pair1.getPublic().getEncoded()), data4).sign()
                    .start();
            fdht9.awaitUninterruptibly();
            fdht9.getFutureRequests().awaitUninterruptibly();
            System.err.println("reason " + fdht9.failedReason());
            Assert.assertEquals(true, fdht9.isSuccess());
        } finally {
            // Utils.sleep(1000000);
            master.shutdown();
            slave1.shutdown();
            slave2.shutdown();
        }
    }
    
    @Test
    public void testContentProtectoin() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InterruptedException, InvalidKeyException, SignatureException {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

        KeyPair keyPairPeer1 = gen.generateKeyPair();
        PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838)
                .keyPair(keyPairPeer1).start());
        KeyPair keyPairPeer2 = gen.generateKeyPair();
        PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer())
                .keyPair(keyPairPeer2).start());

        p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
        p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
        KeyPair keyPair = gen.generateKeyPair();

        String locationKey = "location";
        Number160 lKey = Number160.createHash(locationKey);
        String contentKey = "content";
        Number160 cKey = Number160.createHash(contentKey);

        String testData1 = "data1";
        Data data = new Data(testData1).setProtectedEntry();
        // put trough peer 1 with key pair -------------------------------------------------------
        FuturePut futurePut1 = p1.put(lKey).setData(cKey, data).keyPair(keyPair).start();
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
        // put trough peer 2 without key pair ----------------------------------------------------
        String testData2 = "data2";
        Data data2 = new Data(testData2);
        FuturePut futurePut2 = p2.put(lKey).setData(cKey, data2).start();
        futurePut2.awaitUninterruptibly();
        //PutStatus.FAILED_SECURITY
        Assert.assertFalse(futurePut2.isSuccess());
        
        FutureGet futureGet2 = p2.get(lKey).setContentKey(cKey).start();
        futureGet2.awaitUninterruptibly();
        Assert.assertTrue(futureGet2.isSuccess());
        // should have been not modified
        Assert.assertEquals(testData1, (String) futureGet2.getData().object());
        // put trough peer 1 without key pair ----------------------------------------------------
        String testData3 = "data3";
        Data data3 = new Data(testData3);
        FuturePut futurePut3 = p2.put(lKey).setData(cKey, data3).start();
        futurePut3.awaitUninterruptibly();

        // FAILED_SECURITY?
        Assert.assertFalse(futurePut3.isSuccess());
        
        FutureGet futureGet3 = p2.get(lKey).setContentKey(cKey).start();
        futureGet3.awaitUninterruptibly();
        Assert.assertTrue(futureGet3.isSuccess());
        // should have been not modified ---> why it has been modified without giving a key pair?
        Assert.assertEquals(testData1, (String) futureGet3.getData().object());
        Assert.assertEquals(keyPair.getPublic(), futureGet3.getData().publicKey());
        
        //now we store a signed data object and we will get back the public key as well
        data = new Data("Juhuu").setProtectedEntry().sign(keyPair, factory);
        FuturePut futurePut4 = p1.put(lKey).setData(cKey, data).keyPair(keyPair).start();
        futurePut4.awaitUninterruptibly();
        Assert.assertTrue(futurePut4.isSuccess());
        FutureGet futureGet4 = p2.get(lKey).setContentKey(cKey).start();
        futureGet4.awaitUninterruptibly();
        Assert.assertTrue(futureGet4.isSuccess());
        // should have been not modified ---> why it has been modified without giving a key pair?
        Assert.assertEquals("Juhuu", (String) futureGet4.getData().object());
        Assert.assertEquals(keyPair.getPublic(), futureGet4.getData().publicKey());
        

        p1.shutdown().awaitUninterruptibly();
        p2.shutdown().awaitUninterruptibly();
    }
    
    @Test
    public void testContentProtectoinGeneric() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InterruptedException, InvalidKeyException, SignatureException {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

        KeyPair keyPair = gen.generateKeyPair();
        PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838)
                .keyPair(keyPair).start());
        
        String locationKey = "location";
        Number160 lKey = Number160.createHash(locationKey);
        String contentKey = "content";
        Number160 cKey = Number160.createHash(contentKey);
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<1000;i++) {
        	sb.append(i);
        }
        
        Data data = new Data(sb.toString()).setProtectedEntry().sign(keyPair, factory);
        FuturePut futurePut4 = p1.put(lKey).setData(cKey, data).keyPair(keyPair).start();
        futurePut4.awaitUninterruptibly();
        System.err.println(futurePut4.failedReason());
        Assert.assertTrue(futurePut4.isSuccess());
        FutureGet futureGet4 = p1.get(lKey).setContentKey(cKey).start();
        futureGet4.awaitUninterruptibly();
        Assert.assertTrue(futureGet4.isSuccess());
        // should have been not modified ---> why it has been modified without giving a key pair?
        Assert.assertEquals(sb.toString(), (String) futureGet4.getData().object());
        Assert.assertEquals(keyPair.getPublic(), futureGet4.getData().publicKey());
        
        
        p1.shutdown().awaitUninterruptibly();
    }
    
    @Test
    public void testVersionContentProtectoinGeneric() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InterruptedException, InvalidKeyException, SignatureException {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

        KeyPair keyPair1 = gen.generateKeyPair();
        KeyPair keyPair2 = gen.generateKeyPair();
        PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838)
                .keyPair(keyPair1).start());
        PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).ports(4839)
                .keyPair(keyPair2).start());
        
        p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
        p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
        
        String locationKey = "location";
        Number160 lKey = Number160.createHash(locationKey);
        String contentKey = "content";
        Number160 cKey = Number160.createHash(contentKey);
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<1000;i++) {
        	sb.append(i);
        }
        
        Data data1 = new Data(sb.toString()).setProtectedEntry().sign(keyPair1, factory);
        
        FuturePut futurePut4 = p1.put(lKey).setData(cKey, data1).keyPair(keyPair1).setVersionKey(Number160.ZERO).start();
        futurePut4.awaitUninterruptibly();
        Assert.assertTrue(futurePut4.isSuccess());
        
        Data data2 = new Data(sb.toString()).setProtectedEntry().sign(keyPair2, factory);
        
        FuturePut futurePut5 = p2.put(lKey).setData(cKey, data2).keyPair(keyPair2).setVersionKey(Number160.ONE).start();
        futurePut5.awaitUninterruptibly();
        Assert.assertTrue(!futurePut5.isSuccess());
        
        Data data3 = new Data(sb.toString()).setProtectedEntry().sign(keyPair2, factory);
        FuturePut futurePut6 = p1.put(lKey).setData(cKey, data3).keyPair(keyPair1).setVersionKey(Number160.MAX_VALUE).start();
        futurePut6.awaitUninterruptibly();
        Assert.assertTrue(futurePut6.isSuccess());
        
        FuturePut futurePut7 = p2.put(lKey).setData(cKey, data2).keyPair(keyPair2).setVersionKey(Number160.ONE).start();
        futurePut7.awaitUninterruptibly();
        Assert.assertTrue(futurePut7.isSuccess());
        
        
        p1.shutdown().awaitUninterruptibly();
        p2.shutdown().awaitUninterruptibly();
    }
    
    @Test
    public void testChangeDomainProtectionKey() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InterruptedException, InvalidKeyException, SignatureException {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

        KeyPair keyPair1 = gen.generateKeyPair();
        KeyPair keyPair2 = gen.generateKeyPair();
        PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838)
                .keyPair(keyPair1).start());
        PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).ports(4839)
                .keyPair(keyPair2).start());
        
        p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
        p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
        
        Data data = new Data("test");
        FuturePut fp1 = p1.put(Number160.createHash("key1")).setProtectDomain().setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(fp1.isSuccess());
        FuturePut fp2 = p2.put(Number160.createHash("key1")).setProtectDomain().setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(!fp2.isSuccess());
        
        FuturePut fp3 = p1.put(Number160.createHash("key1")).changePublicKey(keyPair2.getPublic()).start().awaitUninterruptibly();
        Assert.assertTrue(fp3.isSuccess());
        FuturePut fp4 = p2.put(Number160.createHash("key1")).setProtectDomain().setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(fp4.isSuccess());
        
        p1.shutdown().awaitUninterruptibly();
        p2.shutdown().awaitUninterruptibly();
    }
    
    @Test
    public void testChangeEntryProtectionKey() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InterruptedException, InvalidKeyException, SignatureException {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

        KeyPair keyPair1 = gen.generateKeyPair();
        KeyPair keyPair2 = gen.generateKeyPair();
        PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838)
                .keyPair(keyPair1).start());
        PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).ports(4839)
                .keyPair(keyPair2).start());
        
        p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
        p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
        
        Data data = new Data("test").setProtectedEntry();
        FuturePut fp1 = p1.put(Number160.createHash("key1")).sign().setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(fp1.isSuccess());
        FuturePut fp2 = p2.put(Number160.createHash("key1")).setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(!fp2.isSuccess());
        
        Data data2 = new Data().setProtectedEntry();
        data2.publicKey(keyPair2.getPublic());
        FuturePut fp3 = p1.put(Number160.createHash("key1")).sign().putMeta().setData(data2).start().awaitUninterruptibly();
        Assert.assertTrue(fp3.isSuccess());
        
        FuturePut fp4 = p2.put(Number160.createHash("key1")).sign().setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(fp4.isSuccess());   
        
        p1.shutdown().awaitUninterruptibly();
        p2.shutdown().awaitUninterruptibly();
    }
    
    @Test
    public void testChangeEntryProtectionKeySignature() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InterruptedException, InvalidKeyException, SignatureException {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

        KeyPair keyPair1 = gen.generateKeyPair();
        KeyPair keyPair2 = gen.generateKeyPair();
        PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838)
                .keyPair(keyPair1).start());
        PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).ports(4839)
                .keyPair(keyPair2).start());
        
        p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
        p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
        
        Data data = new Data("test1").setProtectedEntry().sign(keyPair1, factory);
        FuturePut fp1 = p1.put(Number160.createHash("key1")).sign().setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(fp1.isSuccess());
        FuturePut fp2 = p2.put(Number160.createHash("key1")).setData(data).start().awaitUninterruptibly();
        Assert.assertTrue(!fp2.isSuccess());
        
        Data data2 = new Data("test1").setProtectedEntry().sign(keyPair2, factory).duplicateMeta();
        FuturePut fp3 = p1.put(Number160.createHash("key1")).sign().putMeta().setData(data2).start().awaitUninterruptibly();
        Assert.assertTrue(fp3.isSuccess());
        
        Data retData = p2.get(Number160.createHash("key1")).start().awaitUninterruptibly().getData();
        Assert.assertTrue(retData.verify(keyPair2.getPublic(), factory));
        
        p1.shutdown().awaitUninterruptibly();
        p2.shutdown().awaitUninterruptibly();
    }
    
	@Test
	public void testTTLUpdate() throws IOException, ClassNotFoundException, NoSuchAlgorithmException,
	        InterruptedException, InvalidKeyException, SignatureException {

		PeerDHT p1 = null;
		PeerDHT p2 = null;

		try {
			StorageMemory sm1 = new StorageMemory(1);
			StorageMemory sm2 = new StorageMemory(1);
			p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1))
			        .ports(4838).start(), sm1);
			p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2))
			        .ports(4839).start(), sm2);

			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
			p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

			Data data = new Data("test1");
			data.ttlSeconds(1);
			FuturePut fp1 = p1.put(Number160.createHash("key1")).setData(data).start().awaitUninterruptibly();
			Assert.assertTrue(fp1.isSuccess());

			Thread.sleep(2000);
			Data retData = p2.get(Number160.createHash("key1")).start().awaitUninterruptibly().getData();
			Assert.assertNull(retData);

			FuturePut fp2 = p1.put(Number160.createHash("key1")).setData(data).start().awaitUninterruptibly();
			Assert.assertTrue(fp2.isSuccess());

			Data update = data.duplicateMeta();
			update.ttlSeconds(10);

			FuturePut fp3 = p1.put(Number160.createHash("key1")).putMeta().setData(update).start()
			        .awaitUninterruptibly();
			System.err.println(fp3.failedReason());
			Assert.assertTrue(fp3.isSuccess());

			Thread.sleep(1000);
			retData = p2.get(Number160.createHash("key1")).start().awaitUninterruptibly().getData();
			Assert.assertEquals("test1", retData.object());
		} finally {
			p1.shutdown().awaitUninterruptibly();
			p2.shutdown().awaitUninterruptibly();
		}
	}
    
    
	@Test
	public void testRemoveFromTo2() throws NoSuchAlgorithmException, IOException, InvalidKeyException,
	        SignatureException, ClassNotFoundException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1)
		        .start());
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
		        .start());

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

		KeyPair key1 = gen.generateKeyPair();

		String locationKey = "location";
		Number160 lKey = Number160.createHash(locationKey);
		String domainKey = "domain";
		Number160 dKey = Number160.createHash(domainKey);
		String contentKey = "content";
		Number160 cKey = Number160.createHash(contentKey);

		String testData1 = "data1";
		Data data = new Data(testData1).setProtectedEntry().sign(key1, factory);

		// put trough peer 1 with key pair
		// -------------------------------------------------------

		FuturePut futurePut1 = p1.put(lKey).domainKey(dKey).setData(cKey, data).keyPair(key1).start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());

		FutureGet futureGet1a = p1.get(lKey).domainKey(dKey).setContentKey(cKey).start();
		futureGet1a.awaitUninterruptibly();
		Assert.assertTrue(futureGet1a.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1a.getData().object());

		FutureGet futureGet1b = p2.get(lKey).domainKey(dKey).setContentKey(cKey).start();
		futureGet1b.awaitUninterruptibly();
		Assert.assertTrue(futureGet1b.isSuccess());
		Assert.assertEquals(testData1, (String) futureGet1b.getData().object());

		// remove with correct key pair
		// -----------------------------------------------------------

		FutureRemove futureRemove4 = p1.remove(lKey).from(new Number640(lKey, dKey, cKey, Number160.ZERO))
		        .to(new Number640(lKey, dKey, cKey, Number160.MAX_VALUE)).keyPair(key1).start();
		futureRemove4.awaitUninterruptibly();
		Assert.assertTrue(futureRemove4.isSuccess());

		FutureGet futureGet4a = p2.get(lKey).domainKey(dKey).setContentKey(cKey).start();
		futureGet4a.awaitUninterruptibly();
		// we did not find the data
		Assert.assertTrue(futureGet4a.isFailed());
		// should have been removed
		Assert.assertNull(futureGet4a.getData());

		FutureGet futureGet4b = p2.get(lKey).setContentKey(cKey).start();
		futureGet4b.awaitUninterruptibly();
		// we did not find the data
		Assert.assertTrue(futureGet4b.isFailed());
		// should have been removed
		Assert.assertNull(futureGet4b.getData());

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}
	
	@Test
	public void testRemoveFutureResponse() throws NoSuchAlgorithmException, IOException, InvalidKeyException,
	        SignatureException, ClassNotFoundException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4838).keyPair(keyPairPeer1)
		        .start());
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
		        .start());

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

		KeyPair key1 = gen.generateKeyPair();

		String locationKey = "location";
		Number160 lKey = Number160.createHash(locationKey);
		//String domainKey = "domain";
		//Number160 dKey = Number160.createHash(domainKey);
		String contentKey = "content";
		Number160 cKey = Number160.createHash(contentKey);

		String testData1 = "data1";
		Data data = new Data(testData1).setProtectedEntry().sign(key1, factory);
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

		// try to remove without key pair using the direct remove
		// -------------------------------
		// should fail

		FutureRemove futureRemoveDirect = p1.remove(lKey).contentKey(cKey).start();
		futureRemoveDirect.awaitUninterruptibly();
		// try to remove without key pair using the from/to
		// -------------------------------------
		// should fail
		FutureRemove futureRemoveFromTo = p1.remove(lKey).from(new Number640(lKey, Number160.ZERO, cKey, Number160.ZERO))
		        .to(new Number640(lKey, Number160.ZERO, cKey, Number160.MAX_VALUE)).start();
		futureRemoveFromTo.awaitUninterruptibly();

		Assert.assertEquals(futureRemoveDirect.isSuccess(), futureRemoveFromTo.isSuccess());
		Assert.assertFalse(futureRemoveDirect.isSuccess());
		Assert.assertFalse(futureRemoveFromTo.isSuccess());

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}
	
	
	@Test
	public void testChangeProtectionKey() throws NoSuchAlgorithmException, IOException, InvalidKeyException,
	        SignatureException, ClassNotFoundException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4834).keyPair(keyPairPeer1)
		        .start());
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
		        .start());
		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
		KeyPair keyPair1 = gen.generateKeyPair();
		KeyPair keyPair2 = gen.generateKeyPair();
		Number160 lKey = Number160.createHash("location");
		Number160 dKey = Number160.createHash("domain");
		Number160 cKey = Number160.createHash("content");
		// initial put
		String testData = "data";
		Data data = new Data(testData).setProtectedEntry().sign(keyPair1, factory);
		FuturePut futurePut1 = p1.put(lKey).domainKey(dKey).sign().setData(cKey, data).keyPair(keyPair1).start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());
		Data retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(testData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair1.getPublic(), factory));
		retData = p2.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(testData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair1.getPublic(), factory));
		// change the key pair to the new one using an empty data object
		data = new Data(testData).setProtectedEntry().sign(keyPair2, factory).duplicateMeta();
		// use the old protection key to sign the message
		FuturePut futurePut2 = p1.put(lKey).domainKey(dKey).sign().putMeta().setData(cKey, data)
		        .keyPair(keyPair1).start();
		futurePut2.awaitUninterruptibly();
		Assert.assertTrue(futurePut2.isSuccess());
		retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(testData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair2.getPublic(), factory));
		retData = p2.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(testData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair2.getPublic(), factory));
		// should be not possible to modify
		data = new Data().setProtectedEntry().sign(keyPair1, factory);
		FuturePut futurePut3 = p1.put(lKey).domainKey(dKey).sign().setData(cKey, data).keyPair(keyPair1).start();
		futurePut3.awaitUninterruptibly();
		Assert.assertFalse(futurePut3.isSuccess());
		retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(testData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair2.getPublic(), factory));
		retData = p2.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(testData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair2.getPublic(), factory));
		// modify with new protection key
		String newTestData = "new data";
		data = new Data(newTestData).setProtectedEntry().sign(keyPair2, factory);
		FuturePut futurePut4 = p1.put(lKey).domainKey(dKey).sign().setData(cKey, data).keyPair(keyPair2).start();
		futurePut4.awaitUninterruptibly();
		Assert.assertTrue(futurePut4.isSuccess());
		retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(newTestData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair2.getPublic(), factory));
		retData = p2.get(lKey).domainKey(dKey).setContentKey(cKey).start().awaitUninterruptibly().getData();
		Assert.assertEquals(newTestData, (String) retData.object());
		Assert.assertTrue(retData.verify(keyPair2.getPublic(), factory));
		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}

	@Test
	public void testChangeProtectionKeyWithVersions() throws NoSuchAlgorithmException, IOException,
	        ClassNotFoundException, InvalidKeyException, SignatureException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4834).keyPair(keyPairPeer1)
		        .start());
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
		        .start());
		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();
		KeyPair keyPair1 = gen.generateKeyPair();
		KeyPair keyPair2 = gen.generateKeyPair();
		Number160 lKey = Number160.createHash("location");
		Number160 dKey = Number160.createHash("domain");
		Number160 cKey = Number160.createHash("content");
		// put the first version of the content with key pair 1
		Number160 vKey1 = Number160.createHash("version1");
		Data data = new Data("data1v1").setProtectedEntry().sign(keyPair1, factory);
		data.addBasedOn(Number160.ZERO);
		FuturePut futurePut1 = p1.put(lKey).domainKey(dKey).sign().setData(cKey, data).keyPair(keyPair1)
		        .setVersionKey(vKey1).start();
		futurePut1.awaitUninterruptibly();
		Assert.assertTrue(futurePut1.isSuccess());
		// add another version with the correct key pair 1
		Number160 vKey2 = Number160.createHash("version2");
		data = new Data("data1v2").setProtectedEntry().sign(keyPair1, factory);
		data.addBasedOn(vKey1);
		FuturePut futurePut2 = p1.put(lKey).domainKey(dKey).sign().setData(cKey, data).keyPair(keyPair1)
		        .setVersionKey(vKey2).start();
		futurePut2.awaitUninterruptibly();
		Assert.assertTrue(futurePut2.isSuccess());
		// put new version with other key pair 2 (expected to fail)
		Number160 vKey3 = Number160.createHash("version3");
		data = new Data("data1v3").setProtectedEntry().sign(keyPair2, factory);
		data.addBasedOn(vKey2);
		FuturePut futurePut3 = p1.put(lKey).domainKey(dKey).setData(cKey, data).keyPair(keyPair2)
		        .setVersionKey(vKey3).start();
		futurePut3.awaitUninterruptibly();
		Assert.assertFalse(futurePut3.isSuccess());
		// change the key pair to the new one using an empty data object
		data = new Data().setProtectedEntry().sign(keyPair2, factory).duplicateMeta();
		// use the old protection key to sign the message
		FuturePut futurePut4 = p1.put(lKey).domainKey(dKey).sign().putMeta().setData(cKey, data)
		        .keyPair(keyPair1).start();
		futurePut4.awaitUninterruptibly();
		Assert.assertFalse(futurePut4.isSuccess());
		// verify if the two versions have the new protection key
		Data retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).setVersionKey(vKey1).start()
		        .awaitUninterruptibly().getData();
		Assert.assertEquals("data1v1", (String) retData.object());
		Assert.assertFalse(retData.verify(keyPair2.getPublic(), factory));
		retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).setVersionKey(vKey2).start()
		        .awaitUninterruptibly().getData();
		//Assert.assertEquals("data1v2", (String) retData.object());
		Assert.assertFalse(retData.verify(keyPair2.getPublic(), factory));
		// add another version with the new protection key
		Number160 vKey4 = Number160.createHash("version4");
		data = new Data("data1v4").setProtectedEntry().sign(keyPair2, factory);
		data.addBasedOn(vKey2);
		FuturePut futurePut5 = p1.put(lKey).domainKey(dKey).sign().setData(cKey, data).keyPair(keyPair2)
		        .setVersionKey(vKey4).start();
		futurePut5.awaitUninterruptibly();
		Assert.assertTrue(futurePut5.isSuccess());
		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}
	
	@Test
	public void testGetMeta() throws NoSuchAlgorithmException, IOException, ClassNotFoundException,
	        InvalidKeyException, SignatureException {
		PeerDHT p1 = null;
		PeerDHT p2 = null;
		try {
			KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
			KeyPair keyPairPeer1 = gen.generateKeyPair();
			p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4834).keyPair(keyPairPeer1)
			        .start());
			KeyPair keyPairPeer2 = gen.generateKeyPair();
			p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
			        .start());
			p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
			p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

			KeyPair keyPair1 = gen.generateKeyPair();

			Number160 lKey = Number160.createHash("location");
			Data data = new Data("data1v11").setProtectedEntry().sign(keyPair1, factory);

			FuturePut futurePut = p1.put(lKey).sign().setData(data).keyPair(keyPair1).start();
			futurePut.awaitUninterruptibly();
			Assert.assertTrue(futurePut.isSuccess());

			FutureDigest futureDigest = p1.digest(lKey).returnMetaValues().start();
			futureDigest.awaitUninterruptibly();
			Assert.assertTrue(futureDigest.isSuccess());
			Data dataMeta = futureDigest.getDigest().dataMap().values().iterator().next();

			Assert.assertEquals(keyPair1.getPublic(), dataMeta.publicKey());
			Assert.assertEquals(data.signature(), dataMeta.signature());
			Assert.assertEquals(0, dataMeta.length());
		} finally {
			p1.shutdown().awaitUninterruptibly();
			p2.shutdown().awaitUninterruptibly();
		}
	}
	
	@Test
	public void testSignature() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException {
		SignatureFactory signatureFactory = new RSASignatureFactory();

		// generate some test data
		Data testData = new Data("test");
		// create a content protection key
		
		KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
		KeyPair protectionKey = gen.generateKeyPair();

		SignatureCodec signature = signatureFactory.sign(protectionKey.getPrivate(), testData.buffer());

		boolean isVerified = signatureFactory.verify(protectionKey.getPublic(), testData.buffer(), signature);

		Assert.assertTrue(isVerified);
	}
	
	@Test
	public void testTTLDecrement() throws IOException, ClassNotFoundException, NoSuchAlgorithmException,
	        InvalidKeyException, SignatureException, InterruptedException {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		
		StorageMemory sm1 = new StorageMemory(1);
		StorageMemory sm2 = new StorageMemory(1);
		
		PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4834).keyPair(keyPairPeer1)
		        .start(), sm1);
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
		        .start(), sm2);

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

		KeyPair keyPair1 = gen.generateKeyPair();

		Number160 lKey = Number160.createHash("location");
		Number160 dKey = Number160.createHash("domain");
		Number160 cKey = Number160.createHash("content");
		Number160 vKey = Number160.createHash("version");
		Number160 bKey = Number160.ZERO;

		int ttl = 4;

		String testData = "data";
		Data data = new Data(testData).setProtectedEntry().lazySign(keyPair1);
		data.ttlSeconds(ttl).addBasedOn(bKey);

		// initial put
		FuturePut futurePut = p1.put(lKey).domainKey(dKey).setData(cKey, data).setVersionKey(vKey).keyPair(keyPair1)
		        .start();
		futurePut.awaitUninterruptibly();
		Assert.assertTrue(futurePut.isSuccess());

		// wait a moment, so that the ttl decrements
		Thread.sleep(2000);

		// check decrement of ttl through a normal get
		FutureGet futureGet = p1.get(lKey).domainKey(dKey).setContentKey(cKey).setVersionKey(vKey).start();
		futureGet.awaitUninterruptibly();
		Assert.assertTrue(futureGet.isSuccess());
		System.err.println(futureGet.getData().ttlSeconds());
		Assert.assertTrue(ttl > futureGet.getData().ttlSeconds());

		// check decrement of ttl through a get meta
		FutureDigest futureDigest = p1.digest(lKey).domainKey(dKey).setContentKey(cKey).setVersionKey(vKey)
		        .returnMetaValues().start();
		futureDigest.awaitUninterruptibly();
		Assert.assertTrue(futureDigest.isSuccess());
		Data dataMeta = futureDigest.getDigest().dataMap().values().iterator().next();
		Assert.assertTrue(ttl > dataMeta.ttlSeconds());

		// wait again a moment, till data gets expired
		Thread.sleep(2000);

		// check if data has been removed
		Data retData = p2.get(lKey).domainKey(dKey).setContentKey(cKey).setVersionKey(vKey).start()
		        .awaitUninterruptibly().getData();
		Assert.assertNull(retData);

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}
	
	@Test
	public void testChangeProtectionKeyWithReusedSignature() throws Exception {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");

		// create custom RSA factories
		SignatureFactory factory = new RSASignatureFactory();
		SignatureCodec codec = new RSASignatureCodec();

		// replace default signature factories
		ChannelClientConfiguration clientConfig = PeerBuilder.createDefaultChannelClientConfiguration();
		clientConfig.signatureFactory(factory);
		ChannelServerConficuration serverConfig = PeerBuilder.createDefaultChannelServerConfiguration();
		serverConfig.signatureFactory(factory);

		KeyPair keyPairPeer1 = gen.generateKeyPair();
		PeerDHT p1 = new PeerDHT(new PeerBuilder(Number160.createHash(1)).ports(4834).keyPair(keyPairPeer1)
		        .channelClientConfiguration(clientConfig)
		        .channelServerConfiguration(serverConfig).start());
		KeyPair keyPairPeer2 = gen.generateKeyPair();
		PeerDHT p2 = new PeerDHT(new PeerBuilder(Number160.createHash(2)).masterPeer(p1.peer()).keyPair(keyPairPeer2)
		        .channelClientConfiguration(clientConfig)
		        .channelServerConfiguration(serverConfig).start());

		p2.peer().bootstrap().peerAddress(p1.peerAddress()).start().awaitUninterruptibly();
		p1.peer().bootstrap().peerAddress(p2.peerAddress()).start().awaitUninterruptibly();

		KeyPair keyPairOld = gen.generateKeyPair();
		KeyPair keyPairNew = gen.generateKeyPair();

		Number160 lKey = Number160.createHash("location");
		Number160 dKey = Number160.createHash("domain");
		Number160 cKey = Number160.createHash("content");
		Number160 vKey = Number160.createHash("version");
		Number160 bKey = Number160.ZERO;

		int ttl = 10;

		String testData = "data";
		Data data = new Data(testData).setProtectedEntry().sign(keyPairOld, factory);
		data.ttlSeconds(ttl).addBasedOn(bKey);

		// initial put of some test data
		FuturePut futurePut = p1.put(lKey).domainKey(dKey).setData(cKey, data).setVersionKey(vKey)
		        .keyPair(keyPairOld).start();
		futurePut.awaitUninterruptibly();
		Assert.assertTrue(futurePut.isSuccess());

		// create signature with old key pair having the data object
		byte[] signature1 = factory.sign(keyPairOld.getPrivate(), data.buffer()).encode();

		// decrypt signature to get hash of the object
		Cipher rsa = Cipher.getInstance("RSA");
		rsa.init(Cipher.DECRYPT_MODE, keyPairOld.getPublic());
		byte[] hash = rsa.doFinal(signature1);

		// encrypt hash with new key pair to get the new signature (without
		// having the data object)
		rsa = Cipher.getInstance("RSA");
		rsa.init(Cipher.ENCRYPT_MODE, keyPairNew.getPrivate());
		byte[] signatureNew = rsa.doFinal(hash);

		// verify old content protection keys
		Data retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).setVersionKey(vKey).start()
		        .awaitUninterruptibly().getData();
		Assert.assertTrue(retData.verify(keyPairOld.getPublic(), factory));

		// create a dummy data object for changing the content protection key
		// through a put meta
		Data dummyData = new Data();
		dummyData.addBasedOn(bKey).ttlSeconds(ttl);
		// assign the reused hash from signature (don't forget to set the
		// signedflag)
		dummyData.signature(codec.decode(signatureNew)).signed(true).duplicateMeta();
		// change content protection key through a put meta
		FuturePut futurePutMeta = p1.put(lKey).domainKey(dKey).putMeta().setData(cKey, dummyData)
		        .setVersionKey(vKey).keyPair(keyPairOld).start();
		futurePutMeta.awaitUninterruptibly();
		Assert.assertTrue(futurePutMeta.isSuccess());

		// verify new content protection keys
		retData = p1.get(lKey).domainKey(dKey).setContentKey(cKey).setVersionKey(vKey).start()
		        .awaitUninterruptibly().getData();
		Assert.assertTrue(retData.verify(keyPairNew.getPublic(), factory));

		p1.shutdown().awaitUninterruptibly();
		p2.shutdown().awaitUninterruptibly();
	}
}
