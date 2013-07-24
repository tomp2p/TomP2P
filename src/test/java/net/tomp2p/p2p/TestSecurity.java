package net.tomp2p.p2p;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric.ProtectionEnable;
import net.tomp2p.storage.StorageGeneric.ProtectionMode;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestSecurity {
    final private static Random rnd = new Random(42L);

    @Test
    public void testPublicKeyReceived() throws Exception {
        final Random rnd = new Random(43L);
        Peer master = null;
        Peer slave1 = null;
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        // make master
        try {
            master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).makeAndListen();
            // make slave
            slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).makeAndListen();
            final AtomicBoolean gotPK = new AtomicBoolean(false);
            // set storage to test PK
            slave1.getPeerBean().setStorage(new StorageMemory() {
                @Override
                public PutStatus put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data newData,
                        PublicKey publicKey, boolean putIfAbsent, boolean domainProtection) {
                    System.err.println("P is " + publicKey);
                    gotPK.set(publicKey != null);
                    System.err.println("PK is " + gotPK);
                    return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent,
                            domainProtection);
                }
            });
            // perfect routing
            boolean peerInMap1 = master.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
            boolean peerInMap2 = slave1.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
            Assert.assertEquals(true, peerInMap1);
            Assert.assertEquals(true, peerInMap2);
            //
            Number160 locationKey = new Number160(50);

            RequestP2PConfiguration rc = new RequestP2PConfiguration(1, 1, 0);
            master.put(locationKey).setData(new Data(new byte[100000])).setRequestP2PConfiguration(rc).setSignMessage()
                    .start().awaitUninterruptibly();
            // master.put(locationKey, new Data("test"),
            // cs1).awaitUninterruptibly();
            Assert.assertEquals(true, gotPK.get());
            // without PK, this test should fail.
            master.put(locationKey).setData(new Data("test1")).setRequestP2PConfiguration(rc).start()
                    .awaitUninterruptibly();
            Assert.assertEquals(false, gotPK.get());
        } finally {
            master.halt();
            slave1.halt();
        }
    }

    @Test
    public void testPublicKeyReceivedDomain() throws Exception {
        final Random rnd = new Random(43L);
        Peer master = null;
        try {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
            KeyPair pair1 = gen.generateKeyPair();
            // make master
            master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).makeAndListen();
            // make slave
            final AtomicBoolean gotPK = new AtomicBoolean(false);
            // set storage to test PK
            master.getPeerBean().setStorage(new StorageMemory() {
                @Override
                public PutStatus put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data newData,
                        PublicKey publicKey, boolean putIfAbsent, boolean domainProtection) {
                    gotPK.set(publicKey != null);
                    System.err.println("PK is " + gotPK);
                    return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent,
                            domainProtection);
                }
            });
            //
            Number160 locationKey = new Number160(50);
            RequestP2PConfiguration rc = new RequestP2PConfiguration(1, 1, 0);
            master.put(locationKey).setData(Number160.ONE, new Data(new byte[2000])).setRequestP2PConfiguration(rc)
                    .setDomainKey(Number160.ONE).setSignMessage().start().awaitUninterruptibly();
            Assert.assertEquals(true, gotPK.get());
            // without PK
            master.put(locationKey).setData(Number160.ONE, new Data("test1")).setRequestP2PConfiguration(rc)
                    .setDomainKey(Number160.ONE).start().awaitUninterruptibly();
            Assert.assertEquals(false, gotPK.get());
        } finally {
            master.halt();
        }
    }

    @Test
    public void testProtection() throws Exception {
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
        try {
            master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).makeAndListen();
            master.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).makeAndListen();
            slave1.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave2 = new PeerMaker(new Number160(rnd)).setKeyPair(pair3).setMasterPeer(master).makeAndListen();
            slave2.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
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
            FutureDHT fdht1 = master.put(locationKey).setData(new Number160(10), new Data("test1"))
                    .setDomainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).setProtectDomain().start();
            fdht1.awaitUninterruptibly();
            Assert.assertEquals(true, fdht1.isSuccess());
            // try to insert in same domain from different peer
            FutureDHT fdht2 = slave1.put(locationKey).setData(new Number160(11), new Data("tes2"))
                    .setDomainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).setProtectDomain().start();
            fdht2.awaitUninterruptibly();
            Assert.assertEquals(false, fdht2.isSuccess());
            // insert from same peer but with public key protection
            FutureDHT fdht3 = slave2.put(locationKey).setData(new Number160(12), new Data("tes2"))
                    .setDomainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).setProtectDomain().start();
            fdht3.awaitUninterruptibly();
            Assert.assertEquals(true, fdht3.isSuccess());
            //
            // get at least 3 results, because we want to test the domain
            // removel feature
            RequestP2PConfiguration rc = new RequestP2PConfiguration(3, 3, 3);
            FutureDHT fdht4 = slave1.get(locationKey).setAll().setRequestP2PConfiguration(rc)
                    .setDomainKey(Utils.makeSHAHash(pair3.getPublic().getEncoded())).start();
            fdht4.awaitUninterruptibly();
            Assert.assertEquals(true, fdht4.isSuccess());
            Assert.assertEquals(2, fdht4.getDataMap().size());
        } finally {
            master.halt();
            slave1.halt();
            slave2.halt();
        }
    }

    @Test
    public void testProtectionWithRemove() throws Exception {
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
        try {
            master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).makeAndListen();
            master.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).makeAndListen();
            slave1.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave2 = new PeerMaker(new Number160(rnd)).setKeyPair(pair3).setMasterPeer(master).makeAndListen();
            slave2.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
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
            FutureDHT fdht1 = master.put(locationKey).setData(new Data("test1"))
                    .setDomainKey(Utils.makeSHAHash(pair1.getPublic().getEncoded())).setProtectDomain().start();
            fdht1.awaitUninterruptibly();
            // remove from different peer, should fail
            FutureDHT fdht2 = slave1.remove(locationKey)
                    .setDomainKey(Utils.makeSHAHash(pair1.getPublic().getEncoded())).setSignMessage().start();
            fdht2.awaitUninterruptibly();
            Assert.assertEquals(0, fdht2.getKeys().size());
            // this should work
            FutureDHT fdht3 = master.remove(locationKey)
                    .setDomainKey(Utils.makeSHAHash(pair1.getPublic().getEncoded())).setSignMessage().start();
            fdht3.awaitUninterruptibly();
            Assert.assertEquals(1, fdht3.getKeys().size());
        } finally {
            master.halt();
            slave1.halt();
            slave2.halt();
        }
    }

    @Test
    public void testProtectionDomain() throws Exception {
        final Random rnd = new Random(43L);
        Peer master = null;
        Peer slave1 = null;
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        // make master
        try {
            master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).makeAndListen();
            // make slave
            slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).makeAndListen();
            master.getPeerBean().setStorage(new StorageMemory() {
                public PutStatus put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data newData,
                        PublicKey publicKey, boolean putIfAbsent, boolean domainProtection) {
                    // System.out.println("store1");
                    return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent,
                            domainProtection);
                }
            });
            slave1.getPeerBean().setStorage(new StorageMemory() {
                @Override
                public PutStatus put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data newData,
                        PublicKey publicKey, boolean putIfAbsent, boolean domainProtection) {
                    // System.out.println("store2");
                    return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent,
                            domainProtection);
                }
            });
            // perfect routing
            boolean peerInMap1 = master.getPeerBean().getPeerMap().peerFound(slave1.getPeerAddress(), null);
            boolean peerInMap2 = slave1.getPeerBean().getPeerMap().peerFound(master.getPeerAddress(), null);
            Assert.assertEquals(true, peerInMap1);
            Assert.assertEquals(true, peerInMap2);

            // since we have to peers, we store on both, otherwise this test may
            // sometimes work, sometimes not.
            RequestP2PConfiguration rc = new RequestP2PConfiguration(1, 1, 1);
            Number160 locationKey = Number160.createHash("loctaion");
            FutureDHT futureDHT = master.put(locationKey).setData(Number160.createHash("content1"), new Data("test1"))
                    .setDomainKey(Number160.createHash("domain1")).setProtectDomain().setRequestP2PConfiguration(rc)
                    .start();
            futureDHT.awaitUninterruptibly();
            Assert.assertEquals(true, futureDHT.isSuccess());
            // now the slave stores with different in the same domain. This
            // should not work
            futureDHT = slave1.put(locationKey).setData(Number160.createHash("content2"), new Data("test2"))
                    .setDomainKey(Number160.createHash("domain1")).setProtectDomain().setRequestP2PConfiguration(rc)
                    .start();
            futureDHT.awaitUninterruptibly();
            System.err.println(futureDHT.getFailedReason());
            Assert.assertEquals(false, futureDHT.isSuccess());
        } finally {
            master.halt();
            slave1.halt();
        }
    }

    @Test
    public void testSecurePutGet1() throws Exception {
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
        try {

            // make slave
            master = new PeerMaker(new Number160(rnd)).setKeyPair(pair1).setPorts(4001).makeAndListen();
            master.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave1 = new PeerMaker(new Number160(rnd)).setKeyPair(pair2).setMasterPeer(master).makeAndListen();
            slave1.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
            slave2 = new PeerMaker(new Number160(rnd)).setKeyPair(pair3).setMasterPeer(master).makeAndListen();
            slave2.getPeerBean()
                    .getStorage()
                    .setProtection(ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY, ProtectionEnable.ALL,
                            ProtectionMode.MASTER_PUBLIC_KEY);
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

            Data data1 = new Data("test1");
            data1.setProtectedEntry(true);
            FutureDHT fdht1 = master.put(locationKey).setData(data1).start();
            fdht1.awaitUninterruptibly();
            fdht1.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(true, fdht1.isSuccess());
            // store again
            Data data2 = new Data("test1");
            data2.setProtectedEntry(true);
            FutureDHT fdht2 = slave1.put(locationKey).setData(data2).start();
            fdht2.awaitUninterruptibly();
            fdht2.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(0, fdht2.getKeys().size());
            Assert.assertEquals(false, fdht2.isSuccess());
            // Utils.sleep(1000000);
            // try to removze it
            FutureDHT fdht3 = slave2.remove(locationKey).start();
            fdht3.awaitUninterruptibly();
            // true, since we have domain protection yet
            Assert.assertEquals(true, fdht3.isSuccess());
            Assert.assertEquals(0, fdht3.getKeys().size());
            // try to put another thing
            Data data3 = new Data("test2");
            data3.setProtectedEntry(true);
            FutureDHT fdht4 = master.put(locationKey).setData(new Number160(33), data3).start();
            fdht4.awaitUninterruptibly();
            fdht4.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(true, fdht4.isSuccess());
            // get it
            FutureDHT fdht7 = slave2.get(locationKey).setAll().start();
            fdht7.awaitUninterruptibly();
            Assert.assertEquals(2, fdht7.getDataMap().size());
            Assert.assertEquals(true, fdht7.isSuccess());
            // if(true)
            // System.exit(0);
            // try to remove for real, all
            FutureDHT fdht5 = master.remove(locationKey).setAll().setSignMessage().start();
            fdht5.awaitUninterruptibly();
            System.err.println(fdht5.getFailedReason());
            Assert.assertEquals(true, fdht5.isSuccess());
            // get all, they should be removed now
            FutureDHT fdht6 = slave2.get(locationKey).setAll().start();
            fdht6.awaitUninterruptibly();
            Assert.assertEquals(0, fdht6.getDataMap().size());
            Assert.assertEquals(false, fdht6.isSuccess());
            // put there the data again...
            FutureDHT fdht8 = slave1.put(locationKey)
                    .setData(Utils.makeSHAHash(pair1.getPublic().getEncoded()), new Data("test1")).start();
            fdht8.awaitUninterruptibly();
            fdht8.getFutureRequests().awaitUninterruptibly();
            Assert.assertEquals(true, fdht8.isSuccess());
            // overwrite
            Data data4 = new Data("test1");
            data4.setProtectedEntry(true);
            FutureDHT fdht9 = master.put(locationKey).setData(Utils.makeSHAHash(pair1.getPublic().getEncoded()), data4)
                    .start();
            fdht9.awaitUninterruptibly();
            fdht9.getFutureRequests().awaitUninterruptibly();
            System.err.println("reason " + fdht9.getFailedReason());
            Assert.assertEquals(true, fdht9.isSuccess());
        } finally {
            // Utils.sleep(1000000);
            master.halt();
            slave1.halt();
            slave2.halt();
        }
    }
}
