package net.tomp2p.dht;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.KeyMapByte;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.NeighborRPC.SearchValues;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStorageDHT {
    

    private static String DIR1;

    private static String DIR2;
    
    public static final int PORT_TCP = 5001;
    public static final int PORT_UDP = 5002;

    @Before
    public void before() throws IOException {
        DIR1 = UtilsDHT2.createTempDirectory().getCanonicalPath();
        DIR2 = UtilsDHT2.createTempDirectory().getCanonicalPath();
    }

    @After
    public void after() {
        cleanUp(DIR1);
        cleanUp(DIR2);
    }

    private void cleanUp(String dir) {
        File f = new File(dir);
        f.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.isFile())
                    pathname.delete();
                return false;
            }
        });
        f.delete();
    }
    
    @Test
    public void testNeigbhorBloomfilter() throws Exception {
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).start();
            PeerAddress[] pa = UtilsDHT2.createDummyAddress(300, PORT_TCP, PORT_UDP);
            for (int i = 0; i < pa.length; i++) {
                sender.peerBean().peerMap().peerFound(pa[i], null, null);
            }
            new NeighborRPC(sender.peerBean(), sender.peer().connectionBean());
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).start();
            NeighborRPC neighbors2 = new NeighborRPC(recv1.peerBean(), recv1.peer().connectionBean());
            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(1, 0);

            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.channelCreator();

            SimpleBloomFilter<Number160> bf = new SimpleBloomFilter<Number160>(20, 10);
            for (int i = 0; i < 10; i++) {
                Number640 key = new Number640(new Number160(0x1), Number160.ZERO, Number160.createHash(i), Number160.ZERO);
                sender.storageLayer().put(key, new Data("test"), null, false, false, false);
                bf.add(Number160.createHash(i));
            }

            SearchValues v = new SearchValues(new Number160("0x1"), null, bf);
            FutureResponse fr = neighbors2.closeNeighbors(sender.peerAddress(), v, 
                    Type.REQUEST_2, cc, new DefaultConnectionConfiguration());

            fr.awaitUninterruptibly();
            // Thread.sleep(10000000);
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.responseMessage().neighborsSet(0);
            // we are able to fit 40 neighbors into 1400 bytes
            Assert.assertEquals(33, pas.size());
            Assert.assertEquals(10, (int) fr.responseMessage().intAt(0));
            Assert.assertEquals(new Number160("0x1"), pas.neighbors().iterator().next().peerId());
            Assert.assertEquals(PORT_TCP, pas.neighbors().iterator().next().tcpPort());
            Assert.assertEquals(PORT_UDP, pas.neighbors().iterator().next().udpPort());
            cc.shutdown();
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testAdd() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            
            StorageRPC smmSender = sender.storeRPC(); 

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            Collection<Data> dataSet = new HashSet<Data>();
            dataSet.add(new Data(1));
            AddBuilder addBuilder = new AddBuilder(recv1, new Number160(33));
            addBuilder.domainKey(Number160.createHash("test"));
            addBuilder.dataSet(dataSet);
            addBuilder.versionKey(Number160.ZERO);
            // addBuilder.setList();
            // addBuilder.random(new Random(42));
            FutureResponse fr = smmSender.add(recv1.peerAddress(), addBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            // add a the same data twice
            fr = smmSender.add(recv1.peerAddress(), addBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());

            Number320 key = new Number320(new Number160(33), Number160.createHash("test"));
            // Set<Number480> tofetch = new HashSet<Number480>();
            Number640 from = new Number640(key, Number160.ZERO, Number160.ZERO);
            Number640 to = new Number640(key, Number160.MAX_VALUE, Number160.MAX_VALUE);
            SortedMap<Number640, Data> c = storeRecv.subMap(from, to, -1, true);
            Assert.assertEquals(1, c.size());
            for (Data data : c.values()) {
                Assert.assertEquals((Integer) 1, (Integer) data.object());
            }

            // now add again, but as a list

            addBuilder.list();
            addBuilder.random(new Random(42));

            fr = smmSender.add(recv1.peerAddress(), addBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());

            key = new Number320(new Number160(33), Number160.createHash("test"));
            // Set<Number480> tofetch = new HashSet<Number480>();
            from = new Number640(key, Number160.ZERO, Number160.ZERO);
            to = new Number640(key, Number160.MAX_VALUE, Number160.MAX_VALUE);
            c = storeRecv.subMap(from, to, -1, true);
            Assert.assertEquals(2, c.size());
            for (Data data : c.values()) {
                Assert.assertEquals((Integer) 1, (Integer) data.object());
            }

        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testStorePut() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            
            StorageRPC smmSender = sender.storeRPC();
            
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            Data test = new Data(me1);
            Data test2 = new Data(me2);
            tmp.put(new Number160(77), test);
            tmp.put(new Number160(88), test2);
            System.err.println(recv1.peerAddress());

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.versionKey(Number160.ZERO);
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());

            Number640 key1 = new Number640(new Number160(33), Number160.createHash("test"), new Number160(77), Number160.ZERO);
            Data c = storeRecv.get(key1);
            
            Assert.assertEquals(test, c);
            //Thread.sleep(10000000);
            //
            tmp.clear();
            me1 = new byte[] { 5, 6, 7 };
            me2 = new byte[] { 8, 9, 1, 5 };
            test = new Data(me1);
            test2 = new Data(me2);
            tmp.put(new Number160(77), test);
            tmp.put(new Number160(88), test2);
            putBuilder.dataMapContent(tmp);
            fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            Map<Number640, Data> result2 = storeRecv.subMap(key1.minContentKey(), key1.maxContentKey(), -1, true);
            Assert.assertEquals(result2.size(), 2);
            //Number480 search = new Number480(key, new Number160(88));
            Number640 key2 = new Number640(new Number160(33), Number160.createHash("test"), new Number160(88), Number160.ZERO);
            c = result2.get(key2);
            Assert.assertEquals(c, test2);

        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testStorePutIfAbsent() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            Data test = new Data(me1);
            Data test2 = new Data(me2);
            tmp.put(new Number160(77), test);
            tmp.put(new Number160(88), test2);

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);
            //putBuilder.set

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Number640 key = new Number640(new Number160(33), Number160.createHash("test"), new Number160(77), Number160.ZERO);
            Data c = storeRecv.get(key);

            Assert.assertEquals(c, test);
            //
            tmp.clear();
            byte[] me3 = new byte[] { 5, 6, 7 };
            byte[] me4 = new byte[] { 8, 9, 1, 5 };
            tmp.put(new Number160(77), new Data(me3));
            tmp.put(new Number160(88), new Data(me4));

            putBuilder.putIfAbsent();

            fr = smmSender.putIfAbsent(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // we cannot put anything there, since there already is
            Assert.assertEquals(true, fr.isSuccess());
            Map<Number640, Byte> putKeys = fr.responseMessage().keyMapByte(0).keysMap();
            Assert.assertEquals(2, putKeys.size());
            Assert.assertEquals(Byte.valueOf((byte)PutStatus.FAILED_NOT_ABSENT.ordinal()), putKeys.values().iterator().next());
            Number640 key2 = new Number640(new Number160(33), Number160.createHash("test"), new Number160(88), Number160.ZERO);
            c = storeRecv.get(key2);
            Assert.assertEquals(c, test2);
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testStorePutGetTCP() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), Number160.ZERO, tmp);
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // get

            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            getBuilder.contentKeys(tmp.keySet());
            getBuilder.versionKey(Number160.ZERO);

            fr = smmSender.get(recv1.peerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            System.err.println(fr.failedReason());
            Message m = fr.responseMessage();
            Map<Number640, Data> stored = m.dataMap(0).dataMap();
            compare(dataMap.dataMap(), stored);
            System.err.println("done!");
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testStorePutGetUDP() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), Number160.ZERO, tmp);
            putBuilder.dataMapContent(tmp);
            putBuilder.forceUDP();
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());

            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            getBuilder.contentKeys(tmp.keySet());
            getBuilder.forceUDP();
            getBuilder.versionKey(Number160.ZERO);

            // get
            fr = smmSender.get(recv1.peerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Message m = fr.responseMessage();
            Map<Number640, Data> stored = m.dataMap(0).dataMap();
            compare(dataMap.dataMap(), stored);
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    private void compare(Map<Number640, Data> tmp, Map<Number640, Data> stored) {
        Assert.assertEquals(tmp.size(), stored.size());
        Iterator<Number640> iterator1 = tmp.keySet().iterator();
        while (iterator1.hasNext()) {
            Number640 key1 = iterator1.next();
            Assert.assertEquals(true, stored.containsKey(key1));
            Data data1 = tmp.get(key1);
            Data data2 = stored.get(key1);
            Assert.assertEquals(data1, data2);
        }
    }

    @Test
    public void testStorePutRemoveGet() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // remove
            RemoveBuilder removeBuilder = new RemoveBuilder(recv1, new Number160(33));
            removeBuilder.domainKey(Number160.createHash("test"));
            removeBuilder.contentKeys(tmp.keySet());
            removeBuilder.returnResults();
            removeBuilder.versionKey(Number160.ZERO);
            fr = smmSender.remove(recv1.peerAddress(), removeBuilder, cc);
            fr.awaitUninterruptibly();
            Message m = fr.responseMessage();
            Assert.assertEquals(true, fr.isSuccess());

            // check for returned results
            Map<Number640, Data> stored = m.dataMap(0).dataMap();
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), Number160.ZERO, tmp);
            compare(dataMap.dataMap(), stored);

            // get
            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            getBuilder.contentKeys(tmp.keySet());
            getBuilder.versionKey(Number160.ZERO);

            fr = smmSender.get(recv1.peerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            m = fr.responseMessage();
            DataMap stored2 = m.dataMap(0);
            Assert.assertEquals(0, stored2.size());
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testBigStorePut() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[100];
            byte[] me2 = new byte[10000];
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), Number160.ZERO, tmp);
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            KeyMapByte keys = fr.responseMessage().keyMapByte(0);
            Utils.isSameSets(keys.keysMap().keySet(), dataMap.dataMap().keySet());

        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testConcurrentStoreAddGet() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            final StorageRPC smmSender = sender.storeRPC();
            List<FutureResponse> res = new ArrayList<FutureResponse>();

            for (int i = 0; i < 40; i++) {
                System.err.println("round " + i);

                FutureChannelCreator fcc = recv1.peer().connectionBean().reservation().create(0, 10);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.channelCreator();

                // final ChannelCreator
                // cc1=sender.getConnectionBean().getReservation().reserve(50);
                for (int j = 0; j < 10; j++) {
                    FutureResponse fr = store(sender, recv1, smmSender, cc);
                    res.add(fr);
                }
                // cc1.release();
                for (FutureResponse fr : res) {
                    fr.awaitUninterruptibly();
                    if (!fr.isSuccess()) {
                        System.err.println("failed: " + fr.failedReason());
                    }
                    Assert.assertEquals(true, fr.isSuccess());
                }
                res.clear();
                cc.shutdown().awaitListenersUninterruptibly();
            }
            System.err.println("done.");
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    
    private FutureResponse store(PeerDHT sender, final PeerDHT recv1, StorageRPC smmSender, ChannelCreator cc)
            throws Exception {
        Map<Number160, Data> tmp = new HashMap<Number160, Data>();
        byte[] me1 = new byte[] { 1, 2, 3 };
        byte[] me2 = new byte[] { 2, 3, 4 };
        tmp.put(new Number160(77), new Data(me1));
        tmp.put(new Number160(88), new Data(me2));

        AddBuilder addBuilder = new AddBuilder(recv1, new Number160(33));
        addBuilder.domainKey(Number160.createHash("test"));
        addBuilder.dataSet(tmp.values());
        addBuilder.versionKey(Number160.ZERO);

        FutureResponse fr = smmSender.add(recv1.peerAddress(), addBuilder, cc);
        return fr;
    }

    @Test
    public void testBloomFilter() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            byte[] me3 = new byte[] { 5, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));
            tmp.put(new Number160(99), new Data(me3));

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();

            SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(100, 1);
            sbf.add(new Number160(77));

            // get
            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            getBuilder.keyBloomFilter(sbf);
            getBuilder.versionKey(Number160.ZERO);

            fr = smmSender.get(recv1.peerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Message m = fr.responseMessage();
            Map<Number640, Data> stored = m.dataMap(0).dataMap();
            Assert.assertEquals(1, stored.size());
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testBloomFilterDigest() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).enableMaintenance(false).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).enableMaintenance(false).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            byte[] me3 = new byte[] { 5, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));
            tmp.put(new Number160(99), new Data(me3));

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();

            SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(100, 2);
            sbf.add(new Number160(77));
            sbf.add(new Number160(99));

            // digest
            DigestBuilder getBuilder = new DigestBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            getBuilder.keyBloomFilter(sbf);
            getBuilder.versionKey(Number160.ZERO);

            fr = smmSender.digest(recv1.peerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Message m = fr.responseMessage();
            Assert.assertEquals(2, m.keyMap640Keys(0).size());

        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    // TODO test is not working
    public void testBigStore2() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            PeerBuilder pm1 = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConfiguration css = PeerBuilder.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = new PeerBuilderDHT(pm1.start()).storage(storeSender).start();

            PeerBuilder pm2 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = new PeerBuilderDHT(pm2.start()).storage(storeRecv).start();

            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.dataMapContent(tmp);
            putBuilder.idleTCPSeconds(Integer.MAX_VALUE);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Data data = recv1.storageLayer()
                    .get(new Number640(new Number160(33), Number160.createHash("test"), new Number160(77), Number160.ZERO));
            Assert.assertEquals(true, data != null);

        }
        finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testBigStoreGet() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            PeerBuilder pm1 = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConfiguration css = PeerBuilder.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = new PeerBuilderDHT(pm1.start()).storage(storeSender).start();

            PeerBuilder pm2 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = new PeerBuilderDHT(pm2.start()).storage(storeRecv).start();

            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.dataMapContent(tmp);
            putBuilder.idleTCPSeconds(Integer.MAX_VALUE);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);

            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            //

            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            getBuilder.idleTCPSeconds(Integer.MAX_VALUE);
            getBuilder.versionKey(Number160.ZERO);

            fr = smmSender.get(recv1.peerAddress(), getBuilder, cc);

            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            Number640 key = new Number640(new Number160(33), Number160.createHash("test"), new Number160(77), Number160.ZERO);
            Assert.assertEquals(50 * 1024 * 1024, fr.responseMessage().dataMap(0).dataMap().get(key)
                    .length());
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testBigStoreCancel() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderDHT(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).storage(storeSender).start();
            recv1 = new PeerBuilderDHT(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).storage(storeRecv).start();
            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.dataMapContent(tmp);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);

            Thread.sleep(5);
            fr.cancel();
            Assert.assertEquals(false, fr.isSuccess());
            System.err.println("good!");
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testBigStoreGetCancel() throws Exception {
        StorageMemory storeSender = new StorageMemory();
        StorageMemory storeRecv = new StorageMemory();
        PeerDHT sender = null;
        PeerDHT recv1 = null;
        ChannelCreator cc = null;
        try {

            PeerBuilder pm1 = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConfiguration css = PeerBuilder.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = new PeerBuilderDHT(pm1.start()).storage(storeSender).start();

            PeerBuilder pm2 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = new PeerBuilderDHT(pm2.start()).storage(storeRecv).start();

            StorageRPC smmSender = sender.storeRPC();
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.domainKey(Number160.createHash("test"));
            putBuilder.dataMapContent(tmp);
            putBuilder.idleTCPSeconds(Integer.MAX_VALUE);
            putBuilder.versionKey(Number160.ZERO);

            FutureResponse fr = smmSender.put(recv1.peerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println("XX:" + fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            //
            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.domainKey(Number160.createHash("test"));
            fr = smmSender.get(recv1.peerAddress(), getBuilder, cc);
            Thread.sleep(5);
            fr.cancel();
            System.err.println("XX:" + fr.failedReason());
            Assert.assertEquals(false, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }
    
    @Test
	public void testData480() throws Exception {
    	final Random rnd = new Random(42L);
    	PeerDHT master = null;
    	PeerDHT slave = null;
		ChannelCreator cc = null;
		try {

			master = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4001).start()).start();
			slave = new PeerBuilderDHT(new PeerBuilder(new Number160(rnd)).ports(4002).start()).start();
			
			Map<Number640,Data> tmp = new HashMap<Number640,Data>();
			for(int i=0;i<5;i++) {
				byte[] me = new byte[480];
				Arrays.fill(me, (byte)(i-6));
				Data test = new Data(me);
				tmp.put(new Number640(rnd), test);
			}
			
			PutBuilder pb = master.put(new Number160("0x51")).dataMap(tmp);
			
			FutureChannelCreator fcc = master.peer().connectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
			
			FutureResponse fr = master.storeRPC().put(slave.peerAddress(), pb, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			
			GetBuilder gb = master.get(new Number160("0x51")).domainKey(Number160.ZERO);

			fr = master.storeRPC().get(slave.peerAddress(), gb, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());

			System.err.println("done");
			
			

		} finally {
			if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (master != null) {
            	master.shutdown().await();
            }
            if (slave != null) {
            	slave.shutdown().await();
            }
		}
	}
}
