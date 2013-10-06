package net.tomp2p.rpc;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.connection2.ChannelClientConfiguration;
import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ChannelServerConficuration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureSuccessEvaluatorCommunication;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Keys;
import net.tomp2p.message.Message2;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.builder.AddBuilder;
import net.tomp2p.p2p.builder.GetBuilder;
import net.tomp2p.p2p.builder.PutBuilder;
import net.tomp2p.p2p.builder.RemoveBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener.FailReason;
import net.tomp2p.replication.Replication;
import net.tomp2p.replication.ResponsibilityListener;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.HashData;
//import net.tomp2p.storage.StorageDisk;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.storage.StorageMemory;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStorage {
    final private static Number160 domainKey = new Number160(20);

    private static String DIR1;

    private static String DIR2;

    @Before
    public void before() throws IOException {
        DIR1 = Utils2.createTempDirectory().getCanonicalPath();
        DIR2 = Utils2.createTempDirectory().getCanonicalPath();
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
    public void testAdd() throws Exception {
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            Collection<Data> dataSet = new HashSet<Data>();
            dataSet.add(new Data(1));
            AddBuilder addBuilder = new AddBuilder(recv1, new Number160(33));
            addBuilder.setDomainKey(Number160.createHash("test"));
            addBuilder.setDataSet(dataSet);
            // addBuilder.setList();
            // addBuilder.random(new Random(42));
            FutureResponse fr = smmSender.add(recv1.getPeerAddress(), addBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            // add a the same data twice
            fr = smmSender.add(recv1.getPeerAddress(), addBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());

            Number320 key = new Number320(new Number160(33), Number160.createHash("test"));
            // Set<Number480> tofetch = new HashSet<Number480>();
            SortedMap<Number480, Data> c = storeRecv.get(key.getLocationKey(), key.getDomainKey(),
                    Number160.ZERO, Number160.MAX_VALUE);
            Assert.assertEquals(1, c.size());
            for (Data data : c.values()) {
                Assert.assertEquals((Integer) 1, (Integer) data.object());
            }

            // now add again, but as a list

            addBuilder.setList();
            addBuilder.random(new Random(42));

            fr = smmSender.add(recv1.getPeerAddress(), addBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());

            key = new Number320(new Number160(33), Number160.createHash("test"));
            // Set<Number480> tofetch = new HashSet<Number480>();
            c = storeRecv.get(key.getLocationKey(), key.getDomainKey(), Number160.ZERO, Number160.MAX_VALUE);
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            Data test = new Data(me1);
            Data test2 = new Data(me2);
            tmp.put(new Number160(77), test);
            tmp.put(new Number160(88), test2);
            System.err.println(recv1.getPeerAddress());

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());

            Number320 key = new Number320(new Number160(33), Number160.createHash("test"));
            // Set<Number480> tofetch = new HashSet<Number480>();
            Data c = storeRecv.get(key.getLocationKey(), key.getDomainKey(), new Number160(77));
            Assert.assertEquals(test, c);

            //
            tmp.clear();
            me1 = new byte[] { 5, 6, 7 };
            me2 = new byte[] { 8, 9, 1, 5 };
            test = new Data(me1);
            test2 = new Data(me2);
            tmp.put(new Number160(77), test);
            tmp.put(new Number160(88), test2);
            putBuilder.setDataMapContent(tmp);
            fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            Map<Number480, Data> result2 = storeRecv.subMap(key.getLocationKey(), key.getDomainKey(),
                    Number160.ZERO, Number160.MAX_VALUE);
            Assert.assertEquals(result2.size(), 2);
            Number480 search = new Number480(key, new Number160(88));
            c = result2.get(search);
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            Data test = new Data(me1);
            Data test2 = new Data(me2);
            tmp.put(new Number160(77), test);
            tmp.put(new Number160(88), test2);

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Data c = storeRecv.get(new Number160(33), Number160.createHash("test"), new Number160(77));

            Assert.assertEquals(c, test);
            //
            tmp.clear();
            byte[] me3 = new byte[] { 5, 6, 7 };
            byte[] me4 = new byte[] { 8, 9, 1, 5 };
            tmp.put(new Number160(77), new Data(me3));
            tmp.put(new Number160(88), new Data(me4));

            putBuilder.setPutIfAbsent();

            fr = smmSender.putIfAbsent(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // we cannot put anything there, since there already is
            Assert.assertEquals(true, fr.isSuccess());
            Collection<Number480> putKeys = fr.getResponse().getKeys(0).keys();
            Assert.assertEquals(0, putKeys.size());
            c = storeRecv.get(new Number160(33), Number160.createHash("test"), new Number160(88));
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), tmp);
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // get

            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.setDomainKey(Number160.createHash("test"));
            getBuilder.setContentKeys(tmp.keySet());

            fr = smmSender.get(recv1.getPeerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            System.err.println(fr.getFailedReason());
            Message2 m = fr.getResponse();
            Map<Number480, Data> stored = m.getDataMap(0).dataMap();
            compare(dataMap.convertToMap480(), stored);
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), tmp);
            putBuilder.setDataMapContent(tmp);
            putBuilder.setForceUDP();

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();

            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.setDomainKey(Number160.createHash("test"));
            getBuilder.setContentKeys(tmp.keySet());
            getBuilder.setForceUDP();

            // get
            fr = smmSender.get(recv1.getPeerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Message2 m = fr.getResponse();
            Map<Number480, Data> stored = m.getDataMap(0).dataMap();
            compare(dataMap.convertToMap480(), stored);
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

    private void compare(Map<Number480, Data> tmp, Map<Number480, Data> stored) {
        Assert.assertEquals(tmp.size(), stored.size());
        Iterator<Number480> iterator1 = tmp.keySet().iterator();
        while (iterator1.hasNext()) {
            Number480 key1 = iterator1.next();
            Assert.assertEquals(true, stored.containsKey(key1));
            Data data1 = tmp.get(key1);
            Data data2 = stored.get(key1);
            Assert.assertEquals(data1, data2);
        }
    }

    @Test
    public void testStorePutRemoveGet() throws Exception {
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), tmp);
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // remove
            RemoveBuilder removeBuilder = new RemoveBuilder(recv1, new Number160(33));
            removeBuilder.setDomainKey(Number160.createHash("test"));
            removeBuilder.setContentKeys(tmp.keySet());
            removeBuilder.setReturnResults();
            fr = smmSender.remove(recv1.getPeerAddress(), removeBuilder, cc);
            fr.awaitUninterruptibly();
            Message2 m = fr.getResponse();
            Assert.assertEquals(true, fr.isSuccess());

            // check for returned results
            Map<Number480, Data> stored = m.getDataMap(0).dataMap();
            compare(dataMap.convertToMap480(), stored);

            // get
            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.setDomainKey(Number160.createHash("test"));
            getBuilder.setContentKeys(tmp.keySet());

            fr = smmSender.get(recv1.getPeerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            m = fr.getResponse();
            DataMap stored2 = m.getDataMap(0);
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[100];
            byte[] me2 = new byte[10000];
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(recv1, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            DataMap dataMap = new DataMap(new Number160(33), Number160.createHash("test"), tmp);
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Keys keys = fr.getResponse().getKeys(0);
            Utils.isSameSets(keys.keys(), dataMap.convertToMap480().keySet());

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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            final StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            List<FutureResponse> res = new ArrayList<FutureResponse>();

            for (int i = 0; i < 40; i++) {
                System.err.println("round " + i);

                FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 10);
                fcc.awaitUninterruptibly();
                ChannelCreator cc = fcc.getChannelCreator();

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
                        System.err.println("failed: " + fr.getFailedReason());
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

    /**
     * Test the responsibility and the notifications.
     * 
     * @throws Exception .
     */
    @Test
    public void testResponsibility() throws Exception {
        // Random rnd=new Random(42L);
        Peer master = null;
        Peer slave = null;
        ChannelCreator cc = null;
        try {
            master = new PeerMaker(new Number160("0xee")).makeAndListen();
            StorageGeneric s1 = new StorageMemory();
            master.getPeerBean().storage(s1);
            final AtomicInteger test1 = new AtomicInteger(0);
            final AtomicInteger test2 = new AtomicInteger(0);
            final int replicatioFactor = 5;
            Replication replication = new Replication(s1, master.getPeerAddress(), master.getPeerBean()
                    .peerMap(), replicatioFactor);
            replication.addResponsibilityListener(new ResponsibilityListener() {
                @Override
                public void otherResponsible(final Number160 locationKey, final PeerAddress other) {
                    System.err.println("Other peer (" + other + ")is responsible for " + locationKey);
                    test1.incrementAndGet();
                }

                @Override
                public void meResponsible(final Number160 locationKey) {
                    System.err.println("I'm responsible for " + locationKey + " / ");
                    test2.incrementAndGet();
                }
            });
            master.getPeerBean().replicationStorage(replication);
            Number160 location = new Number160("0xff");
            Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
            dataMap.put(Number160.ZERO, new Data("string"));

            FutureChannelCreator fcc = master.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(master, location);
            putBuilder.setDomainKey(location);
            putBuilder.setDataMapContent(dataMap);

            FutureResponse fr = master.getStoreRPC().put(master.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            // s1.put(location, Number160.ZERO, null, dataMap, false, false);
            final int slavePort = 7701;
            slave = new PeerMaker(new Number160("0xfe")).ports(slavePort).makeAndListen();
            master.getPeerBean().peerMap().peerFound(slave.getPeerAddress(), null);
            master.getPeerBean().peerMap().peerFailed(slave.getPeerAddress(), FailReason.Shutdown);
            Assert.assertEquals(1, test1.get());
            Assert.assertEquals(2, test2.get());
        } catch (Throwable t) {
            t.printStackTrace();
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

    /**
     * Test the responsibility and the notifications.
     * 
     * @throws Exception .
     */
    @Test
    public void testResponsibility2() throws Exception {
        final Random rnd = new Random(42L);
        final int port = 8000;
        Peer master = null;
        Peer slave1 = null;
        Peer slave2 = null;
        ChannelCreator cc = null;
        try {
            Number160 loc = new Number160(rnd);
            Map<Number160, Data> contentMap = new HashMap<Number160, Data>();
            contentMap.put(Number160.ZERO, new Data("string"));
            final AtomicInteger test1 = new AtomicInteger(0);
            final AtomicInteger test2 = new AtomicInteger(0);
            master = new PeerMaker(new Number160(rnd)).ports(port).makeAndListen();
            System.err.println("master is " + master.getPeerAddress());

            StorageGeneric s1 = new StorageMemory();
            master.getPeerBean().storage(s1);
            final int replicatioFactor = 5;
            Replication replication = new Replication(s1, master.getPeerAddress(), master.getPeerBean()
                    .peerMap(), replicatioFactor);

            replication.addResponsibilityListener(new ResponsibilityListener() {
                @Override
                public void otherResponsible(final Number160 locationKey, final PeerAddress other) {
                    System.err.println("Other peer (" + other + ")is responsible for " + locationKey);
                    test1.incrementAndGet();
                }

                @Override
                public void meResponsible(final Number160 locationKey) {
                    System.err.println("I'm responsible for " + locationKey);
                    test2.incrementAndGet();
                }
            });

            master.getPeerBean().replicationStorage(replication);

            FutureChannelCreator fcc = master.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(master, loc);
            putBuilder.setDomainKey(domainKey);
            putBuilder.setDataMapContent(contentMap);

            master.getStoreRPC().put(master.getPeerAddress(), putBuilder, cc).awaitUninterruptibly();
            slave1 = new PeerMaker(new Number160(rnd)).ports(port + 1).makeAndListen();
            slave2 = new PeerMaker(new Number160(rnd)).ports(port + 2).makeAndListen();
            System.err.println("slave1 is " + slave1.getPeerAddress());
            System.err.println("slave2 is " + slave2.getPeerAddress());
            // master peer learns about the slave peers
            master.getPeerBean().peerMap().peerFound(slave1.getPeerAddress(), null);
            master.getPeerBean().peerMap().peerFound(slave2.getPeerAddress(), null);

            System.err.println("both peers online");
            PeerAddress slaveAddress1 = slave1.getPeerAddress();
            slave1.shutdown().await();
            master.getPeerBean().peerMap().peerFailed(slaveAddress1, FailReason.Shutdown);

            Assert.assertEquals(1, test1.get());
            Assert.assertEquals(1, test2.get());

            PeerAddress slaveAddress2 = slave2.getPeerAddress();
            slave2.shutdown().await();
            master.getPeerBean().peerMap().peerFailed(slaveAddress2, FailReason.Shutdown);

            Assert.assertEquals(1, test1.get());
            Assert.assertEquals(2, test2.get());

        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (master != null) {
                master.shutdown().await();
            }
        }

    }

    private FutureResponse store(Peer sender, final Peer recv1, StorageRPC smmSender, ChannelCreator cc)
            throws Exception {
        Map<Number160, Data> tmp = new HashMap<Number160, Data>();
        byte[] me1 = new byte[] { 1, 2, 3 };
        byte[] me2 = new byte[] { 2, 3, 4 };
        tmp.put(new Number160(77), new Data(me1));
        tmp.put(new Number160(88), new Data(me2));

        AddBuilder addBuilder = new AddBuilder(recv1, new Number160(33));
        addBuilder.setDomainKey(Number160.createHash("test"));
        addBuilder.setDataSet(tmp.values());

        FutureResponse fr = smmSender.add(recv1.getPeerAddress(), addBuilder, cc);
        return fr;
    }

    @Test
    public void testBloomFilter() throws Exception {
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            byte[] me3 = new byte[] { 5, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));
            tmp.put(new Number160(99), new Data(me3));

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();

            SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(100, 1);
            sbf.add(new Number160(77));

            // get
            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.setDomainKey(Number160.createHash("test"));
            getBuilder.setKeyBloomFilter(sbf);

            fr = smmSender.get(recv1.getPeerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Message2 m = fr.getResponse();
            Map<Number480, Data> stored = m.getDataMap(0).dataMap();
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[] { 1, 2, 3 };
            byte[] me2 = new byte[] { 2, 3, 4 };
            byte[] me3 = new byte[] { 5, 3, 4 };
            tmp.put(new Number160(77), new Data(me1));
            tmp.put(new Number160(88), new Data(me2));
            tmp.put(new Number160(99), new Data(me3));

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();

            SimpleBloomFilter<Number160> sbf = new SimpleBloomFilter<Number160>(100, 2);
            sbf.add(new Number160(77));
            sbf.add(new Number160(99));

            // get
            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.setDomainKey(Number160.createHash("test"));
            getBuilder.setKeyBloomFilter(sbf);
            getBuilder.setDigest();

            fr = smmSender.get(recv1.getPeerAddress(), getBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Message2 m = fr.getResponse();
            Assert.assertEquals(2, m.getKeysMap(0).size());

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
    public void testBigStore2() throws Exception {
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            PeerMaker pm1 = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConficuration css = pm1.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = pm1.makeAndListen();

            PeerMaker pm2 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = pm2.makeAndListen();

            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);
            putBuilder.idleTCPSeconds(Integer.MAX_VALUE);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Data data = recv1.getPeerBean().storage()
                    .get(new Number160(33), Number160.createHash("test"), new Number160(77));
            Assert.assertEquals(true, data != null);

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
    public void testBigStoreGet() throws Exception {
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            PeerMaker pm1 = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConficuration css = pm1.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = pm1.makeAndListen();

            PeerMaker pm2 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = pm2.makeAndListen();

            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);
            putBuilder.idleTCPSeconds(Integer.MAX_VALUE);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);

            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            //

            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.setDomainKey(Number160.createHash("test"));
            getBuilder.idleTCPSeconds(Integer.MAX_VALUE);

            fr = smmSender.get(recv1.getPeerAddress(), getBuilder, cc);

            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            Number480 key = new Number480(new Number160(33), Number160.createHash("test"), new Number160(77));
            Assert.assertEquals(50 * 1024 * 1024, fr.getResponse().getDataMap(0).dataMap().get(key)
                    .bufferLength());
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);

            Timings.sleep(5);
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
        StorageGeneric storeSender = new StorageMemory();
        StorageGeneric storeRecv = new StorageMemory();
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {

            PeerMaker pm1 = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424);
            ChannelServerConficuration css = pm1.createDefaultChannelServerConfiguration();
            css.idleTCPSeconds(Integer.MAX_VALUE);
            pm1.channelServerConfiguration(css);
            sender = pm1.makeAndListen();

            PeerMaker pm2 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088);
            pm2.channelServerConfiguration(css);
            recv1 = pm2.makeAndListen();

            sender.getPeerBean().storage(storeSender);
            StorageRPC smmSender = new StorageRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1.getPeerBean().storage(storeRecv);
            new StorageRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            Map<Number160, Data> tmp = new HashMap<Number160, Data>();
            byte[] me1 = new byte[50 * 1024 * 1024];
            tmp.put(new Number160(77), new Data(me1));

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();

            PutBuilder putBuilder = new PutBuilder(sender, new Number160(33));
            putBuilder.setDomainKey(Number160.createHash("test"));
            putBuilder.setDataMapContent(tmp);
            putBuilder.idleTCPSeconds(Integer.MAX_VALUE);

            FutureResponse fr = smmSender.put(recv1.getPeerAddress(), putBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println("XX:" + fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            //
            GetBuilder getBuilder = new GetBuilder(recv1, new Number160(33));
            getBuilder.setDomainKey(Number160.createHash("test"));
            fr = smmSender.get(recv1.getPeerAddress(), getBuilder, cc);
            Timings.sleep(5);
            fr.cancel();
            System.err.println("XX:" + fr.getFailedReason());
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
}
