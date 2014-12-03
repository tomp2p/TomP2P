package net.tomp2p.tracker;

import java.util.Random;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestTrackerRPC {
    final static Random rnd = new Random(0);

    @Test
    public void testTrackerPut() throws Exception {
        PeerTracker sender = null;
        PeerTracker recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderTracker(new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start()).start();
            recv1 = new PeerBuilderTracker(new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start()).start();
            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found
            SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(100, 10);

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.domainKey(dom);
            addTrackerBuilder.setBloomFilter(bloomFilter);

            FutureResponse fr = sender.trackerRPC().addToTracker(recv1.peerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            //either you sleep for 1 sec and let the maintenance do its work, or do it manually
            recv1.peer().peerBean().notifyPeerFound(sender.peerAddress(), null, null, null);
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            bloomFilter = new SimpleBloomFilter<Number160>(100, 10);

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);
            getTrackerBuilder.domainKey(dom);
            getTrackerBuilder.knownPeers(bloomFilter);

            fr = sender.trackerRPC().getFromTracker(recv1.peerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            PeerAddress peerAddress = fr.responseMessage().trackerData(0).peerAddresses().keySet()
                    .iterator().next();
            Assert.assertEquals(sender.peerAddress(), peerAddress);

        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail();
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.peer().shutdown().await();
            }
            if (recv1 != null) {
                recv1.peer().shutdown().await();
            }
        }
    }

    @Test
    public void testTrackerPutNoBloomFilter() throws Exception {
    	PeerTracker sender = null;
    	PeerTracker recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderTracker(new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start()).start();

            recv1 = new PeerBuilderTracker(new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start()).start();

            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.domainKey(dom);

            FutureResponse fr = sender.trackerRPC().addToTracker(recv1.peerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            //either you sleep for 1 sec and let the maintenance do its work, or do it manually
            recv1.peer().peerBean().notifyPeerFound(sender.peerAddress(), null, null, null);
            Assert.assertEquals(true, fr.isSuccess());

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);
            getTrackerBuilder.domainKey(dom);

            fr = sender.trackerRPC().getFromTracker(recv1.peerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            PeerAddress peerAddress = fr.responseMessage().trackerData(0).peerAddresses().keySet()
                    .iterator().next();
            Assert.assertEquals(sender.peerAddress(), peerAddress);
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.peer().shutdown().await();
            }
            if (recv1 != null) {
                recv1.peer().shutdown().await();
            }
        }
    }

    @Test
    public void testTrackerPutAttachment() throws Exception {
    	PeerTracker sender = null;
    	PeerTracker recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderTracker(new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start()).start();
            recv1 = new PeerBuilderTracker(new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start()).start();
            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(1, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.domainKey(dom);
            addTrackerBuilder.attachement(new Data("data"));

            FutureResponse fr = sender.trackerRPC().addToTracker(recv1.peerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            //either you sleep for 1 sec and let the maintenance do its work, or do it manually
            recv1.peer().peerBean().notifyPeerFound(sender.peerAddress(), null, null, null);
            Assert.assertEquals(true, fr.isSuccess());

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);
            getTrackerBuilder.domainKey(dom);
            getTrackerBuilder.expectAttachement(true);

            fr = sender.trackerRPC().getFromTracker(recv1.peerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println("ERR:" + fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            PeerAddress peerAddress = fr.responseMessage().trackerData(0).peerAddresses().keySet()
                    .iterator().next();
            Assert.assertEquals(sender.peerAddress(), peerAddress);
            Data tmp = fr.responseMessage().trackerData(0).peerAddresses().values().iterator().next();
            Assert.assertEquals(tmp.object(), "data");
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.peer().shutdown().await();
            }
            if (recv1 != null) {
                recv1.peer().shutdown().await();
            }
        }
    }

    @Test
    public void testTrackerBloomFilter() throws Exception {
    	PeerTracker sender = null;
    	PeerTracker recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilderTracker(new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start()).start();
            recv1 = new PeerBuilderTracker(new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start()).start();
            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found
            SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(100, 10);

            FutureChannelCreator fcc = sender.peer().connectionBean().reservation().create(1, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.domainKey(dom);
            addTrackerBuilder.attachement(new Data("data"));
            addTrackerBuilder.setBloomFilter(bloomFilter);

            FutureResponse fr = sender.trackerRPC().addToTracker(recv1.peerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            //either you sleep for 1 sec and let the maintenance do its work, or do it manually
            recv1.peer().peerBean().notifyPeerFound(sender.peerAddress(), null, null, null);
            Assert.assertEquals(true, fr.isSuccess());
            bloomFilter.add(sender.peerAddress().peerId());

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);
            getTrackerBuilder.expectAttachement(true);
            getTrackerBuilder.knownPeers(bloomFilter);
            getTrackerBuilder.domainKey(dom);

            fr = sender.trackerRPC().getFromTracker(recv1.peerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            Assert.assertEquals(true, fr.isSuccess());
            Assert.assertEquals(0, fr.responseMessage().trackerData(0).size());
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail();
        } finally {
            if (cc != null) {
                cc.shutdown().awaitListenersUninterruptibly();
            }
            if (sender != null) {
                sender.peer().shutdown().await();
            }
            if (recv1 != null) {
                recv1.peer().shutdown().await();
            }
        }
    }
}
