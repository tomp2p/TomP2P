package net.tomp2p.rpc;

import java.util.Random;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.builder.AddTrackerBuilder;
import net.tomp2p.p2p.builder.GetTrackerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestTracker {
    final static Random rnd = new Random(0);

    @Test
    public void testTrackerPut() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found
            SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(100, 10);

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.setDomainKey(dom);
            addTrackerBuilder.setBloomFilter(bloomFilter);

            FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            bloomFilter = new SimpleBloomFilter<Number160>(100, 10);

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);
            getTrackerBuilder.setKnownPeers(bloomFilter);

            fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            PeerAddress peerAddress = fr.getResponse().getTrackerData(0).getPeerAddresses().keySet()
                    .iterator().next();
            Assert.assertEquals(sender.getPeerAddress(), peerAddress);

        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail();
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
    public void testTrackerPutNoBloomFilter() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();

            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();

            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.setDomainKey(dom);

            FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);

            fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            PeerAddress peerAddress = fr.getResponse().getTrackerData(0).getPeerAddresses().keySet()
                    .iterator().next();
            Assert.assertEquals(sender.getPeerAddress(), peerAddress);
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
    public void testTrackerPutAttachment() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(1, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.setDomainKey(dom);
            addTrackerBuilder.setAttachement(new Data("data"));

            FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);
            getTrackerBuilder.setExpectAttachement(true);

            fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println("ERR:" + fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            PeerAddress peerAddress = fr.getResponse().getTrackerData(0).getPeerAddresses().keySet()
                    .iterator().next();
            Assert.assertEquals(sender.getPeerAddress(), peerAddress);
            Data tmp = fr.getResponse().getTrackerData(0).getPeerAddresses().values().iterator().next();
            Assert.assertEquals(tmp.object(), "data");
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
    public void testTrackerBloomFilter() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerMaker(new Number160("0x9876")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x1234")).p2pId(55).ports(8088).makeAndListen();
            Number160 loc = new Number160(rnd);
            Number160 dom = new Number160(rnd);
            // make a good guess based on the config and the maxium tracker that
            // can be found
            SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(100, 10);

            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();

            AddTrackerBuilder addTrackerBuilder = new AddTrackerBuilder(sender, loc);
            addTrackerBuilder.setDomainKey(dom);
            addTrackerBuilder.setAttachement(new Data("data"));
            addTrackerBuilder.setBloomFilter(bloomFilter);

            FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(),
                    addTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            bloomFilter.add(sender.getPeerID());

            GetTrackerBuilder getTrackerBuilder = new GetTrackerBuilder(sender, loc);
            getTrackerBuilder.setExpectAttachement(true);
            getTrackerBuilder.setKnownPeers(bloomFilter);

            fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), getTrackerBuilder, cc);
            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            Assert.assertEquals(true, fr.isSuccess());
            Assert.assertEquals(0, fr.getResponse().getTrackerData(0).size());
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail();
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
