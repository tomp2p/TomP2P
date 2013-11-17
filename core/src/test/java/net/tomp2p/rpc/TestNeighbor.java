/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.rpc;

import java.net.InetAddress;
import java.net.UnknownHostException;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.NeighborRPC.SearchValues;
import net.tomp2p.storage.Data;

import org.junit.Assert;
import org.junit.Test;

public class TestNeighbor {
    public static final int PORT_TCP = 5001;

    public static final int PORT_UDP = 5002;

    @Test
    public void testNeigbhor() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            PeerAddress[] pa = createDummyAddress(300);
            for (int i = 0; i < pa.length; i++) {
                sender.getPeerBean().peerMap().peerFound(pa[i], null);
            }
            new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());
           
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();

            SearchValues v = new SearchValues(new Number160("0x1"), null);
            FutureResponse fr = neighbors2.closeNeighbors(sender.getPeerAddress(), v, 
                    Type.REQUEST_2, cc, new DefaultConnectionConfiguration());

            fr.awaitUninterruptibly();
            // Thread.sleep(10000000);
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.getResponse().getNeighborsSet(0);
            // we are able to fit 40 neighbors into 1400 bytes
            Assert.assertEquals(33, pas.size());
            Assert.assertEquals(new Number160("0x1"), pas.neighbors().iterator().next().getPeerId());
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
    public void testNeigbhorTCP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            PeerAddress[] pa = createDummyAddress(300);
            for (int i = 0; i < pa.length; i++) {
                sender.getPeerBean().peerMap().peerFound(pa[i], null);
            }
            new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(0, 1);

            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();

            SearchValues v = new SearchValues(new Number160("0x1"), null);
            DefaultConnectionConfiguration d = new DefaultConnectionConfiguration();
            d.setForceTCP();
            FutureResponse fr = neighbors2.closeNeighbors(sender.getPeerAddress(), v, 
                    Type.REQUEST_2, cc, d);

            fr.awaitUninterruptibly();
            System.err.println(fr.getFailedReason());
            // Thread.sleep(10000000);
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.getResponse().getNeighborsSet(0);
            // we are able to fit 40 neighbors into 1400 bytes
            Assert.assertEquals(33, pas.size());
            Assert.assertEquals(new Number160("0x1"), pas.neighbors().iterator().next().getPeerId());
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
    public void testNeigbhorBloomfilter() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            PeerAddress[] pa = createDummyAddress(300);
            for (int i = 0; i < pa.length; i++) {
                sender.getPeerBean().peerMap().peerFound(pa[i], null);
            }
            new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());
            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);

            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();

            SimpleBloomFilter<Number160> bf = new SimpleBloomFilter<Number160>(20, 10);
            for (int i = 0; i < 10; i++) {
                Number640 key = new Number640(new Number160(0x1), Number160.ZERO, Number160.createHash(i), Number160.ZERO);
                sender.getPeerBean().storage().put(key, new Data("test"), null, false, false);
                bf.add(Number160.createHash(i));
            }

            SearchValues v = new SearchValues(new Number160("0x1"), null, bf);
            FutureResponse fr = neighbors2.closeNeighbors(sender.getPeerAddress(), v, 
                    Type.REQUEST_2, cc, new DefaultConnectionConfiguration());

            fr.awaitUninterruptibly();
            // Thread.sleep(10000000);
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.getResponse().getNeighborsSet(0);
            // we are able to fit 40 neighbors into 1400 bytes
            Assert.assertEquals(33, pas.size());
            Assert.assertEquals(10, (int) fr.getResponse().getInteger(0));
            Assert.assertEquals(new Number160("0x1"), pas.neighbors().iterator().next().getPeerId());
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
    public void testNeigbhor2() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
            NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();

            SearchValues v = new SearchValues(new Number160("0x30"), null);
            FutureResponse fr = neighbors2.closeNeighbors(sender.getPeerAddress(), v, 
                    Type.REQUEST_2, cc, new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.getResponse().getNeighborsSet(0);

            // I see no one, not evenmyself. My peer was added in the overflow map
            Assert.assertEquals(0, pas.size());
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
    public void testNeigbhorFail() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerMaker(new Number160("0x50")).p2pId(55).ports(2424).makeAndListen();
            recv1 = new PeerMaker(new Number160("0x20")).p2pId(55).ports(8088).makeAndListen();
            new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
            NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());

            FutureChannelCreator fcc = recv1.getConnectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();

            try {
                SearchValues v = new SearchValues(new Number160("0x30"), null);
                neighbors2.closeNeighbors(sender.getPeerAddress(), v,
                        Type.EXCEPTION, cc, new DefaultConnectionConfiguration());
                Assert.fail("");
            } catch (IllegalArgumentException i) {
                cc.shutdown();
            }
        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    private PeerAddress[] createDummyAddress(int size) throws UnknownHostException {
        PeerAddress[] pa = new PeerAddress[size];
        for (int i = 0; i < size; i++) {
            pa[i] = createAddress(i + 1);
        }
        return pa;
    }

    private PeerAddress createAddress(int iid) throws UnknownHostException {
        Number160 id = new Number160(iid);
        InetAddress address = InetAddress.getByName("127.0.0.1");
        int portTCP = PORT_TCP;
        int portUDP = PORT_UDP;
        return new PeerAddress(id, address, portTCP, portUDP);
    }
}
