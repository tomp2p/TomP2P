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

import net.tomp2p.Utils2;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.NeighborRPC.SearchValues;

import org.junit.Assert;
import org.junit.Test;

public class TestNeighbor {
    public static final int PORT_TCP = 5001;

    public static final int PORT_UDP = 5002;

    @Test
    public void testNeigbhorUdp() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start();
            PeerAddress[] pa = Utils2.createDummyAddresses(300, PORT_TCP, PORT_UDP);
            for (int i = 0; i < pa.length; i++) {
                sender.peerBean().peerMap().peerFound(pa[i], null, null);
            }
            new NeighborRPC(sender.peerBean(), sender.connectionBean());
            recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start();
            NeighborRPC neighbors2 = new NeighborRPC(recv1.peerBean(), recv1.connectionBean());
           
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.channelCreator();

            SearchValues v = new SearchValues(new Number160("0x1"), null);
            FutureResponse fr = neighbors2.closeNeighbors(sender.peerAddress(), v, 
                    Type.REQUEST_2, cc, new DefaultConnectionConfiguration());

            fr.awaitUninterruptibly();
            // Thread.sleep(10000000);
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.responseMessage().neighborsSet(0);
            // we are able to fit 33 neighbors into 1000 bytes
            Assert.assertEquals(33, pas.size());
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
    public void testNeigbhorTCP() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start();
            PeerAddress[] pa = Utils2.createDummyAddresses(300, PORT_TCP, PORT_UDP);
            for (int i = 0; i < pa.length; i++) {
                sender.peerBean().peerMap().peerFound(pa[i], null, null);
            }
            new NeighborRPC(sender.peerBean(), sender.connectionBean());
            recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start();
            NeighborRPC neighbors2 = new NeighborRPC(recv1.peerBean(), recv1.connectionBean());
            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(0, 1);

            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.channelCreator();

            SearchValues v = new SearchValues(new Number160("0x1"), null);
            DefaultConnectionConfiguration d = new DefaultConnectionConfiguration();
            d.forceTCP();
            FutureResponse fr = neighbors2.closeNeighbors(sender.peerAddress(), v, 
                    Type.REQUEST_2, cc, d);

            fr.awaitUninterruptibly();
            System.err.println(fr.failedReason());
            // Thread.sleep(10000000);
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.responseMessage().neighborsSet(0);
            // we are able to fit 33 neighbors into 1000 bytes
            Assert.assertEquals(33, pas.size());
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
    public void testNeigbhor2() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        try {
            sender = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start();
            new NeighborRPC(sender.peerBean(), sender.connectionBean());
            NeighborRPC neighbors2 = new NeighborRPC(recv1.peerBean(), recv1.connectionBean());

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.channelCreator();

            SearchValues v = new SearchValues(new Number160("0x30"), null);
            FutureResponse fr = neighbors2.closeNeighbors(sender.peerAddress(), v, 
                    Type.REQUEST_2, cc, new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            NeighborSet pas = fr.responseMessage().neighborsSet(0);

            // I see no one, not even myself. My peer was added in the overflow map
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
            sender = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088).start();
            new NeighborRPC(sender.peerBean(), sender.connectionBean());
            NeighborRPC neighbors2 = new NeighborRPC(recv1.peerBean(), recv1.connectionBean());

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.channelCreator();

            try {
                SearchValues v = new SearchValues(new Number160("0x30"), null);
                neighbors2.closeNeighbors(sender.peerAddress(), v,
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

    
}
