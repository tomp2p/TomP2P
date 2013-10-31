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

import java.io.IOException;
import java.net.Inet4Address;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.DefaultConnectionConfiguration;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This is not an automated test and needs manual interaction. Thus by default these tests are disabled.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestRealPing {

    private static final String IP = "127.0.0.1";
    private static final int PORT = 5000;
    private static final int WAIT = 1000000;

    /**
     * Test regular ping.
     * 
     * @throws InterruptedException .
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void sendPingTCP() throws IOException, InterruptedException {
        Peer sender = null;
        ChannelCreator cc = null;
        try {
            PeerAddress pa = new PeerAddress(Number160.ZERO, Inet4Address.getByName(IP), PORT, PORT);
            sender = new PeerMaker(new Number160("0x9876")).ports(PORT).setEnableMaintenance(false)
                    .makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean());
            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCP(pa, cc, new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Thread.sleep(WAIT);
        } finally {
            if (cc != null) {
                cc.shutdown().await();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
        }
    }

    /**
     * Test discover ping.
     * 
     * @throws InterruptedException .
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void sendPingTCPDiscover() throws IOException, InterruptedException {
        Peer sender = null;
        ChannelCreator cc = null;
        try {
            PeerAddress pa = new PeerAddress(Number160.ZERO, Inet4Address.getByName(IP), PORT, PORT);
            sender = new PeerMaker(new Number160("0x9876")).ports(PORT).setEnableMaintenance(false)
                    .makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean());
            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCPDiscover(pa, cc, new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Thread.sleep(WAIT);
        } finally {
            if (cc != null) {
                cc.shutdown().await();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
        }
    }

    /**
     * Test probe ping.
     * 
     * @throws InterruptedException .
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void sendPingTCPProbe() throws IOException, InterruptedException {
        Peer sender = null;
        ChannelCreator cc = null;
        try {
            PeerAddress pa = new PeerAddress(Number160.ZERO, Inet4Address.getByName(IP), PORT, PORT);
            sender = new PeerMaker(new Number160("0x9876")).ports(PORT).setEnableMaintenance(false)
                    .makeAndListen();
            PingRPC handshake = new PingRPC(sender.getPeerBean(), sender.getConnectionBean());
            FutureChannelCreator fcc = sender.getConnectionBean().reservation().create(0, 1);
            fcc.awaitUninterruptibly();
            cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCPProbe(pa, cc, new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            Thread.sleep(WAIT);
        } finally {
            if (cc != null) {
                cc.shutdown().await();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
        }
    }

    /**
     * The receiver.
     * 
     * @throws InterruptedException .
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void receivePing() throws IOException, InterruptedException {
        Peer recv = null;
        try {
            recv = new PeerMaker(new Number160("0x1234")).ports(PORT).setEnableMaintenance(false)
                    .makeAndListen();
            /**
             * HandshakeRPC with custom debug output.
             * 
             * @author Thomas Bocek
             * 
             */
            final class MyHandshakeRPC extends PingRPC {
                /**
                 * Constructor that registers the ping command. Creating a new class is enough to register it.
                 * 
                 * @param peerBean
                 *            The bean of this peer
                 * @param connectionBean
                 *            The connection bean.
                 */
                public MyHandshakeRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
                    super(peerBean, connectionBean);
                }

                @Override
                public Message handleResponse(final Message message, final boolean sign) throws Exception {
                    System.err.println("handle message " + message);
                    return super.handleResponse(message, sign);
                }

            }
            new MyHandshakeRPC(recv.getPeerBean(), recv.getConnectionBean());
            Thread.sleep(WAIT);
        } finally {
            if (recv != null) {
                recv.shutdown().await();
            }
        }
    }
}
