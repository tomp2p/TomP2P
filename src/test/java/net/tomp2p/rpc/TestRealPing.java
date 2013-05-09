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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
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

    /**
     * Test regular ping.
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void sendPingTCP() throws IOException {
        Peer sender = null;
        try {
            PeerAddress pa = new PeerAddress(Number160.ZERO, Inet4Address.getByName(IP), PORT, PORT);
            sender = new PeerMaker(new Number160("0x9876")).setPorts(PORT).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean());
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCP(pa, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null) {
                sender.shutdown();
            }
        }
    }

    /**
     * Test discover ping.
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void sendPingTCPDiscover() throws IOException {
        Peer sender = null;
        try {
            PeerAddress pa = new PeerAddress(Number160.ZERO, Inet4Address.getByName(IP), PORT, PORT);
            sender = new PeerMaker(new Number160("0x9876")).setPorts(PORT).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean());
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCPDiscover(pa, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null) {
                sender.shutdown();
            }
        }
    }

    /**
     * Test probe ping.
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void sendPingTCPProbe() throws IOException {
        Peer sender = null;
        try {
            PeerAddress pa = new PeerAddress(Number160.ZERO, Inet4Address.getByName(IP), PORT, PORT);
            sender = new PeerMaker(new Number160("0x9876")).setPorts(PORT).makeAndListen();
            HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender.getConnectionBean());
            final FutureChannelCreator fcc = sender.getConnectionBean().getConnectionReservation().reserve(1);
            fcc.awaitUninterruptibly();
            ChannelCreator cc = fcc.getChannelCreator();
            FutureResponse fr = handshake.pingTCPProbe(pa, cc);
            fr.awaitUninterruptibly();
            Assert.assertEquals(true, fr.isSuccess());
            sender.getConnectionBean().getConnectionReservation().release(cc);
        } finally {
            if (sender != null) {
                sender.shutdown();
            }
        }
    }

    /**
     * The receiver.
     * 
     * @throws IOException .
     */
    @Ignore
    @Test
    public void receivePing() throws IOException {
        Peer recv = null;
        try {
            recv = new PeerMaker(new Number160("0x1234")).setPorts(PORT).makeAndListen();
            /**
             * HandshakeRPC with custom debug output.
             * 
             * @author Thomas Bocek
             * 
             */
            final class MyHandshakeRPC extends HandshakeRPC {
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
        } finally {
            if (recv != null) {
                recv.shutdown();
            }
        }
    }
}
