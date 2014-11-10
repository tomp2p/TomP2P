/*
 * This file is part of Bitsquare.
 *
 * Bitsquare is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at
 * your option) any later version.
 *
 * Bitsquare is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Bitsquare. If not, see <http://www.gnu.org/licenses/>.
 */

package net.tomp2p.examples;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.StandardProtocolFamily;
import net.tomp2p.dht.*;
import net.tomp2p.examples.utils.Repeat;
import net.tomp2p.examples.utils.RepeatRule;
import net.tomp2p.futures.*;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.storage.Data;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test bootstrapping, DHT operations like put/get/add/remove and sendDirect in both LAN and WAN environment
 * Test scenarios in direct connection, auto port forwarding or relay mode.
 * <p>
 * To start a bootstrap node code use the {@link net.tomp2p.examples.SeedNodeForTesting} class.
 * <p>
 * To configure your test environment edit the static fields for id, IP and port.
 * In the configure method and the connectionType you can define your test scenario.
 */
@Ignore
public class TomP2PTests {
    private enum ConnectionType {
        UNKNOWN, DIRECT, NAT, RELAY
    }

    private static final Logger log = LoggerFactory.getLogger(TomP2PTests.class);

    final static String BOOTSTRAP_NODE_ID = "seed";
    private final static String BOOTSTRAP_NODE_IP = "127.0.0.1";
    final static int BOOTSTRAP_NODE_PORT = 5000;

    // If you want to test in one specific connection mode define it directly, otherwise use UNKNOWN
    private static final ConnectionType FORCED_CONNECTION_TYPE = ConnectionType.DIRECT;

    private static final PeerAddress BOOTSTRAP_NODE_ADDRESS;

    static {
        try {
            BOOTSTRAP_NODE_ADDRESS = new PeerAddress(
                    Number160.createHash(BOOTSTRAP_NODE_ID),
                    BOOTSTRAP_NODE_IP, BOOTSTRAP_NODE_PORT, BOOTSTRAP_NODE_PORT);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    // Use to stress tests by repeating them
    private static final int STRESS_TEST_COUNT = 100;

    private Peer peer;
    private PeerDHT peer1DHT;
    private PeerDHT peer2DHT;
    private int client1Port;
    private int client2Port;
    private ConnectionType resolvedConnectionType;
    public @Rule RepeatRule repeatRule = new RepeatRule();

    @Before
    public void setUp() {
        client1Port = 7777;
        client2Port = 7778;
    }

    @After
    public void tearDown() {
        if (peer1DHT != null) {
            BaseFuture future = peer1DHT.shutdown();
            future.awaitUninterruptibly();
            future.awaitListenersUninterruptibly();
        }
        if (peer2DHT != null) {
            BaseFuture future = peer2DHT.shutdown();
            future.awaitUninterruptibly();
            future.awaitListenersUninterruptibly();
        }
        if (peer != null) {
            BaseFuture future = peer.shutdown();
            future.awaitUninterruptibly();
            future.awaitListenersUninterruptibly();
        }
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void bootstrapInUnknownMode() throws Exception {
        if (FORCED_CONNECTION_TYPE == ConnectionType.UNKNOWN) {
            peer = bootstrapInUnknownMode(client1Port);
            assertNotNull(peer);
        }
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testBootstrapDirectConnection() throws Exception {
        if (FORCED_CONNECTION_TYPE == ConnectionType.DIRECT) {
            peer = bootstrapDirectConnection(client1Port);
            assertNotNull(peer);
        }
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testBootstrapWithPortForwarding() throws Exception {
        if (FORCED_CONNECTION_TYPE == ConnectionType.NAT) {
            peer = bootstrapWithPortForwarding(client1Port);
            assertNotNull(peer);
        }
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testBootstrapInRelayMode() throws Exception {
        if (FORCED_CONNECTION_TYPE == ConnectionType.RELAY) {
            peer = bootstrapInRelayMode(client1Port);
            assertNotNull(peer);
        }
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testPut() throws Exception {
        peer1DHT = getDHTPeer(client1Port);
        FuturePut futurePut = peer1DHT.put(Number160.createHash("key")).data(new Data("hallo")).start();
        futurePut.awaitUninterruptibly();
        assertTrue(futurePut.isSuccess());
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testPutGet() throws Exception {
        peer1DHT = getDHTPeer(client1Port);
        FuturePut futurePut = peer1DHT.put(Number160.createHash("key")).data(new Data("hallo")).start();
        futurePut.awaitUninterruptibly();
        assertTrue(futurePut.isSuccess());

        peer2DHT = getDHTPeer(client2Port);
        FutureGet futureGet = peer2DHT.get(Number160.createHash("key")).start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());
        assertEquals("hallo", futureGet.data().object());
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testAdd() throws Exception {
        peer1DHT = getDHTPeer(client1Port);
        FuturePut futurePut1 = peer1DHT.add(Number160.createHash("locationKey")).data(new Data("hallo1")).start();
        futurePut1.awaitUninterruptibly();
        assertTrue(futurePut1.isSuccess());
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testAddGet() throws Exception {
        peer1DHT = getDHTPeer(client1Port);
        FuturePut futurePut1 = peer1DHT.add(Number160.createHash("locationKey")).data(new Data("hallo1"))
                                       .start();
        futurePut1.awaitUninterruptibly();
        assertTrue(futurePut1.isSuccess());

        FuturePut futurePut2 = peer1DHT.add(Number160.createHash("locationKey")).data(new Data("hallo2"))
                                       .start();
        futurePut2.awaitUninterruptibly();
        assertTrue(futurePut2.isSuccess());


        peer2DHT = getDHTPeer(client2Port);
        FutureGet futureGet = peer2DHT.get(Number160.createHash("locationKey")).all().start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());

        assertTrue(futureGet.dataMap().values().contains(new Data("hallo1")));
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo2")));
        assertTrue(futureGet.dataMap().values().size() == 2);
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testAddGetWithReconnect() throws Exception {
        peer1DHT = getDHTPeer(client1Port);
        FuturePut futurePut1 = peer1DHT.add(Number160.createHash("locationKey")).data(new Data("hallo1")).start();
        futurePut1.awaitUninterruptibly();
        assertTrue(futurePut1.isSuccess());
        FuturePut futurePut2 = peer1DHT.add(Number160.createHash("locationKey")).data(new Data("hallo2")).start();
        futurePut2.awaitUninterruptibly();
        assertTrue(futurePut2.isSuccess());

        peer2DHT = getDHTPeer(client2Port);
        FutureGet futureGet = peer2DHT.get(Number160.createHash("locationKey")).all().start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo1")));
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo2")));
        assertTrue(futureGet.dataMap().values().size() == 2);

        // shut down peer2
        BaseFuture future = peer2DHT.shutdown();
        future.awaitUninterruptibly();
        future.awaitListenersUninterruptibly();

        // start up peer2
        peer2DHT = getDHTPeer(client2Port);
        futureGet = peer2DHT.get(Number160.createHash("locationKey")).all().start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo1")));
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo2")));
        assertTrue(futureGet.dataMap().values().size() == 2);

        futureGet = peer1DHT.get(Number160.createHash("locationKey")).all().start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo1")));
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo2")));
        assertTrue(futureGet.dataMap().values().size() == 2);

        // shut down both
        future = peer2DHT.shutdown();
        future.awaitUninterruptibly();
        future.awaitListenersUninterruptibly();
        future = peer1DHT.shutdown();
        future.awaitUninterruptibly();
        future.awaitListenersUninterruptibly();

        // start up both
        peer1DHT = getDHTPeer(client1Port);
        futureGet = peer1DHT.get(Number160.createHash("locationKey")).all().start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo1")));
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo2")));
        assertTrue(futureGet.dataMap().values().size() == 2);

        peer2DHT = getDHTPeer(client2Port);
        futureGet = peer2DHT.get(Number160.createHash("locationKey")).all().start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo1")));
        assertTrue(futureGet.dataMap().values().contains(new Data("hallo2")));
        assertTrue(futureGet.dataMap().values().size() == 2);
    }

    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testAddRemove() throws Exception {

        if (FORCED_CONNECTION_TYPE == ConnectionType.DIRECT) {
            SeedNodeForTesting.main(null);
        }

        peer1DHT = getDHTPeer(client1Port);
        FuturePut futurePut1 = peer1DHT.add(Number160.createHash("locationKey")).data(new Data("hallo1")).start();
        futurePut1.awaitUninterruptibly();
        futurePut1.awaitListenersUninterruptibly();
        assertTrue(futurePut1.isSuccess());

        FuturePut futurePut2 = peer1DHT.add(Number160.createHash("locationKey")).data(new Data("hallo2")).start();
        futurePut2.awaitUninterruptibly();
        futurePut2.awaitListenersUninterruptibly();
        assertTrue(futurePut2.isSuccess());


        peer2DHT = getDHTPeer(client2Port);
        Number160 contentKey = new Data("hallo1").hash();
        FutureRemove futureRemove = peer2DHT.remove(Number160.createHash("locationKey")).contentKey(contentKey).start();
        futureRemove.awaitUninterruptibly();
        futureRemove.awaitListenersUninterruptibly();

        // We don't test futureRemove.isSuccess() as this API does not fit well to that operation,
        // it might change in future to something like foundAndRemoved and notFound
        // See discussion at: https://github.com/tomp2p/TomP2P/issues/57#issuecomment-62069840

        assertTrue(futureRemove.isSuccess());

        FutureGet futureGet = peer2DHT.get(Number160.createHash("locationKey")).all().start();
        futureGet.awaitUninterruptibly();
        assertTrue(futureGet.isSuccess());
        if (!futureGet.dataMap().values().contains(new Data("hallo2"))) {
            log.error("raw data has the value, the evaluated not!");
        }

        assertTrue(futureGet.dataMap().values().contains(new Data("hallo2")));
        assertTrue(futureGet.dataMap().values().size() == 1);

        if (FORCED_CONNECTION_TYPE == ConnectionType.DIRECT) {
            SeedNodeForTesting.stop();
        }
    }


    // The sendDirect operation fails in port forwarding mode because most routers does not support NAT reflections.
    // So if both clients are behind NAT they cannot send direct message to each other.
    // That will probably be fixed in a future version of TomP2P
    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testSendDirectBetweenLocalPeers() throws Exception {
        if (FORCED_CONNECTION_TYPE != ConnectionType.NAT && resolvedConnectionType != ConnectionType.RELAY) {
            peer1DHT = getDHTPeer(client1Port);
            peer2DHT = getDHTPeer(client2Port);

            final CountDownLatch countDownLatch = new CountDownLatch(1);

            final StringBuilder result = new StringBuilder();
            peer2DHT.peer().objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    countDownLatch.countDown();
                    result.append(String.valueOf(request));
                    return "pong";
                }
            });

            FuturePeerConnection futurePeerConnection = peer1DHT.peer().createPeerConnection(peer2DHT.peer()
                                                                                                     .peerAddress(), 500);
            FutureDirect futureDirect = peer1DHT.peer().sendDirect(futurePeerConnection).object("hallo").start();
            futureDirect.awaitUninterruptibly();


            countDownLatch.await(3, TimeUnit.SECONDS);
            if (countDownLatch.getCount() > 0)
                Assert.fail("The test method did not complete successfully!");

            assertEquals("hallo", result.toString());
            assertTrue(futureDirect.isSuccess());
            log.debug(futureDirect.object().toString());
            assertEquals("pong", futureDirect.object());
        }
    }

    // This test should always succeed as we use the bootstrap node as receiver.
    // A node can send a message to another peer which is not in the same LAN.
    @Test
    @Repeat(STRESS_TEST_COUNT)
    public void testSendDirectToSeedNode() throws Exception {
        peer1DHT = getDHTPeer(client1Port);
        FuturePeerConnection futurePeerConnection =
                peer1DHT.peer().createPeerConnection(BOOTSTRAP_NODE_ADDRESS, 500);
        FutureDirect futureDirect = peer1DHT.peer().sendDirect(futurePeerConnection).object("hallo").start();
        futureDirect.awaitUninterruptibly();
        assertTrue(futureDirect.isSuccess());
        assertEquals("pong", futureDirect.object());
    }

    private Peer bootstrapDirectConnection(int clientPort) {
        Peer peer = null;
        try {
            Number160 peerId = new Number160(new Random(43L));
            PeerMapConfiguration pmc = new PeerMapConfiguration(peerId).peerNoVerification();
            PeerMap pm = new PeerMap(pmc);
            ChannelClientConfiguration cc = PeerBuilder.createDefaultChannelClientConfiguration();
            cc.maxPermitsTCP(100);
            cc.maxPermitsUDP(100);
            peer = new PeerBuilder(peerId).bindings(getBindings()).channelClientConfiguration(cc).peerMap(pm)
                                          .ports(clientPort).start();
            FutureDiscover futureDiscover = peer.discover().peerAddress(BOOTSTRAP_NODE_ADDRESS).start();
            futureDiscover.awaitUninterruptibly();
            if (futureDiscover.isSuccess()) {
                log.info("Discover with direct connection successful. Address = " + futureDiscover.peerAddress());

                FutureBootstrap futureBootstrap = peer.bootstrap().peerAddress(BOOTSTRAP_NODE_ADDRESS).start();
                futureBootstrap.awaitUninterruptibly();
                if (futureBootstrap.isSuccess()) {
                    return peer;
                }
                else {
                    log.warn("Bootstrap failed. Reason = " + futureBootstrap.failedReason());
                    peer.shutdown().awaitUninterruptibly();
                    return null;
                }
            }
            else {
                log.warn("Discover with direct connection failed. Reason = " + futureDiscover.failedReason());
                peer.shutdown().awaitUninterruptibly();
                return null;
            }
        } catch (IOException e) {
            log.warn("Discover with direct connection failed. Exception = " + e.getMessage());
            if (peer != null)
                peer.shutdown().awaitUninterruptibly();

            e.printStackTrace();
            return null;
        }
    }

    private Peer bootstrapWithPortForwarding(int clientPort) {
        Number160 peerId = new Number160(new Random(43L));
        Peer peer = null;
        try {
            peer = new PeerBuilder(peerId).bindings(getBindings()).behindFirewall()
                                          .ports(clientPort).start();

            PeerNAT peerNAT = new PeerBuilderNAT(peer).start();
            FutureDiscover futureDiscover = peer.discover().peerAddress(BOOTSTRAP_NODE_ADDRESS).start();
            FutureNAT futureNAT = peerNAT.startSetupPortforwarding(futureDiscover);
            futureNAT.awaitUninterruptibly();
            if (futureNAT.isSuccess()) {
                log.info("Automatic port forwarding is setup. Now we do a futureDiscover again. Address = " +
                                 futureNAT.peerAddress());
                futureDiscover = peer.discover().peerAddress(BOOTSTRAP_NODE_ADDRESS).start();
                futureDiscover.awaitUninterruptibly();
                if (futureDiscover.isSuccess()) {
                    log.info("Discover with automatic port forwarding was successful. Address = " + futureDiscover
                            .peerAddress());

                    FutureBootstrap futureBootstrap = peer.bootstrap().peerAddress(BOOTSTRAP_NODE_ADDRESS).start();
                    futureBootstrap.awaitUninterruptibly();
                    if (futureBootstrap.isSuccess()) {
                        return peer;
                    }
                    else {
                        log.warn("Bootstrap failed. Reason = " + futureBootstrap.failedReason());
                        peer.shutdown().awaitUninterruptibly();
                        return null;
                    }
                }
                else {
                    log.warn("Discover with automatic port forwarding failed. Reason = " + futureDiscover
                            .failedReason());
                    peer.shutdown().awaitUninterruptibly();
                    return null;
                }
            }
            else {
                log.warn("StartSetupPortforwarding failed. Reason = " + futureNAT
                        .failedReason());
                peer.shutdown().awaitUninterruptibly();
                return null;
            }
        } catch (IOException e) {
            log.warn("Discover with automatic port forwarding failed. Exception = " + e.getMessage());
            if (peer != null)
                peer.shutdown().awaitUninterruptibly();

            e.printStackTrace();
            return null;
        }
    }

    private Peer bootstrapInRelayMode(int clientPort) {
        Number160 peerId = new Number160(new Random(43L));
        Peer peer = null;
        try {
            peer = new PeerBuilder(peerId).bindings(getBindings()).behindFirewall()
                                          .ports(clientPort).start();

            PeerNAT peerNAT = new PeerBuilderNAT(peer).start();
            FutureDiscover futureDiscover = peer.discover().peerAddress(BOOTSTRAP_NODE_ADDRESS).start();
            FutureNAT futureNAT = peerNAT.startSetupPortforwarding(futureDiscover);
            FutureRelayNAT futureRelayNAT = peerNAT.startRelay(futureDiscover, futureNAT);
            futureRelayNAT.awaitUninterruptibly();
            if (futureRelayNAT.isSuccess()) {
                log.info("Bootstrap using relay was successful. Address = " + peer.peerAddress());

                FutureBootstrap futureBootstrap = peer.bootstrap().peerAddress(BOOTSTRAP_NODE_ADDRESS).start();
                futureBootstrap.awaitUninterruptibly();
                if (futureBootstrap.isSuccess()) {
                    return peer;
                }
                else {
                    log.warn("Bootstrap failed. Reason = " + futureBootstrap.failedReason());
                    peer.shutdown().awaitUninterruptibly();
                    return null;
                }
            }
            else {
                log.error("Bootstrap using relay failed " + futureRelayNAT.failedReason());
                futureRelayNAT.shutdown();
                peer.shutdown().awaitUninterruptibly();
                return null;
            }
        } catch (IOException e) {
            log.error("Bootstrap using relay failed. Exception " + e.getMessage());
            if (peer != null)
                peer.shutdown().awaitUninterruptibly();

            e.printStackTrace();
            return null;
        }
    }

    private Peer bootstrapInUnknownMode(int clientPort) {
        resolvedConnectionType = ConnectionType.DIRECT;
        Peer peer = bootstrapDirectConnection(clientPort);
        if (peer != null)
            return peer;

        resolvedConnectionType = ConnectionType.NAT;
        peer = bootstrapWithPortForwarding(clientPort);
        if (peer != null)
            return peer;

        resolvedConnectionType = ConnectionType.RELAY;
        peer = bootstrapInRelayMode(clientPort);
        if (peer != null)
            return peer;
        else
            log.error("Bootstrapping in all modes failed. Is bootstrap node with address " + BOOTSTRAP_NODE_ADDRESS + "running?");

        resolvedConnectionType = null;
        return peer;
    }

    private PeerDHT getDHTPeer(int clientPort) {
        Peer peer;
        if (FORCED_CONNECTION_TYPE == ConnectionType.DIRECT) {
            peer = bootstrapDirectConnection(clientPort);
        }
        else if (FORCED_CONNECTION_TYPE == ConnectionType.NAT) {
            peer = bootstrapWithPortForwarding(clientPort);
        }
        else if (FORCED_CONNECTION_TYPE == ConnectionType.RELAY) {
            peer = bootstrapInRelayMode(clientPort);
        }
        else {
            peer = bootstrapInUnknownMode(clientPort);
        }

        if (peer == null)
            Assert.fail("Bootstrapping failed." +
                                " forcedConnectionType= " + FORCED_CONNECTION_TYPE +
                                " resolvedConnectionType= " + resolvedConnectionType + "." +
                                " Is bootstrap node  with address " + BOOTSTRAP_NODE_ADDRESS + "running?");

        return new PeerBuilderDHT(peer).start();
    }

    private Bindings getBindings() {
        Bindings bindings = new Bindings();
        bindings.addProtocol(StandardProtocolFamily.INET);
        return bindings;
    }
}
