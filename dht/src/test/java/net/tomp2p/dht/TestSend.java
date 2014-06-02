package net.tomp2p.dht;

import java.io.IOException;
import java.net.InetAddress;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Test send over network.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestSend {
    private static final int PORT = 5000;
    private static final String ADDR = "192.168.1.x";
    private static final RequestP2PConfiguration REQ = new RequestP2PConfiguration(1, 10, 0);

    /**
     * Start the sender and wait forever.
     * 
     * @throws InterruptedException .
     * @throws IOException .
     */
    @Ignore
    @Test
    public void startSender() throws IOException, InterruptedException {

        final int maxPeers = 10;
        PeerDHT[] peers = createPeers(PORT, maxPeers, "sender");
        for (PeerDHT peer : peers) {
            peer.peer().bootstrap().inetAddress(InetAddress.getByName(ADDR)).ports(PORT).start().awaitUninterruptibly();
            peer.peer().objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(final PeerAddress sender, final Object request) throws Exception {
                    System.out.println("wrong!!!!");
                    return "wrong!!!!";
                }
            });
        }
        Number160 keyForID = Number160.createHash("key");
        Msg message = new Msg();

        System.out.println(String.format("Sending message '%s' to key '%s' converted to '%s'", message.getType(),
                "key", keyForID.toString()));
        FutureSend futureDHT = peers[0].send(keyForID).setObject(message).requestP2PConfiguration(REQ).start();
        futureDHT.awaitUninterruptibly();
        System.out.println("got: " + futureDHT.getObject());
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Start the recipient and wait forever.
     * 
     * @throws InterruptedException .
     * @throws IOException .
     */
    @Ignore
    @Test
    public void startReiver() throws IOException, InterruptedException {

        final int maxPeers = 10;
        PeerDHT[] peers = createPeers(PORT, maxPeers, "receiver");
        for (int i = 1; i < maxPeers; i++) {
        	PeerDHT peer = peers[i];
            peer.peer().bootstrap().peerAddress(peers[0].peer().peerAddress()).start().awaitUninterruptibly();
            peer.peer().objectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(final PeerAddress sender, final Object request) throws Exception {
                    System.out.println("got it!");
                    return "recipient got it";
                }
            });
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    private PeerDHT[] createPeers(int startport, int nr, String name) throws IOException {
        PeerDHT[] peers = new PeerDHT[nr];
        for (int i = 0; i < nr; i++) {
        	Peer peer = new PeerBuilder(Number160.createHash(name + i)).ports(PORT + i).start();
            peers[i] = new PeerDHT(peer);
        }
        return peers;
    }

    private static class Msg {
        private String type = "type";

        public String getType() {
            return type;
        }
    }
}
