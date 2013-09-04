package net.tomp2p.p2p.real;

import java.io.IOException;
import java.net.InetAddress;

import org.junit.Test;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

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
    @Test
    public void startSender() throws IOException, InterruptedException {

        final int maxPeers = 10;
        Peer[] peers = createPeers(PORT, maxPeers, "sender");
        for (Peer peer : peers) {
            peer.bootstrap().setInetAddress(InetAddress.getByName(ADDR)).setPorts(PORT).start().awaitUninterruptibly();
            peer.setObjectDataReply(new ObjectDataReply() {
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
        FutureDirect futureDHT = peers[0].send(keyForID).setObject(message).setRequestP2PConfiguration(REQ).start();
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
    @Test
    public void startReiver() throws IOException, InterruptedException {

        final int maxPeers = 10;
        Peer[] peers = createPeers(PORT, maxPeers, "receiver");
        for (int i = 1; i < maxPeers; i++) {
            Peer peer = peers[i];
            peer.bootstrap().setPeerAddress(peers[0].getPeerAddress()).start().awaitUninterruptibly();
            peer.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(final PeerAddress sender, final Object request) throws Exception {
                    System.out.println("got it!");
                    return "recipient got it";
                }
            });
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    private Peer[] createPeers(int startport, int nr, String name) throws IOException {
        Peer[] peers = new Peer[nr];
        for (int i = 0; i < nr; i++) {
            peers[i] = new PeerMaker(Number160.createHash(name + i)).ports(PORT + i).makeAndListen();
        }
        return peers;
    }

    private static class Msg {
        private String type = "type";

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

}
