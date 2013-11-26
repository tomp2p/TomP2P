package net.tomp2p.p2p;

import java.net.InetAddress;
import java.net.StandardProtocolFamily;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;

//TODO: find out why the shutdown takes 2 seconds
public class TestConnection {

    @Test
    public void test() throws Exception {
        Random rnd = new Random(42);
        Peer peer1 = null;
        Peer peer2 = null;
        try {
            Bindings b1 = new Bindings().addProtocol(StandardProtocolFamily.INET).addAddress(InetAddress.getByName("127.0.0.1"));
            Bindings b2 = new Bindings().addProtocol(StandardProtocolFamily.INET).addAddress(InetAddress.getByName("127.0.0.1"));
            
            peer1 = new PeerMaker(new Number160(rnd)).ports(4005).bindings(b1).makeAndListen();
            peer2 = new PeerMaker(new Number160(rnd)).ports(4006).bindings(b2).makeAndListen();

            peer2.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    return "world!";
                }
            });
            // keep the connection for 20s alive. Setting -1 means to keep it
            // open as long as possible
            FutureBootstrap masterAnother = peer1.bootstrap().setPeerAddress(peer2.getPeerAddress()).start();
            FutureBootstrap anotherMaster = peer2.bootstrap().setPeerAddress(peer1.getPeerAddress()).start();
            masterAnother.awaitUninterruptibly();
            anotherMaster.awaitUninterruptibly();
            FuturePeerConnection fpc = peer1.createPeerConnection(peer2.getPeerAddress());
            // fpc.awaitUninterruptibly();
            // PeerConnection peerConnection = fpc.peerConnection();
            String sentObject = "Hello";
            FutureDirect fd = peer1.sendDirect(fpc).setObject(sentObject).start();
            System.out.println("send " + sentObject);
            fd.awaitUninterruptibly();
            Assert.assertEquals(true, fd.isSuccess());
            System.out.println("received " + fd.object() + " connections: "
                    + ChannelCreator.tcpConnectionCount());
            // we reuse the connection
            long start = System.currentTimeMillis();
            System.out.println("send " + sentObject);
            fd = peer1.sendDirect(fpc).setObject(sentObject).start();
            fd.awaitUninterruptibly();
            System.err.println(fd.getFailedReason());
            Assert.assertEquals(true, fd.isSuccess());
            System.err.println(fd.getFailedReason());
            System.out.println("received " + fd.object() + " connections: "
                    + ChannelCreator.tcpConnectionCount());
            // now we don't want to keep the connection open anymore:
            double duration = (System.currentTimeMillis() - start) / 1000d;
            System.out.println("Send and get in s:" + duration);
            fpc.close().await();
            System.out.println("done");
        } finally {
            if (peer1 != null) {
                peer1.shutdown().await();
                System.out.println("done1");
            }
            if (peer2 != null) {
                peer2.shutdown().await();
                System.out.println("done2");
            }
        }
    }
}
