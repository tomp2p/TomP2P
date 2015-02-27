package net.tomp2p.p2p;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.Assert;
import org.junit.Test;

public class TestBootstrap {
    @Test
    public void testBootstrapDiscover() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        Peer slave = null;
        try {
            master = new PeerBuilder(new Number160(rnd)).ports(4001).start();
            slave = new PeerBuilder(new Number160(rnd)).ports(4002).start();
            FutureDiscover fd = master.discover().peerAddress(slave.peerAddress()).start();
            fd.awaitUninterruptibly();
            System.err.println(fd.failedReason());
            Assert.assertEquals(true, fd.isSuccess());
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
            if (slave != null) {
                slave.shutdown().await();
            }
        }
    }

    @Test
    public void testBootstrapFail() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        Peer slave = null;
        try {
            master = new PeerBuilder(new Number160(rnd)).ports(4001).start();
            slave = new PeerBuilder(new Number160(rnd)).ports(4002).start();
            FutureBootstrap fb = master.bootstrap().inetAddress(InetAddress.getByName("127.0.0.1"))
                    .ports(3000).start();
            fb.awaitUninterruptibly();
            Assert.assertEquals(false, fb.isSuccess());
            System.err.println(fb.failedReason());
            fb = master.bootstrap().peerAddress(slave.peerAddress()).start();
            fb.awaitUninterruptibly();
            Assert.assertEquals(true, fb.isSuccess());

        } finally {
            if (master != null) {
                master.shutdown().await();
            }
            if (slave != null) {
                slave.shutdown().await();
            }
        }
    }

    @Test
    public void testBootstrap() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
            master = peers[0];
            // do testing
            List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
            for (int i = 0; i < peers.length; i++) {
                if (peers[i] != master) {
                    FutureBootstrap res = peers[i].bootstrap().peerAddress(master.peerAddress())
                            .start();
                    tmp.add(res);
                }
            }
            int i = 0;
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                if (fm.isFailed())
                    System.err.println(fm.failedReason());
                Assert.assertEquals(true, fm.isSuccess());
                System.err.println("i:" + (++i));
            }
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testBootstrap2() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        try {
            // setup
            Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
            master = peers[0];
            // do testing
            List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
            for (int i = 0; i < peers.length; i++) {
                if (peers[i] != master) {
                    FutureBootstrap res = peers[i].bootstrap().peerAddress(master.peerAddress())
                            .start();
                    tmp.add(res);
                }
            }
            int i = 0;
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                if (fm.isFailed())
                    System.err.println("FAILL:" + fm.failedReason());
                Assert.assertEquals(true, fm.isSuccess());
                System.err.println("i:" + (++i));
            }
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testBootstrap5() throws Exception {
        final Random rnd = new Random(42);
        Peer peer = null;
        try {
            peer = new PeerBuilder(new Number160(rnd)).ports(4000).start();
            PeerAddress pa = new PeerAddress(new Number160(rnd), "192.168.77.77", 4000, 4000);
            FutureBootstrap tmp = peer.bootstrap().peerAddress(pa).start();
            tmp.awaitUninterruptibly();
            Assert.assertEquals(false, tmp.isSuccess());
        } finally {
            if (peer != null) {
                peer.shutdown().await();
            }
        }
    }

    /**
     * This test works because if a peer bootstraps to itself, then its typically the bootstrapping peer.
     * 
     * @throws Exception .
     */
    @Test
    public void testBootstrap6() throws Exception {
        final Random rnd = new Random(42);
        Peer peer = null;
        try {
            peer = new PeerBuilder(new Number160(rnd)).ports(4000).start();
            FutureBootstrap tmp = peer.bootstrap().peerAddress(peer.peerAddress()).start();
            tmp.awaitUninterruptibly();
            Assert.assertEquals(true, tmp.isSuccess());
        } finally {
            if (peer != null) {
                peer.shutdown().await();
            }
        }
    }

    /**
     * This test fails because if a peer bootstraps to itself, then its typically the bootstrapping peer. However, if we
     * bootstrap to more than one peer, and the boostrap fails, then we fail this test.
     * 
     * @throws Exception .
     */
    @Test
    public void testBootstrap7() throws Exception {
        final Random rnd = new Random(42);
        Peer peer = null;
        try {
            peer = new PeerBuilder(new Number160(rnd)).ports(4000).start();

            Collection<PeerAddress> bootstrapTo = new ArrayList<PeerAddress>(2);
            PeerAddress pa = new PeerAddress(new Number160(rnd), "192.168.77.77", 4000, 4000);
            bootstrapTo.add(peer.peerAddress());
            bootstrapTo.add(pa);
            FutureBootstrap tmp = peer.bootstrap().bootstrapTo(bootstrapTo).start();
            tmp.awaitUninterruptibly();
            Assert.assertEquals(false, tmp.isSuccess());
        } finally {
            if (peer != null) {
                peer.shutdown().await();
            }
        }
    }
}
