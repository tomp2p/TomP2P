package net.tomp2p.p2p;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.futures.BaseFuture;
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
            master = new PeerMaker(new Number160(rnd)).ports(4001).makeAndListen();
            slave = new PeerMaker(new Number160(rnd)).ports(4002).makeAndListen();
            FutureDiscover fd = master.discover().setPeerAddress(slave.getPeerAddress()).start();
            fd.awaitUninterruptibly();
            System.err.println(fd.getFailedReason());
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
            master = new PeerMaker(new Number160(rnd)).ports(4001).makeAndListen();
            slave = new PeerMaker(new Number160(rnd)).ports(4002).makeAndListen();
            FutureBootstrap fb = master.bootstrap().setInetAddress(InetAddress.getByName("127.0.0.1"))
                    .setPorts(3000).start();
            fb.awaitUninterruptibly();
            Assert.assertEquals(false, fb.isSuccess());
            System.err.println(fb.getFailedReason());
            fb = master.bootstrap().setPeerAddress(slave.getPeerAddress()).start();
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
                    FutureBootstrap res = peers[i].bootstrap().setPeerAddress(master.getPeerAddress())
                            .start();
                    tmp.add(res);
                }
            }
            int i = 0;
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                if (fm.isFailed())
                    System.err.println(fm.getFailedReason());
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
                    FutureBootstrap res = peers[i].bootstrap().setPeerAddress(master.getPeerAddress())
                            .start();
                    tmp.add(res);
                }
            }
            int i = 0;
            for (FutureBootstrap fm : tmp) {
                fm.awaitUninterruptibly();
                if (fm.isFailed())
                    System.err.println("FAILL:" + fm.getFailedReason());
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
    public void testBootstrap3() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        try {
            // setup
            Peer[] peers = Utils2.createNonMaintenanceNodes(100, rnd, 4001);
            master = peers[0];
            // do testing
            List<FutureBootstrap> tmp = new ArrayList<FutureBootstrap>();
            // we start from 1, because a broadcast to ourself will not get
            // replied.
            for (int i = 1; i < peers.length; i++) {
                FutureBootstrap res = peers[i].bootstrap().setPorts(4001).setBroadcast().start();
                tmp.add(res);
            }
            int i = 0;
            for (FutureBootstrap fm : tmp) {
                System.err.println("i:" + (++i));
                fm.awaitUninterruptibly();
                if (fm.isFailed())
                    System.err.println("error " + fm.getFailedReason());
                Assert.assertEquals(true, fm.isSuccess());

            }
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
        }
    }

    @Test
    public void testBootstrap4() throws Exception {
        final Random rnd = new Random(42);
        Peer master = null;
        Peer slave = null;
        try {
            master = new PeerMaker(new Number160(rnd)).ports(4001).makeAndListen();
            slave = new PeerMaker(new Number160(rnd)).ports(4002).makeAndListen();
            BaseFuture res = slave.ping().setPort(4001).setBroadcast().start();
            res.awaitUninterruptibly();
            Assert.assertEquals(true, res.isSuccess());
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
    public void testBootstrap5() throws Exception {
        final Random rnd = new Random(42);
        Peer peer = null;
        try {
            peer = new PeerMaker(new Number160(rnd)).ports(4000).makeAndListen();
            PeerAddress pa = new PeerAddress(new Number160(rnd), "192.168.77.77", 4000, 4000);
            FutureBootstrap tmp = peer.bootstrap().setPeerAddress(pa).start();
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
            peer = new PeerMaker(new Number160(rnd)).ports(4000).makeAndListen();
            FutureBootstrap tmp = peer.bootstrap().setPeerAddress(peer.getPeerAddress()).start();
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
            peer = new PeerMaker(new Number160(rnd)).ports(4000).makeAndListen();

            Collection<PeerAddress> bootstrapTo = new ArrayList<PeerAddress>(2);
            PeerAddress pa = new PeerAddress(new Number160(rnd), "192.168.77.77", 4000, 4000);
            bootstrapTo.add(peer.getPeerAddress());
            bootstrapTo.add(pa);
            FutureBootstrap tmp = peer.bootstrap().setBootstrapTo(bootstrapTo).start();
            tmp.awaitUninterruptibly();
            Assert.assertEquals(false, tmp.isSuccess());
        } finally {
            if (peer != null) {
                peer.shutdown().await();
            }
        }
    }
}
