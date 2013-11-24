package net.tomp2p.p2p;

import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRelay {
    final private static Random rnd = new Random(42L);
    private static final Logger LOG = LoggerFactory.getLogger(TestRelay.class);
    @Test
    public void testPeerConnection() throws Exception {
        Peer master = null;
        Peer slave = null;
        try {
        master = Utils2.createNodes(1, rnd, 4001, null, false, false)[0];
        slave = Utils2.createNodes(1, rnd, 4002, null, false, false)[0];
        System.err.println("master is " + master.getPeerAddress());
        System.err.println("slave is " + slave.getPeerAddress());
        
        FuturePeerConnection pcMaster = master.createPeerConnection(slave.getPeerAddress());
        MyDirectDataRPC myDirectDataRPC = new MyDirectDataRPC(slave.getPeerBean(), slave.getConnectionBean());
        slave.setDirectDataRPC(myDirectDataRPC);
        
        slave.setObjectDataReply(new ObjectDataReply() {
            @Override
            public Object reply(PeerAddress sender, Object request) throws Exception {
                return "yoo!";
            }
        });
        
        master.setObjectDataReply(new ObjectDataReply() {
            @Override
            public Object reply(PeerAddress sender, Object request) throws Exception {
                return "world!";
            }
        });
        
        FutureDirect futureResponse = master.sendDirect(pcMaster).setObject("test").start().awaitUninterruptibly0();
        Assert.assertEquals("yoo!", futureResponse.object());
        
        FuturePeerConnection pcSlave = myDirectDataRPC.peerConnection();
        
        futureResponse = slave.sendDirect(pcSlave).setObject("hello").start().awaitUninterruptibly0();
        System.err.println(futureResponse.getFailedReason());
        Assert.assertEquals("world!", futureResponse.object());
        
        Thread.sleep(10000);
        pcSlave.close();
        pcMaster.close();
        LOG.error("done");
        
        } finally {
            if (master != null) {
                master.shutdown().await();
            }
            if (slave != null) {
                slave.shutdown().await();
            }
        }

    }
    
    private static class MyDirectDataRPC extends DirectDataRPC {
        
        private FuturePeerConnection futurePeerConnection;

        MyDirectDataRPC(PeerBean peerBean, ConnectionBean connectionBean) {
            super(peerBean, connectionBean);
        }
        @Override
        public Message handleResponse(Message message, PeerConnection peerConnection, boolean sign)
                throws Exception {
            futurePeerConnection = new FuturePeerConnection(message.getSender());
            futurePeerConnection.setDone(peerConnection);
            return super.handleResponse(message, peerConnection, sign);
        }
        
        public FuturePeerConnection peerConnection() {
            return futurePeerConnection;
        }
    }
}
