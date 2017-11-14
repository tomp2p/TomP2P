package net.tomp2p.rpc;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import net.sctp4nat.connection.SctpUtils;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.Sctp;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ChannelServer;
import net.tomp2p.connection.ClientChannel;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Pair;

public class TestDirectData {
	 @Rule
	    public TestRule watcher = new TestWatcher() {
		   protected void starting(Description description) {
	          System.out.println("Starting test: " + description.getMethodName());
	       }
	    };
	    
	    @Test
	    public void testData() throws Exception {
	    	Sctp.init();
	    	Peer sender = null;
	        Peer recv1 = null;
	        ChannelServer.resetCounters();
	        ChannelCreator cc = null;
	        try {
	            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).enableMaintenance(false).ports(8888).start();
	            DirectDataRPC handshake = new DirectDataRPC(sender.peerBean(), sender.connectionBean());
	            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).enableMaintenance(false).ports(7777).start();
	            new DirectDataRPC(recv1.peerBean(), recv1.connectionBean());
	            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1);
	            fcc.awaitUninterruptibly();
	            cc = fcc.channelCreator();
	            SendDirectBuilder s = new SendDirectBuilder(sender, recv1.peerAddress());
	            FutureDone<SctpChannelFacade> fr = handshake.send(recv1.peerAddress(), s, cc);
	            fr.awaitUninterruptibly();
	            Assert.assertEquals(true, fr.isSuccess());
	           
	        } finally {
	            if (cc != null) {
	                cc.shutdown();
	            }
	            if (sender != null) {
	                sender.shutdown().await();
	            }
	            if (recv1 != null) {
	                recv1.shutdown().await();
	            }
	        }
	    }
}
