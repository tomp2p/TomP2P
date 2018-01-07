package net.tomp2p.rpc;

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.Sctp;
import net.sctp4nat.origin.SctpDataCallback;
import net.tomp2p.connection.ChannelTransceiver;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Triple;

public class TestDirectData {
	 @Rule
	    public TestRule watcher = new TestWatcher() {
		   protected void starting(Description description) {
	          System.out.println("Starting test: " + description.getMethodName());
	       }
	    };
	    
	    @Test
	    public void testData() throws Exception {
	    	Sctp.getInstance().init();
	    	Peer sender = null;
	        Peer recv1 = null;
	        ChannelTransceiver.resetCounters();
	        try {
	            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).enableMaintenance(false).port(9876).start();
	            DirectDataRPC direct = new DirectDataRPC(sender.peerBean(), sender.connectionBean());
	            PeerBuilder b = new PeerBuilder(new Number160("0x1234")).p2pId(55).enableMaintenance(false).port(1234);
	            /*b.sctpCallback(new SctpDataCallback() {
					@Override
					public void onSctpPacket(byte[] data, int sid, int ssn, int tsn, long ppid, int context, int flags,
							SctpChannelFacade so) {
						so.send(new byte[100], true, 0, 0);
					}
				});*/
	            recv1 = b.start();
	            
	            new DirectDataRPC(recv1.peerBean(), recv1.connectionBean());

	            SendDirectBuilder s = new SendDirectBuilder(sender, recv1.peerAddress());
	            Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> fr = direct.send(recv1.peerAddress(), s);
	            fr.first.awaitUninterruptibly();
	            Assert.assertEquals(true, fr.first.isSuccess());
	            //Thread.sleep(1000);
	            final CountDownLatch l = new CountDownLatch(1);
	            fr.second.addListener(new BaseFutureAdapter<FutureDone<SctpChannelFacade>>() {
					@Override
					public void operationComplete(FutureDone<SctpChannelFacade> future) throws Exception {
						if(future.isSuccess()) {
						future.object().setSctpDataCallback(new SctpDataCallback() {
							
							@Override
							public void onSctpPacket(byte[] data, int sid, int ssn, int tsn, long ppid, int context, int flags,
									SctpChannelFacade so) {
								System.err.println("got len: "+ data.length);
								if(data.length == 200) {
									l.countDown();
								}
							}
						});
						future.object().send(new byte[1000], true, 0, 0);
						}
					}
				});
	            
	            l.await();
	            
	        } finally {
	            if (sender != null) {
	                sender.shutdown().await();
	            }
	            if (recv1 != null) {
	                recv1.shutdown().await();
	            }
	        }
	    }
}
