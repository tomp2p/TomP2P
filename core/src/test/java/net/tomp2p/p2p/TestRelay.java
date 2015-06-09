package net.tomp2p.p2p;

import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRelay {
	
	private static final Logger LOG = LoggerFactory.getLogger(TestRelay.class);
	private final static Random rnd = new Random(42L);
	
	@Test 
	public void testLoop() throws Exception {
		for(int i=0;i<100;i++) {
			 testPeerConnection();
		}
	}

	@Test
	public void testPeerConnection() throws Exception {
		Peer master = null;
		Peer slave = null;
		try {
			master = Utils2.createNodes(1, rnd, 4001, null, false)[0];
			slave = Utils2.createNodes(1, rnd, 4002, null, false)[0];
			System.err.println("master is " + master.peerAddress());
			System.err.println("slave is " + slave.peerAddress());

			FuturePeerConnection pcMaster = master.createPeerConnection(slave
					.peerAddress());
			MyDirectDataRPC myDirectDataRPC = new MyDirectDataRPC(
					slave.peerBean(), slave.connectionBean());
			slave.directDataRPC(myDirectDataRPC);

			slave.objectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request)
						throws Exception {
					return "yoo!";
				}
			});

			master.objectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request)
						throws Exception {
					return "world!";
				}
			});

			FutureDirect futureResponse = master.sendDirect(pcMaster)
					.object("test").start().awaitUninterruptibly();
			
			Assert.assertEquals("yoo!", futureResponse.object());

			FuturePeerConnection pcSlave = myDirectDataRPC.peerConnection();

			futureResponse = slave.sendDirect(pcSlave).object("hello").start()
					.awaitUninterruptibly();
			System.err.println(futureResponse.failedReason());
			Assert.assertEquals("world!", futureResponse.object());

			//Thread.sleep(1000);
			pcSlave.close();
			pcMaster.close();
			System.err.println("done");

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
		public void register(Number160 onBehalfOf, final int... names) {
		   	LOG.warn("registering {} for {} with {}", peerBean().serverPeerAddress().peerId(), onBehalfOf, names);
		    super.register(onBehalfOf, names);
		}

		@Override
		public void handleResponse(Message message,
				PeerConnection peerConnection, boolean sign, Responder responder)
				throws Exception {
			futurePeerConnection = new FuturePeerConnection(message.sender());
			futurePeerConnection.done(peerConnection);
			LOG.warn("handling response, object reply is {}", hasObjectDataReply());
			super.handleResponse(message, peerConnection, sign, responder);
		}

		public FuturePeerConnection peerConnection() {
			return futurePeerConnection;
		}
	}
}
