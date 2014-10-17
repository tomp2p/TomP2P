package net.tomp2p.p2p;



import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;

public class TestDirect {
	@Test
	public void testDirectMessage1() throws Exception {
		Peer sender = null;
		Peer recv1 = null;
		try {
			Bindings b = new Bindings();
			sender = new PeerBuilder(new Number160("0x50")).bindings(b).ports(2424).start();
			recv1 = new PeerBuilder(new Number160("0x20")).bindings(b).ports(8088).start();
			recv1.objectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					System.err.println(sender.inetAddress());
					return "yes";
				}
			});
			
			FutureDirect fd = sender.sendDirect(recv1.peerAddress()).object("test").start().awaitUninterruptibly();
			Assert.assertTrue(fd.isSuccess());
			
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
