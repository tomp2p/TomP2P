package net.tomp2p.holep;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;

public class IntegrationTestHolePuncher extends AbstractTestHoleP {

	@Test
	public void testHolePunchPortPreserving() {
		System.err.println("PortPreserving() start!");
		doTest();
	}

	@Test
	public void testRelayFallback() {
		((HolePInitiatorImpl) unreachable1.peerBean().holePunchInitiator()).testCase(true);
		System.err.println("testRelayFallback() start!");
		doTest();
	}
	
	private void doTest() {
		final String requestString = "This is a test String";
		final String replyString = "SUCCESS HIT";

		unreachable2.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (requestString.equals((String) request)) {
					Assert.assertEquals(requestString, request);
					System.err.println("received: " + (String) request);
				}
				return replyString;
			}
		});

		FutureDirect fd = unreachable1.sendDirect(unreachable2.peerAddress()).object(requestString).forceUDP(true).start();
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {

			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				Assert.assertTrue(future.isSuccess());
				Assert.assertEquals(replyString, (String) future.object());
			}
		});
		fd.awaitUninterruptibly();
		shutdown();
	}
}
