package net.tomp2p.holep;

import static org.junit.Assert.*;

import java.io.IOException;

import net.tomp2p.futures.FutureDirect;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;

public class HolePStressTest extends AbstractTestHoleP {

	private static final int NUMBER_OF_MESSAGES = 1000;
	
//	@Test
//	public void test() throws ClassNotFoundException, IOException {
//		for (int i=0; i<NUMBER_OF_MESSAGES; i++) {
//			doTest(i);
////			System.out.println((i+1) + ". message sent");
//		}
//		shutdown();
//	}
//	
//	private void doTest(final int index) throws ClassNotFoundException, IOException {
//		final String requestString = "This is the test String #" + index;
//		final String replyString = "SUCCESS HIT";
//
//		unreachable2.objectDataReply(new ObjectDataReply() {
//			@Override
//			public Object reply(PeerAddress sender, Object request) throws Exception {
//				if (requestString.equals((String) request)) {
//					Assert.assertEquals(requestString, request);
//					System.err.println("received: " + (String) request);
//				}
//				return replyString;
//			}
//		});
//
//		FutureDirect fd = unreachable1.sendDirect(unreachable2.peerAddress()).object(requestString).forceUDP(true).start();
//	}

}
