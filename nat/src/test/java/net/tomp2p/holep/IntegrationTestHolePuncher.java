package net.tomp2p.holep;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.holep.strategy.NonPreservingSequentialStrategy;
import net.tomp2p.message.Message;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayConfig;
import net.tomp2p.relay.UtilsNAT;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.RPC;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class IntegrationTestHolePuncher extends AbstractTestHoleP {

	

	@Test
	public void testHolePunchPortPreserving() {
		System.err.println("PortPreserving() start!");
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
		System.out.println("DONE.");
		shutdown();
	}

	@Test
	public void testRelayFallback() {
		((HolePInitiatorImpl) unreachable1.peerBean().holePunchInitiator()).testCase(true);
		;
		System.err.println("testRelayFallback() start!");
		doTest();
	}
}
