package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import net.tomp2p.futures.FutureDirect;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.UtilsNAT;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.ObjectDataReply;

import org.junit.Assert;
import org.junit.Test;

public class TestAndroidForwarderRPC {

	@Test
	public void testMockedForwarding() throws Exception {
		final Random rnd = new Random(42);
		final int nrOfNodes = 100;
		Peer master = null;
		Peer unreachablePeer = null;
		try {
			// setup test peers
			Peer[] peers = UtilsNAT.createNodes(nrOfNodes, rnd, 4001);
			master = peers[0];
			UtilsNAT.perfectRouting(peers);

			AndroidRelayConfiguration relayConfiguration = new AndroidRelayConfiguration();
			relayConfiguration.bufferCountLimit(1);
			for (Peer peer : peers) {
				new PeerBuilderNAT(peer).androidRelayConfiguration(relayConfiguration).start();
			}

			// setup relay
			unreachablePeer = new PeerBuilder(Number160.createHash(rnd.nextInt())).ports(13337).start();
			GCMServerCredentials gcmServerCredentials = new GCMServerCredentials();
			gcmServerCredentials.senderAuthenticationKey("abc").registrationId("cde").senderId(12345);
			final PeerNAT uNat = new PeerBuilderNAT(unreachablePeer).relayType(RelayType.ANDROID)
					.gcmServerCredentials(gcmServerCredentials).start();
			FutureRelayNAT startRelay = uNat.startRelay(peers[0].peerAddress()).awaitUninterruptibly();
			Assert.assertTrue(startRelay.isSuccess());

			// make sure every forwarder only has one of these listeners
			Set<AndroidForwarderRPC> mockedForwarders = new HashSet<AndroidForwarderRPC>();
			for (Peer peer : peers) {
				Map<Integer, DispatchHandler> handlers = peer.connectionBean().dispatcher()
						.searchHandlerMap(peer.peerID(), unreachablePeer.peerID());
				if (handlers == null) {
					continue;
				}

				for (Entry<Integer, DispatchHandler> entry : handlers.entrySet()) {
					if (entry.getValue() instanceof AndroidForwarderRPC && !mockedForwarders.contains(entry.getValue())) {
						AndroidForwarderRPC forwarderRPC = (AndroidForwarderRPC) entry.getValue();
						forwarderRPC.addMessageBufferListener(new MessageBufferListener() {

							@Override
							public void bufferFull(ByteBuf messageBuffer) {
								System.err.println("Caught sending message over GCM");
								for (BaseRelayConnection connection : uNat.currentRelays()) {
									if(connection instanceof AndroidRelayConnection) {
										AndroidRelayConnection androidConnection = (AndroidRelayConnection) connection;
										androidConnection.sendBufferRequest();
									}
								}
							}
						});
						mockedForwarders.add(forwarderRPC);
					}
				}
			}

			System.out.println("Send direct message to unreachable peer");
			final String request = "Hello ";
			final String response = "World!";

			unreachablePeer.objectDataReply(new ObjectDataReply() {
				public Object reply(PeerAddress sender, Object request) throws Exception {
					Assert.assertEquals(request.toString(), request);
					return response;
				}
			});

			FutureDirect fd = peers[42].sendDirect(unreachablePeer.peerAddress()).object(request).start()
					.awaitUninterruptibly();
			Assert.assertEquals(response, fd.object());

			// make sure we did receive it from the unreachable peer with id
			Assert.assertEquals(unreachablePeer.peerID(), fd.wrappedFuture().responseMessage().sender().peerId());
		} finally {
			if (unreachablePeer != null) {
				unreachablePeer.shutdown().await();
			}
			if (master != null) {
				master.shutdown().await();
			}
		}
	}
}
