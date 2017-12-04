package net.tomp2p.holep.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.connection.SctpDefaultStreamConfig;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.SctpDataCallback;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;

public class UnreachablePeer extends AbstractPeer {

	private static final Logger LOG = LoggerFactory.getLogger(UnreachablePeer.class);

	protected final Peer peer;

	public UnreachablePeer(InetSocketAddress local) throws IOException {
		super(local);

		LOG.debug("start creating unreachablePeer...");
		peer = new PeerBuilder(peerId).start();
		new PeerBuilderNAT(peer).start();
		LOG.debug("masterpeer created! with peerAddress:" + peer.peerAddress().toString());

		LOG.debug("now starting bootstrap to masterPeer with peerAddress:" + masterPeerAddress.toString() + "...");
		FutureBootstrap future = peer.bootstrap().inetAddress(local.getAddress()).ports(9899)
				.bootstrapTo(Arrays.asList(masterPeerAddress)).start();
		future.addListener(new BaseFutureListener<FutureBootstrap>() {

			@Override
			public void operationComplete(FutureBootstrap future) throws Exception {
				if (future.isSuccess()) {
					LOG.debug("Bootstrap success!");

					// TODO jwa: add object data reply
					peer.sctpDataCallback(new SctpDataCallback() {
						
						@Override
						public void onSctpPacket(byte[] data, int sid, int ssn, int tsn, long ppid, int context, int flags,
								SctpChannelFacade facade) {
							System.out.println("I WAS HERE");
							System.out.println("got data: " + new String(data, StandardCharsets.UTF_8));
							facade.send(data, new SctpDefaultStreamConfig());
						}
					});
					
//					peer.sendDirect(recipientAddress)
				} else {
					LOG.error("Could not bootstrap masterpeer. Now closing program!");
					System.exit(1);
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				LOG.error("Could not bootstrap masterpeer. Now closing program!");
				System.exit(1);
			}
		});
	}

}
