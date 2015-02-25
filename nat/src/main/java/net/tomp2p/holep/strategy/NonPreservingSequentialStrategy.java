package net.tomp2p.holep.strategy;

import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.List;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.utils.Pair;

public class NonPreservingSequentialStrategy extends AbstractHolePuncherStrategy {

	private static final NATType NAT_TYPE = NATType.NON_PRESERVING_SEQUENTIAL;

	public NonPreservingSequentialStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	@Override
	protected void prepareInitiatingPeerPorts(final Message initMessage, final FutureDone<Message> initMessageFutureDone, final List<ChannelFuture> channelFutures) {
		// signal the other peer what type of NAT we are using
		initMessage.longValue(NAT_TYPE.ordinal());

		FutureChannelCreator fcc1 = peer.connectionBean().reservation().create(1, 0);
		fcc1.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					FutureDone<List<PeerSocketAddress>> fDone = peer.pingRPC().pingNATType(initMessage.recipient(),
							future.channelCreator(), new DefaultConnectionConfiguration(), peer);
					fDone.addListener(new BaseFutureAdapter<FutureDone<List<PeerSocketAddress>>>() {
						@Override
						public void operationComplete(FutureDone<List<PeerSocketAddress>> future2) throws Exception {
							if (future2.isSuccess()) {
								List<PeerSocketAddress> addresses = future2.object();
								// TODO jwa is this a good thing?
								int startingPort = addresses.get(1).udpPort() + 1; 
								for (int i = 0; i < channelFutures.size(); i++) {
									initMessage.intValue(startingPort + i);
									InetSocketAddress inetSocketAddress = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
									portMappings.add(new Pair<Integer, Integer>(startingPort + i, inetSocketAddress.getPort()));
								}
								initMessageFutureDone.done(initMessage);
							} else {
								initMessageFutureDone.failed("Ping failed!");
							}
						}
					});
				} else {
					initMessageFutureDone.failed("No ChannelFuture could be created!");
				}
			}
		});
	}

	@Override
	protected void prepareTargetPeerPorts(final Message replyMessage, final FutureDone<Message> replyMessageFuture2) {
		FutureChannelCreator fcc = peer.connectionBean().reservation().create(1, 0);
		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {

			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					FutureDone<List<PeerSocketAddress>> fDone = peer.pingRPC().pingNATType(replyMessage.recipient(),
							future.channelCreator(), new DefaultConnectionConfiguration(), peer);
					fDone.addListener(new BaseFutureAdapter<FutureDone<List<PeerSocketAddress>>>() {
						@Override
						public void operationComplete(FutureDone<List<PeerSocketAddress>> future) throws Exception {
							if (future.isSuccess()) {
								List<PeerSocketAddress> addresses = future.object();
								// TODO jwa is this a good thing?
								int startingPort = addresses.get(0).udpPort() + 1; 
								for (int i = 0; i < channelFutures.size(); i++) {
									portMappings.add(new Pair<Integer, Integer>(originalMessage.intList().get(i), startingPort + i));
									replyMessage.intValue((int) originalMessage.intList().get(i));
									replyMessage.intValue(startingPort + i);
								}
								replyMessageFuture2.done(replyMessage);
							} else {
								replyMessageFuture2.failed("Ping failed!");
							}
						}
					});
				} else {
					replyMessageFuture2.failed("Could not create ChannelFuture!");
				}
			}
		});
	}
}
