package net.tomp2p.holep.strategy.legacy;

import io.netty.channel.ChannelFuture;

import java.util.List;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;

/**
 * @since 30.03.2015
 * @author jonaswagner
 *
 */
public class NonPreservingSequentialStrategy extends AbstractHolePStrategy {

	public NonPreservingSequentialStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	@Override
	protected void doPortGuessingInitiatingPeer(final Message initMessage, final FutureDone<Message> initMessageFutureDone,
			final List<ChannelFuture> channelFutures) {
//		// signal the other peer what type of NAT we are using
//		final FutureChannelCreator fcc1 = peer.connectionBean().reservation().create(1, 0);
//		fcc1.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
//			@Override
//			public void operationComplete(final FutureChannelCreator future) throws Exception {
//				if (future.isSuccess()) {
//					final FutureDone<List<PeerSocketAddress>> fDone = peer.pingRPC().pingNATType(initMessage.recipient(),
//							future.channelCreator(), new DefaultConnectionConfiguration(), peer);
//					fDone.addListener(new BaseFutureAdapter<FutureDone<List<PeerSocketAddress>>>() {
//						@Override
//						public void operationComplete(final FutureDone<List<PeerSocketAddress>> future2) throws Exception {
//							if (future2.isSuccess()) {
//								final List<PeerSocketAddress> addresses = future2.object();
//								final int startingPort = addresses.get(1).udpPort() + 2;
//								guessPortsInitiatingPeer(initMessage, channelFutures, startingPort);
//								initMessageFutureDone.done(initMessage);
//							} else {
//								initMessageFutureDone.failed("Ping failed!");
//							}
//						}
//					});
//				} else {
//					initMessageFutureDone.failed("No ChannelFuture could be created!");
//				}
//			}
//		});
	}

//	private void guessPortsInitiatingPeer(final Message initMessage, final List<ChannelFuture> channelFutures, int startingPort)
//			throws IOException {
//		final List<Integer> portList = new ArrayList<Integer>(channelFutures.size());
//		for (int i = 0; i < channelFutures.size(); i++) {
//			final int guessedPort = startingPort + (2 * i);
//			final InetSocketAddress inetSocketAddress = (InetSocketAddress) channelFutures.get(i).channel().localAddress();
//			portMappings.add(new Pair<Integer, Integer>(guessedPort, inetSocketAddress.getPort()));
//			portList.add(guessedPort);
//		}
//		initMessage.intValue(portList.size());
//
//		// send all ports via Buffer
//		initMessage.buffer(encodePortList(portList));
//	}

	@Override
	protected void doPortGuessingTargetPeer(final Message replyMessage, final FutureDone<Message> replyMessageFuture2) {
//		final FutureChannelCreator fcc = peer.connectionBean().reservation().create(1, 0);
//		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
//
//			@Override
//			public void operationComplete(final FutureChannelCreator future) throws Exception {
//				if (future.isSuccess()) {
//					final FutureDone<List<PeerSocketAddress>> fDone = peer.pingRPC().pingNATType(replyMessage.recipient(),
//							future.channelCreator(), new DefaultConnectionConfiguration(), peer);
//					fDone.addListener(new BaseFutureAdapter<FutureDone<List<PeerSocketAddress>>>() {
//						@Override
//						public void operationComplete(final FutureDone<List<PeerSocketAddress>> future) throws Exception {
//							if (future.isSuccess()) {
//								final List<PeerSocketAddress> addresses = future.object();
//								final int startingPort = addresses.get(0).udpPort() + 2;
//								guessPortsTargetPeer(replyMessage, startingPort);
//								replyMessageFuture2.done(replyMessage);
//							} else {
//								replyMessageFuture2.failed("Ping failed!");
//							}
//						}
//					});
//				} else {
//					replyMessageFuture2.failed("Could not create ChannelFuture!");
//				}
//			}
//		});
	}
//
//	private void guessPortsTargetPeer(final Message replyMessage, int startingPort) throws ClassNotFoundException, IOException {
//		@SuppressWarnings("unchecked")
//		final List<Integer> remotePorts = (List<Integer>) Utils.decodeJavaObject(originalMessage.buffer(0).buffer());
//		final List<Integer> replyPorts = new ArrayList<Integer>(channelFutures.size() * 2);
//		for (int i = 0; i < channelFutures.size(); i++) {
//			final int guessedPort = startingPort + (2 * i);
//			portMappings.add(new Pair<Integer, Integer>(remotePorts.get(i), startingPort + i));
//			replyPorts.add(remotePorts.get(i));
//			replyPorts.add(guessedPort);
//		}
//		replyMessage.intValue(replyPorts.size());
//
//		// send all ports via Buffer
//		replyMessage.buffer(encodePortList(replyPorts));
//	}
}