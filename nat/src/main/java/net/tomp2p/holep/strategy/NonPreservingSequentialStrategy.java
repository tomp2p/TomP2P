package net.tomp2p.holep.strategy;

import java.util.List;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonPreservingSequentialStrategy extends AbstractHolePuncher {

	private static final NATType NAT_TYPE = NATType.NON_PRESERVING_SEQUENTIAL;
	private static final Logger LOG = LoggerFactory.getLogger(PortPreservingStrategy.class);

	public NonPreservingSequentialStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	@Override
	protected void prepareInitiatingPeerPorts(final Message holePMessage, final FutureDone<Message> initMessageFutureDone) {
		// signal the other peer what type of NAT we are using
		holePMessage.longValue(NAT_TYPE.ordinal());

		FutureChannelCreator fcc1 = peer.connectionBean().reservation().create(1, 0);
		fcc1.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					FutureDone<List<PeerSocketAddress>> fDone = peer.pingRPC().pingNATType(holePMessage.recipient(),
							future.channelCreator(), new DefaultConnectionConfiguration(), peer);
					fDone.addListener(new BaseFutureAdapter<FutureDone<List<PeerSocketAddress>>>() {
						@Override
						public void operationComplete(FutureDone<List<PeerSocketAddress>> future) throws Exception {
							if (future.isSuccess()) {
								List<PeerSocketAddress> addresses = future.object();
								// TODO jwa is this a good thing?
								int startingPort = addresses.get(0).udpPort() + 1; 
								for (int i = 0; i < channelFutures.size(); i++) {
									holePMessage.intValue(startingPort + i);
								}
								initMessageFutureDone.done(holePMessage);
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
	protected boolean checkReplyValues(Message msg, FutureDone<Message> futureDone) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FutureDone<Message> replyHolePunch() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void doPortMappings() {
		// TODO Auto-generated method stub

	}

}
