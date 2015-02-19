package net.tomp2p.holep;

import io.netty.channel.SimpleChannelInboundHandler;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;

public class NonPreservingSequentialStrategy extends AbstractHolePuncher {

	public NonPreservingSequentialStrategy(Peer peer, int numberOfHoles, int idleUDPSeconds, Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	@Override
	protected SimpleChannelInboundHandler<Message> createAfterHolePHandler(FutureDone<Message> futureDone) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FutureDone<Message> initiateHolePunch(FutureDone<Message> mainFutureDone,
			ChannelCreator originalChannelCreator, FutureResponse originalFutureResponse, NATType natType) {
		// TODO Auto-generated method stub
		return null;
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

}
