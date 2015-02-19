package net.tomp2p.holep;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;

public interface HolePuncherStrategy {

	public FutureDone<Message> initiateHolePunch(final FutureDone<Message> mainFutureDone,
			final ChannelCreator originalChannelCreator, final FutureResponse originalFutureResponse,
			final NATType natType);
	
	public FutureDone<Message> replyHolePunch();
	
	public void tryConnect();
}
