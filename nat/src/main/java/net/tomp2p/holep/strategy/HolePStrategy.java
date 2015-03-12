package net.tomp2p.holep.strategy;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;

public interface HolePStrategy {

	public FutureDone<Message> initiateHolePunch(final FutureDone<Message> mainFutureDone, final FutureResponse originalFutureResponse);

	public FutureDone<Message> replyHolePunch();

	public void tryConnect() throws Exception;
}
