package net.tomp2p.holep.strategy;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;

public interface HolePStrategy {
	
	// these values will never change
	public static final boolean BROADCAST_VALUE = false;
	public static final boolean FIRE_AND_FORGET_VALUE = false;

	public FutureDone<Message> initiateHolePunch(final FutureDone<Message> mainFutureDone, final FutureResponse originalFutureResponse);

	public FutureDone<Message> replyHolePunch();

	public void tryConnect() throws Exception;
}
