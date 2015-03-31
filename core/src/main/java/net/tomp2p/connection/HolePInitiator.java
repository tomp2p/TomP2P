package net.tomp2p.connection;

/**
 * @author Jonas Wagner
 */
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;

/**
 * This interface makes sure that a hole punch procedure can be started from the {@link Sender} class.
 *
 */
public interface HolePInitiator {

	/**
	 * This method will trigger the implementing class to create a new object of
	 * super type {@link AbstractHolePStrategy} and trigger the hole punch
	 * procedure to start by executing the initHolePunch method.
	 * 
	 * @param idleUDPSeconds
	 * @param futureResponse
	 * @param originalMessage
	 * @return futureDone
	 */
	public FutureDone<Message> handleHolePunch(final int idleUDPSeconds, final FutureResponse futureResponse, final Message originalMessage);

}
