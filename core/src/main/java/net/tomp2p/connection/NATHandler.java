package net.tomp2p.connection;

/**
 * @author Jonas Wagner
 */
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;

/**
 * This interface makes sure that a hole punch procedure can be started from the {@link Sender} class.
 *
 */
public interface NATHandler {

	/**
	 * This method will trigger the implementing class to create a new object and trigger the hole punch
	 * procedure to start by executing the initHolePunch method.
	 * 
	 * @param idleUDPSeconds
	 * @param futureResponse
	 * @param originalMessage
	 * @return futureDone
	 */
	public FutureDone<Message> handleHolePunch(final int idleUDPSeconds, final FutureResponse futureResponse, final Message originalMessage);
        public List<FutureResponse> handleRcon(final Dispatcher dispatcher, final Message message, 
            final FutureResponse futureResponse, int idleUDPMillis,
            ScheduledExecutorService executorService);
}
