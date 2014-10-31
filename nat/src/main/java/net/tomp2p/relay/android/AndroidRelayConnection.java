package net.tomp2p.relay.android;

import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When an Android device is behind a NAT, this class Holds a connection to one relay. It has
 * additional capabilities like retrieving the buffer.
 * 
 * @author Nico Rutishauser
 *
 */
public class AndroidRelayConnection extends BaseRelayConnection {

	private static final Logger LOG = LoggerFactory.getLogger(AndroidRelayConnection.class);
	/**
	 * The maximum number of attempts to reach the relay peer. If the counter exceeds this limit, the relay is
	 * declared as unreachable
	 */
	private static final int MAX_FAIL_COUNT = 5;

	private final DispatchHandler dispatchHandler;
	private final Peer peer;
	private final ConnectionConfiguration config;
	private final GCMServerCredentials gcmServerCredentials;
	private int reachRelayFailCounter = 0;
	private final BufferedMessageHandler bufferedMessageHandler;
	private final AtomicBoolean shutdown;

	public AndroidRelayConnection(PeerAddress relayAddress, DispatchHandler dispatchHandler, Peer peer,
			ConnectionConfiguration config, GCMServerCredentials gcmServerCredentials) {
		super(relayAddress);
		this.dispatchHandler = dispatchHandler;
		this.peer = peer;
		this.config = config;
		this.gcmServerCredentials = gcmServerCredentials;
		this.bufferedMessageHandler = new BufferedMessageHandler(peer, config);
		this.shutdown = new AtomicBoolean(false);
	}

	@Override
	public FutureResponse sendToRelay(Message message) {
		if(shutdown.get()) {
			return new FutureResponse(message).failed("Relay connection is already shut down");
		}
		
		// send it over a newly opened connection
		return RelayUtils.connectAndSend(peer, message, config);
	}

	/**
	 * Get the buffer from the relay. This method should be called as soon as the device receives the tickle
	 * message from the relay over GCM.
	 * 
	 * @return when the buffer request is done
	 */
	public FutureDone<Void> sendBufferRequest() {
		if(shutdown.get()) {
			return new FutureDone<Void>().failed("Relay connection is already shut down");
		}
		
		LOG.debug("Sending buffer request to relay {}", relayAddress());
		final FutureDone<Void> futureDone = new FutureDone<Void>();

		Message message = dispatchHandler.createMessage(relayAddress(), Commands.RELAY.getNr(), Type.REQUEST_4);
		// close the connection after this message
		message.keepAlive(false);

		FutureResponse response = sendToRelay(message);
		response.addListener(new BaseFutureAdapter<FutureResponse>() {
			@Override
			public void operationComplete(FutureResponse futureResponse) throws Exception {
				if (futureResponse.isSuccess()) {
					// reset the fail counter
					reachRelayFailCounter = 0;
					
					LOG.debug("Successfully got the buffer from relay {}", relayAddress());
					bufferedMessageHandler.handleBufferResponse(futureResponse.responseMessage(), futureDone);
				} else {
					LOG.error("Cannot get the buffer from relay {}. Reason: {}", relayAddress(), futureResponse.failedReason());
					futureDone.failed(futureResponse);
					failedToContactRelay();
				}
			}
		});

		return futureDone;
	}

	@Override
	public FutureDone<Void> shutdown() {
		shutdown.set(true);
		// else, nothing to do
		return new FutureDone<Void>().done();
	}
	
	private void failedToContactRelay() {
		LOG.warn("Failed to contact the relay peer. Increase the counter to detect long-term disconnections");
		reachRelayFailCounter++;
		if(reachRelayFailCounter > MAX_FAIL_COUNT) {
			LOG.error("The relay {} was not reachable for {} send attempts", relayAddress(), reachRelayFailCounter);
			notifyCloseListeners();
		}
	}

	@Override
	public void onMapUpdateFailed() {
		failedToContactRelay();
	}

	@Override
	public void onMapUpdateSuccess() {
		// reset the couter
		reachRelayFailCounter = 0;
	}

	public GCMServerCredentials gcmServerCredentials() {
		return gcmServerCredentials;
	}

}
