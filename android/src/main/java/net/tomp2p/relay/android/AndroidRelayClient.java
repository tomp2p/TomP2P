package net.tomp2p.relay.android;

import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.relay.buffer.BufferedRelayClient;
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
public class AndroidRelayClient extends BufferedRelayClient {

	private static final Logger LOG = LoggerFactory.getLogger(AndroidRelayClient.class);
	/**
	 * The maximum number of attempts to reach the relay peer. If the counter exceeds this limit, the relay is
	 * declared as unreachable
	 */
	private static final int MAX_FAIL_COUNT = 5;

	private int reachRelayFailCounter = 0;
	private final AtomicBoolean shutdown;

	public AndroidRelayClient(PeerAddress relayAddress, Peer peer) {
		super(relayAddress, peer);
		this.shutdown = new AtomicBoolean(false);
	}

	@Override
	public FutureResponse sendToRelay(Message message) {
		if (shutdown.get()) {
			return new FutureResponse(message).failed("Relay connection is already shut down");
		}

		// send it over a newly opened connection
		return RelayUtils.connectAndSend(peer, message);
	}

	/**
	 * Get the buffer from the relay. This method should be called as soon as the device receives the tickle
	 * message from the relay over GCM.
	 * 
	 * @return when the buffer request is done
	 */
	public FutureDone<Void> sendBufferRequest() {
		if (shutdown.get()) {
			return new FutureDone<Void>().failed("Relay connection is already shut down");
		}

		LOG.debug("Sending buffer request to relay {}", relayAddress());
		final FutureDone<Void> futureDone = new FutureDone<Void>();

		Message message = new Message().recipient(relayAddress()).sender(peer.peerBean().serverPeerAddress())
				.command(Commands.RELAY.getNr()).type(Type.REQUEST_4).version(peer.connectionBean().p2pId())
				.keepAlive(false);

		FutureResponse response = sendToRelay(message);
		response.addListener(new BaseFutureAdapter<FutureResponse>() {
			@Override
			public void operationComplete(FutureResponse futureResponse) throws Exception {
				if (futureResponse.isSuccess()) {
					// reset the fail counter
					reachRelayFailCounter = 0;

					LOG.debug("Successfully got the buffer from relay {}", relayAddress());
					onReceiveMessageBuffer(futureResponse.responseMessage(), futureDone);
				} else {
					LOG.error("Cannot get the buffer from relay {}. Reason: {}", relayAddress(),
							futureResponse.failedReason());
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
		if (reachRelayFailCounter > MAX_FAIL_COUNT) {
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
}
