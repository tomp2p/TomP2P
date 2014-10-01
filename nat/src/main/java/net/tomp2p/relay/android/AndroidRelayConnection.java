package net.tomp2p.relay.android;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
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

	private final DispatchHandler dispatchHandler;
	private final Peer peer;
	private final ConnectionConfiguration config;
	private final GCMServerCredentials gcmServerCredentials;

	public AndroidRelayConnection(PeerAddress relayAddress, DispatchHandler dispatchHandler, Peer peer,
			ConnectionConfiguration config, GCMServerCredentials gcmServerCredentials) {
		super(relayAddress);
		this.dispatchHandler = dispatchHandler;
		this.peer = peer;
		this.config = config;
		this.gcmServerCredentials = gcmServerCredentials;
	}

	@Override
	public FutureResponse sendToRelay(Message message) {
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
		final FutureDone<Void> futureDone = new FutureDone<Void>();

		Message message = dispatchHandler.createMessage(relayAddress(), Commands.RELAY.getNr(), Type.REQUEST_4);
		// close the connection after this message
		message.keepAlive(false);

		FutureResponse response = sendToRelay(message);
		response.addListener(new BaseFutureAdapter<FutureResponse>() {
			@Override
			public void operationComplete(FutureResponse futureResponse) throws Exception {
				if (futureResponse.isSuccess()) {
					LOG.debug("Successfully go the buffer from relay {}", relayAddress());
					handleBufferResponse(futureResponse.responseMessage(), futureDone);
				} else {
					LOG.error("Cannot get the buffer from relay {}. Reason: ", relayAddress(), futureResponse.failedReason());
					futureDone.failed(futureResponse);
				}
			}
		});

		return futureDone;
	}

	private void handleBufferResponse(Message response, FutureDone<Void> futureDone) {
		for (Buffer buffer : response.bufferList()) {
			try {
				Message bufferedMessage = RelayUtils.decodeMessage(buffer, response.recipientSocket(),
						response.senderSocket());
				LOG.debug("Received buffered message {}", bufferedMessage);
				// TODO handle the buffered message
			} catch (Exception e) {
				// continue to process the buffers anyway
				LOG.error("Cannot decode the buffer {}", buffer, e);
			}
		}
	}

	@Override
	public FutureDone<Void> shutdown() {
		// TODO Auto-generated method stub
		return new FutureDone<Void>().done();
	}

	@Override
	public void onMapUpdateFailed() {
		// TODO Auto-generated method stub
	}

	@Override
	public void onMapUpdateSuccess() {
		// TODO Auto-generated method stub
	}

	public GCMServerCredentials gcmServerCredentials() {
		return gcmServerCredentials;
	}

}
