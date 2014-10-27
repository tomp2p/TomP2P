package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayForwarderRPC;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;

/**
 * Manages the mapping between a peer address and the registration id. The registration id is sent by the
 * mobile device when the relay is set up.
 * 
 * @author Nico Rutishauser
 *
 */
public class AndroidForwarderRPC extends BaseRelayForwarderRPC implements MessageBufferListener {

	private static final Logger LOG = LoggerFactory.getLogger(AndroidForwarderRPC.class);

	private final MessageBufferConfiguration bufferConfig;
	private final Sender sender;
	private String registrationId;
	private final int mapUpdateIntervalMS;
	private final MessageBuffer buffer;
	private final List<Message> readyToSend;
	private final AtomicLong lastUpdate;

	// holds the current request. When completed, the android device requested the buffered messages
	private FutureDone<Void> pendingRequest;

	public AndroidForwarderRPC(Peer peer, PeerAddress unreachablePeer, MessageBufferConfiguration bufferConfig,
			String authenticationToken, String registrationId, int mapUpdateIntervalS) {
		super(peer, unreachablePeer, RelayType.ANDROID);
		this.bufferConfig = bufferConfig;
		this.registrationId = registrationId;

		// stretch the update interval by factor 1.5 to be tolerant for slow messages
		this.mapUpdateIntervalMS = (int) (mapUpdateIntervalS * 1000 * 1.5);
		this.lastUpdate = new AtomicLong(System.currentTimeMillis());

		sender = new Sender(authenticationToken);
		readyToSend = Collections.synchronizedList(new ArrayList<Message>());

		buffer = new MessageBuffer(bufferConfig.bufferCountLimit(), bufferConfig.bufferSizeLimit(),
				bufferConfig.bufferAgeLimit());
		addMessageBufferListener(this);
	}

	public void addMessageBufferListener(MessageBufferListener listener) {
		buffer.addListener(listener);
	}

	@Override
	public FutureDone<Message> forwardToUnreachable(Message message) {
		// create temporal OK message
		final FutureDone<Message> futureDone = new FutureDone<Message>();
		final Message response = createResponseMessage(message, Type.PARTIALLY_OK);
		response.recipient(message.sender());
		response.sender(unreachablePeerAddress());

		try {
			buffer.addMessage(message, connectionBean().channelServer().channelServerConfiguration().signatureFactory());
		} catch (Exception e) {
			LOG.error("Cannot encode the message", e);
			return futureDone.done(createResponseMessage(message, Type.EXCEPTION));
		}

		LOG.debug("Added message {} to buffer and returning a partially ok", message);
		return futureDone.done(response);
	}

	/**
	 * Tickle the device through Google Cloud Messaging
	 */
	public void sendTickleMessage() {
		if (pendingRequest == null || pendingRequest.isCompleted()) {
			// no current pending request or the last one is finished
			pendingRequest = new FutureDone<Void>();
		} else {
			LOG.debug("A GCM message is already sent but not answered yet. Skip to send another.");
			return;
		}

		// the collapse key is the relay's peerId
		final com.google.android.gcm.server.Message tickleMessage = new com.google.android.gcm.server.Message.Builder()
				.collapseKey(relayPeerId().toString()).delayWhileIdle(false).build();
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					LOG.debug("Send GCM message to the device {}", registrationId);
					Result result = sender.send(tickleMessage, registrationId, bufferConfig.gcmSendRetries());
					if (result.getMessageId() == null) {
						LOG.error("Could not send the tickle messge. Reason: {}", result.getErrorCodeName());
						pendingRequest.failed("Cannot send message over GCM. Reason: " + result.getErrorCodeName());
					} else {
						LOG.debug("Successfully sent the message over GCM");
						if (result.getCanonicalRegistrationId() != null) {
							LOG.debug("Update the registration id {} to canonical name {}", registrationId,
									result.getCanonicalRegistrationId());
							registrationId = result.getCanonicalRegistrationId();
						}
					}
				} catch (IOException e) {
					LOG.error("Cannot send tickle message to device {}", registrationId, e);
					pendingRequest.failed(e);
				}
			}
		}, "Send-GCM-Tickle-Message").start();
		
		// TODO watch the pending request and if it takes too long, answer all buffered messages with an error
	}

	@Override
	public void bufferFull(List<Message> messageBuffer) {
		synchronized (readyToSend) {
			readyToSend.addAll(messageBuffer);
		}
		sendTickleMessage();
	}

	/**
	 * Retrieves the messages that are ready to send. Ready to send means that they have been buffered and the
	 * Android device has already been notified.
	 * 
	 * @return the buffer containing all buffered messages
	 */
	public Buffer getBufferedMessages() {
		// finish the pending request
		pendingRequest.done();
		
		ByteBuf buffer;
		synchronized (readyToSend) {
			buffer = RelayUtils.composeMessageBuffer(readyToSend, connectionBean().channelServer().channelServerConfiguration().signatureFactory());
			readyToSend.clear();
		}

		lastUpdate.set(System.currentTimeMillis());
		return new Buffer(buffer);
	}

	@Override
	protected void peerMapUpdated() {
		// take this event as an indicator that the mobile device is online
		lastUpdate.set(System.currentTimeMillis());
	}

	@Override
	protected boolean isAlive() {
		// Check if the mobile device is still alive by checking its last update time.
		if (lastUpdate.get() + mapUpdateIntervalMS > System.currentTimeMillis()) {
			LOG.trace("Device {} seems to be alive", registrationId);
			return true;
		} else {
			LOG.warn("Device {} did not send the map update for a long time", registrationId);
			return false;
		}
	}
}
