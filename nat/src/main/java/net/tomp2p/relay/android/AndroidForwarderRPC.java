package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
	private final AtomicLong lastUpdate;

	// holds the current requests
	private List<FutureGCM> pendingRequests;

	public AndroidForwarderRPC(Peer peer, PeerAddress unreachablePeer, MessageBufferConfiguration bufferConfig,
			String authenticationToken, String registrationId, int mapUpdateIntervalS) {
		super(peer, unreachablePeer, RelayType.ANDROID);
		this.bufferConfig = bufferConfig;
		this.registrationId = registrationId;

		// stretch the update interval by factor 1.5 to be tolerant for slow messages
		this.mapUpdateIntervalMS = (int) (mapUpdateIntervalS * 1000 * 1.5);
		this.lastUpdate = new AtomicLong(System.currentTimeMillis());
		
		this.sender = new Sender(authenticationToken);
		this.pendingRequests = new ArrayList<FutureGCM>();
		
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
	 * @param list 
	 */
	private void sendTickleMessage(final FutureGCM futureGCM) {
		// the collapse key is the relay's peerId
		final com.google.android.gcm.server.Message tickleMessage = new com.google.android.gcm.server.Message.Builder()
				.collapseKey(relayPeerId().toString()).delayWhileIdle(false).build();

		// start in a separate thread since the sender is blocking
		connectionBean().timer().submit(new Runnable() {
			@Override
			public void run() {
				try {
					LOG.debug("Send GCM message to the device {}", registrationId);
					Result result = sender.send(tickleMessage, registrationId, bufferConfig.gcmSendRetries());
					if (result.getMessageId() == null) {
						LOG.error("Could not send the tickle messge. Reason: {}", result.getErrorCodeName());
						futureGCM.failed("Cannot send message over GCM. Reason: " + result.getErrorCodeName());
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
					futureGCM.failed(e);
				}
			}
		}, "Send-GCM-Tickle-Message");
	}

	@Override
	public void bufferFull(List<Message> messages) {
		final FutureGCM futureGCM = new FutureGCM(messages);
		boolean sendTickle = pendingRequests.isEmpty();
		pendingRequests.add(futureGCM);

		if (sendTickle) {
			sendTickleMessage(futureGCM);
		} else {
			LOG.debug("Another tickle message is already on the way to the mobile device");
		}

		// watch the pending request and if it takes too long, answer all buffered messages with an error
		connectionBean().timer().schedule(new Runnable() {
			@Override
			public void run() {
				// remove it, not that they are denied and still delivered to the device
				pendingRequests.remove(futureGCM);
				for (Message message : futureGCM.buffer()) {
					// TODO deny the pending request because it took too long
				}
			}
		}, mapUpdateIntervalMS, TimeUnit.MILLISECONDS);
	}

	/**
	 * Retrieves the messages that are ready to send. Ready to send means that they have been buffered and the
	 * Android device has already been notified.
	 * 
	 * @return the buffer containing all buffered messages
	 */
	public Buffer collectBufferedMessages() {
		// the mobile device seems to be alive
		lastUpdate.set(System.currentTimeMillis());

		List<Message> messages = new ArrayList<Message>();
		for (FutureGCM futureGCM : pendingRequests) {
			messages.addAll(futureGCM.buffer());
			futureGCM.done();
		}
		pendingRequests.clear();
		
		ByteBuf byteBuffer = RelayUtils.composeMessageBuffer(messages, connectionBean().channelServer()
				.channelServerConfiguration().signatureFactory());
		return new Buffer(byteBuffer);
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
			LOG.warn("Device {} did not send any messages for a long time", registrationId);
			return false;
		}
	}
}
