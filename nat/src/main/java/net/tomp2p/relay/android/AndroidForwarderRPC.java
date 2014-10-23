package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

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

	private final AndroidRelayConfiguration config;
	private final Sender sender;
	private String registrationId;
	private final int mapUpdateIntervalMS;
	private final MessageBuffer buffer;
	private final List<ByteBuf> readyToSend;
	private final AtomicLong lastUpdate;

	public AndroidForwarderRPC(Peer peer, PeerAddress unreachablePeer, AndroidRelayConfiguration config,
			String authenticationToken, String registrationId, int mapUpdateIntervalS) {
		super(peer, unreachablePeer, RelayType.ANDROID);
		this.config = config;
		this.registrationId = registrationId;

		// stretch the update interval by factor 1.5 to be tolerant for slow messages
		this.mapUpdateIntervalMS = (int) (mapUpdateIntervalS * 1000 * 1.5);
		this.lastUpdate = new AtomicLong(System.currentTimeMillis());

		sender = new Sender(authenticationToken);
		readyToSend = Collections.synchronizedList(new ArrayList<ByteBuf>());

		buffer = new MessageBuffer(config.bufferCountLimit(), config.bufferSizeLimit(), config.bufferAgeLimit());
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
	public FutureDone<Void> sendTickleMessage() {
		// the collapse key is the relay's peerId
		final com.google.android.gcm.server.Message tickleMessage = new com.google.android.gcm.server.Message.Builder()
				.collapseKey(relayPeerId().toString()).build();
		final FutureDone<Void> future = new FutureDone<Void>();
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					LOG.debug("Send GCM message to the device {}", registrationId);
					Result result = sender.send(tickleMessage, registrationId, config.gcmSendRetries());
					if (result.getMessageId() == null) {
						LOG.error("Could not send the tickle messge. Reason: {}", result.getErrorCodeName());
						future.failed("Cannot send message over GCM. Reason: " + result.getErrorCodeName());
					} else if (result.getCanonicalRegistrationId() != null) {
						LOG.debug("Update the registration id {} to canonical name {}", registrationId,
								result.getCanonicalRegistrationId());
						registrationId = result.getCanonicalRegistrationId();
						future.done();
					} else {
						LOG.debug("Successfully sent the message over GCM");
						future.done();
					}
				} catch (IOException e) {
					LOG.error("Cannot send tickle message to device {}", registrationId, e);
					future.failed(e);
				}
			}
		}, "Send-GCM-Tickle-Message").start();

		return future;
	}

	@Override
	public void bufferFull(ByteBuf messageBuffer) {
		synchronized (readyToSend) {
			readyToSend.add(messageBuffer);
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
		ByteBuf buffer = Unpooled.buffer();
		synchronized (readyToSend) {
			for (ByteBuf rts : readyToSend) {
				buffer.writeBytes(rts);
				rts.release();
			}

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