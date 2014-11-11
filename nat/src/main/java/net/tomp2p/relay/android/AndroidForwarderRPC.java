package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collection;
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
import net.tomp2p.relay.android.gcm.FutureGCM;
import net.tomp2p.relay.android.gcm.IGCMSender;
import net.tomp2p.relay.android.gcm.RemoteGCMSender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the mapping between a peer address and the registration id. The registration id is sent by the
 * mobile device when the relay is set up.
 * 
 * @author Nico Rutishauser
 *
 */
public class AndroidForwarderRPC extends BaseRelayForwarderRPC implements MessageBufferListener<Message> {

	private static final Logger LOG = LoggerFactory.getLogger(AndroidForwarderRPC.class);

	private final String registrationId;
	private final IGCMSender sender;
	private final int mapUpdateIntervalMS;
	private final MessageBuffer<Message> buffer;
	private final AtomicLong lastUpdate;

	// holds the current requests
	private List<FutureGCM> pendingRequests;

	public AndroidForwarderRPC(Peer peer, PeerAddress unreachablePeer, MessageBufferConfiguration bufferConfig,
			String registrationId, IGCMSender sender, int mapUpdateIntervalS) {
		super(peer, unreachablePeer, RelayType.ANDROID);
		this.registrationId = registrationId;
		this.sender = sender;

		// stretch the update interval by factor 1.5 to be tolerant for slow messages
		this.mapUpdateIntervalMS = (int) (mapUpdateIntervalS * 1000 * 1.5);
		this.lastUpdate = new AtomicLong(System.currentTimeMillis());

		this.pendingRequests = Collections.synchronizedList(new ArrayList<FutureGCM>());

		buffer = new MessageBuffer<Message>(bufferConfig);
		addBufferListener(this);
	}

	public void addBufferListener(MessageBufferListener<Message> listener) {
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
			int messageSize = RelayUtils.getMessageSize(message, connectionBean().channelServer()
					.channelServerConfiguration().signatureFactory());
			buffer.addMessage(message, messageSize);
		} catch (Exception e) {
			LOG.error("Cannot encode the message", e);
			return futureDone.done(createResponseMessage(message, Type.EXCEPTION));
		}

		LOG.debug("Added message {} to buffer and returning a partially ok", message);
		return futureDone.done(response);
	}

	@Override
	public void bufferFull(List<Message> messages) {
		final FutureGCM futureGCM = new FutureGCM(messages, registrationId, relayPeerId());
		boolean sendTickle;
		synchronized (pendingRequests) {
			sendTickle = pendingRequests.isEmpty();
			pendingRequests.add(futureGCM);
		}

		if (sendTickle) {
			sender.send(futureGCM);
		} else {
			LOG.debug("Another tickle message is already on the way to the mobile device");
		}
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
		synchronized (pendingRequests) {
			for (FutureGCM futureGCM : pendingRequests) {
				messages.addAll(futureGCM.buffer());
				futureGCM.done();
			}
			pendingRequests.clear();
		}

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
	
	public void changeGCMServers(Collection<PeerAddress> gcmServers) {
		if(sender instanceof RemoteGCMSender) {
			RemoteGCMSender remoteGCMSender = (RemoteGCMSender) sender;
			remoteGCMSender.gcmServers(gcmServers);
			LOG.debug("Received update of the GCM servers");
		}
	}
}
