package net.tomp2p.relay.android.gcm;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RPC is dedicated to send messages over Google Cloud Messaging. It's only used if
 * {@link RelayType#ANDROID} devices are relayed in the network. The GCM server authentication keys should not
 * be distributed in the whole network (because of privacy) and thus, this RPC acts as the key-holder and
 * server to send GCM messages. Relayed Android devices need to provide the {@link PeerAddress} of one or
 * multiple GCMSenders of this kind.
 * 
 * @author Nico Rutishauser
 * 
 */
public class GCMSenderRPC extends DispatchHandler implements IGCMSender {

	private static final Logger LOG = LoggerFactory.getLogger(GCMSenderRPC.class);

	private final Sender sender;
	private final int retries;
	private final ScheduledExecutorService executor;

	public GCMSenderRPC(final Peer peer, String authenticationKey, int retries) {
		super(peer.peerBean(), peer.connectionBean());
		this.sender = new Sender(authenticationKey);
		this.retries = retries;
		this.executor = peer.connectionBean().timer();
		register(RPC.Commands.GCM.getNr());
	}

	/**
	 * This method is called from the {@link Dispatcher} and handles GCM requests
	 * 
	 * REQUEST_1 = forward the GCM request to the mobile device.<br />
	 * 
	 * @param message
	 * @param peerConnection
	 * @param sign
	 * @param responder
	 */
	@Override
	public void handleResponse(final Message message, final PeerConnection peerConnection, final boolean sign,
			final Responder responder) throws Exception {
		LOG.warn("Received GCM message {}", message);
		if (message.type() == Message.Type.REQUEST_1 && message.command() == RPC.Commands.GCM.getNr()) {
			// the message reached the relay peer
			LOG.debug("Forwarding tickle message over GCM");
			handleGCMForward(message, responder);
		} else {
			LOG.warn("Received invalid GCM message {}", message);
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	private void handleGCMForward(final Message message, final Responder responder) {
		if (message.bufferList().isEmpty()) {
			LOG.error("GCM message does not contain the registrationID");
			responder.response(createResponseMessage(message, Type.EXCEPTION));
			return;
		}

		String registrationId = RelayUtils.decodeString(message.buffer(0));
		if(registrationId == null || registrationId.isEmpty()) {
			LOG.error("RegistrationID of device cannot be read from message");
			responder.response(createResponseMessage(message, Type.EXCEPTION));
			return;
		}
		
		FutureGCM futureGCM = new FutureGCM(registrationId, message.sender().peerId(), message.recipient());
		send(futureGCM);
		futureGCM.addListener(new BaseFutureAdapter<FutureGCM>() {

			@Override
			public void operationComplete(FutureGCM future) throws Exception {
				if (future.isSuccess()) {
					LOG.debug("Successfully sent message over GCM");
					responder.response(createResponseMessage(message, Type.OK));
				} else {
					LOG.debug("Could not send message over GCM");
					responder.response(createResponseMessage(message, Type.EXCEPTION));
				}
			}
		});
	}

	@Override
	public void send(final FutureGCM futureGCM) {
		// the collapse key is the relay's peerId
		final String registrationId = futureGCM.registrationId();
		final com.google.android.gcm.server.Message tickleMessage = new com.google.android.gcm.server.Message.Builder()
				.collapseKey(futureGCM.senderId().toString()).delayWhileIdle(false).build();

		// start in a separate thread since the sender is blocking
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					LOG.debug("Send GCM message to the device {}", registrationId);
					Result result = sender.send(tickleMessage, registrationId, retries);
					if (result.getMessageId() == null) {
						LOG.error("Could not send the tickle messge. Reason: {}", result.getErrorCodeName());
						futureGCM.failed("Cannot send message over GCM. Reason: " + result.getErrorCodeName());
					} else {
						LOG.debug("Successfully sent the message over GCM");
						futureGCM.done();
						if (result.getCanonicalRegistrationId() != null) {
							LOG.debug("Update the registration id {} to canonical name {}", registrationId,
									result.getCanonicalRegistrationId());
							// TODO update the registration ID
							// registrationId = result.getCanonicalRegistrationId();
						}
					}
				} catch (IOException e) {
					LOG.error("Cannot send tickle message to device {}", registrationId, e);
					futureGCM.failed(e);
				}
			}
		}, "Send-GCM-Tickle-Message");
	}
}
