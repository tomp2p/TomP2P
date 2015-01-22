package net.tomp2p.relay.android;

import java.util.Collection;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayServer;
import net.tomp2p.relay.RelayServerConfig;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.relay.android.gcm.GCMSenderRPC;
import net.tomp2p.relay.android.gcm.IGCMSender;
import net.tomp2p.relay.android.gcm.RemoteGCMSender;
import net.tomp2p.relay.buffer.MessageBufferConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AndroidRelayServerConfig extends RelayServerConfig {

	private static final Logger LOG = LoggerFactory.getLogger(AndroidRelayServerConfig.class);
	private static final int DEFAULT_GCM_RETRIES = 5;

	// can be null in case this peer cannot send GCM messages. But then, the mobile device must provide peers
	// with GCM ability.
	private final IGCMSender gcmSender;
	private final MessageBufferConfiguration bufferConfig;

	/**
	 * Creates an Android relay server configuration that is able to send GCM messages itself
	 * 
	 * @param peer the peer
	 * @param config the connection configuration
	 * @param gcmAuthenticationKey the api key / authentication token for Google Cloud Messaging. The key
	 *            can be obtained through Google's developer console. The key should be kept secret.
	 * @param gcmSendRetries the number of retries sending a GCM message
	 * @param bufferConfig the buffer behavior before sending a GCM message to the Android device
	 */
	public AndroidRelayServerConfig(Peer peer, String gcmAuthenticationKey, MessageBufferConfiguration bufferConfig) {
		this(peer, new GCMSenderRPC(peer, gcmAuthenticationKey, DEFAULT_GCM_RETRIES), bufferConfig);
	}

	public AndroidRelayServerConfig(Peer peer, IGCMSender gcmSender, MessageBufferConfiguration bufferConfig) {
		super(peer);
		this.gcmSender = gcmSender;
		this.bufferConfig = bufferConfig;
	}

	@Override
	public BaseRelayServer createServer(Message message, PeerConnection peerConnection, Responder responder) {
		/** The registration ID */
		if (message.bufferList().size() < 1) {
			LOG.error("Device {} did not send any GCM registration id", peerConnection.remotePeer());
			responder.response(createResponse(message, Type.DENIED));
			return null;
		}

		String registrationId = RelayUtils.decodeString(message.buffer(0));
		if (registrationId == null) {
			LOG.error("Cannot decode the registrationID from the message");
			responder.response(createResponse(message, Type.DENIED));
			return null;
		}

		/** Update interval */
		Integer mapUpdateInterval = message.intAt(1);
		if (mapUpdateInterval == null) {
			LOG.error("Android device did not send the peer map update interval.");
			responder.response(createResponse(message, Type.DENIED));
			return null;
		}

		/** GCM handing */
		IGCMSender sender = null;
		if (message.neighborsSetList().isEmpty()) {
			// no known GCM servers, check GCM ability of this peer
			if (gcmSender == null) {
				LOG.error("This relay is unable to serve unreachable Android devices because no GCM Authentication Key is configured");
				responder.response(createResponse(message, Type.DENIED));
				return null;
			} else {
				sender = gcmSender;
			}
		} else {
			// device sent well-known GCM servers to use
			Collection<PeerAddress> gcmServers = message.neighborsSet(0).neighbors();
			sender = new RemoteGCMSender(peer, gcmServers);
		}

		LOG.debug("Hello Android device! You'll be relayed over GCM. {}", message);
		AndroidRelayServer androidServer = new AndroidRelayServer(peer, peerConnection.remotePeer(), bufferConfig,
				registrationId, sender, mapUpdateInterval);
		responder.response(createResponse(message, Type.OK));
		return androidServer;
	}

}
