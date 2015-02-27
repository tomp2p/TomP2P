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

	private final MessageBufferConfiguration bufferConfig;
	private final String gcmAuthenticationKey;
	private final int gcmRetries;
	protected IGCMSender gcmSender;

	/**
	 * Creates an Android relay server configuration that is able to send GCM messages itself
	 * 
	 * @param gcmAuthenticationKey the api key / authentication token for Google Cloud Messaging. The key
	 *            can be obtained through Google's developer console. The key should be kept secret.
	 * @param gcmRetries how many times the GCM message should be resent before giving up. Normal ranges are 1-5
	 * @param bufferConfig the buffer behavior before sending a GCM message to the Android device
	 */
	public AndroidRelayServerConfig(String gcmAuthenticationKey, int gcmRetries, MessageBufferConfiguration bufferConfig) {
		this.gcmAuthenticationKey = gcmAuthenticationKey;
		this.gcmRetries = gcmRetries;
		this.bufferConfig = bufferConfig;
	}

	/**
	 * Creates an Android relay server config that is <strong>not</strong> able to send GCM messages itself.
	 * Connecting unreachable peers need to provide at least one known other peer that has the ability to send
	 * GCM messages.
	 * 
	 * @param bufferConfig the buffer behavior before sending a GCM message to the Android device (over a
	 *            {@link RemoteGCMSender}).
	 */
	public AndroidRelayServerConfig(MessageBufferConfiguration bufferConfig) {
		this(null, 0, bufferConfig);
	}

	@Override
	public void start(Peer peer) {
		if (gcmAuthenticationKey != null) {
			gcmSender = new GCMSenderRPC(peer, gcmAuthenticationKey, gcmRetries);
			LOG.debug("GCM server started on {}", peer.peerAddress());
		}
	}

	@Override
	public BaseRelayServer createServer(Message message, PeerConnection peerConnection, Responder responder, Peer peer) {
		/** The registration ID */
		if (message.bufferList().size() < 1) {
			LOG.error("Device {} did not send any GCM registration id", peerConnection.remotePeer());
			responder.response(createResponse(message, Type.DENIED, peer.peerBean().serverPeerAddress()));
			return null;
		}

		String registrationId = RelayUtils.decodeString(message.buffer(0));
		if (registrationId == null) {
			LOG.error("Cannot decode the registrationID from the message");
			responder.response(createResponse(message, Type.DENIED, peer.peerBean().serverPeerAddress()));
			return null;
		}

		/** Update interval */
		Integer mapUpdateInterval = message.intAt(1);
		if (mapUpdateInterval == null) {
			LOG.error("Android device did not send the peer map update interval.");
			responder.response(createResponse(message, Type.DENIED, peer.peerBean().serverPeerAddress()));
			return null;
		}

		/** GCM handing */
		IGCMSender sender = null;
		if (message.neighborsSetList().isEmpty()) {
			// no known GCM servers, use GCM ability of this peer
			sender = gcmSender;
			if (sender == null) {
				LOG.error("This relay is unable to serve unreachable Android devices because no GCM Authentication Key is configured");
				responder.response(createResponse(message, Type.DENIED, peer.peerBean().serverPeerAddress()));
				return null;
			}
		} else {
			// device sent well-known GCM servers to use
			Collection<PeerAddress> gcmServers = message.neighborsSet(0).neighbors();
			sender = new RemoteGCMSender(peer, gcmServers);
		}

		LOG.debug("Hello Android device! You'll be relayed over GCM. {}", message);
		AndroidRelayServer androidServer = new AndroidRelayServer(peer, peerConnection.remotePeer(), bufferConfig,
				registrationId, sender, mapUpdateInterval);
		responder.response(createResponse(message, Type.OK, peer.peerBean().serverPeerAddress()));
		return androidServer;
	}
}
