package net.tomp2p.relay.android;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayClient;
import net.tomp2p.relay.RelayClientConfig;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AndroidRelayClientConfig extends RelayClientConfig {

	private final static Logger LOG = LoggerFactory.getLogger(AndroidRelayClientConfig.class);

	private final String registrationId;
	private Collection<PeerAddress> gcmServers;
	private final AtomicBoolean gcmServersChanged;

	/**
	 * Creates an Android relay configuration. The messages at the relayed peer are not buffered.
	 * 
	 * @param registrationId the Google Cloud Messaging registration ID. This can be obtained on an Android
	 *            device by providing the correct senderID. The registration ID is unique for each device for
	 *            each senderID.
	 */
	public AndroidRelayClientConfig(String registrationId) {
		super(RelayType.ANDROID, 60, 120, 2);
		
		assert registrationId != null;
		this.registrationId = registrationId;
		this.gcmServers = Collections.emptySet();
		this.gcmServersChanged = new AtomicBoolean(false);
	}

	/**
	 * <strong>Only used for {@link RelayClientConfig#ANDROID}</strong><br>
	 * 
	 * @return the GCM registration ID. If this peer is an unreachable Android device, this value needs to be
	 *         provided.
	 */
	public String registrationId() {
		return registrationId;
	}
	
	/**
	 * @return a collection of known GCM servers which are known to be able to send GCM messages. A GCM server
	 *         can
	 *         be configured by setting {@link PeerBuilderNAT#gcmAuthenticationKey(String)}.
	 */
	public Collection<PeerAddress> gcmServers() {
		return gcmServers;
	}

	/**
	 * Defines well-known peers that have the ability to send messages over Google Cloud Messaging. If an
	 * empty list or null is provided, the relays try to send it by themselves or deny the relay connection.
	 * 
	 * @param gcmServers a set of peers that can send GCM messages
	 * @return this instance
	 */
	public RelayClientConfig gcmServers(Set<PeerAddress> gcmServers) {
		if (gcmServers == null) {
			this.gcmServers = Collections.emptySet();
		} else {
			this.gcmServers = gcmServers;
			this.gcmServersChanged.set(true);
		}
		return this;
	}

	@Override
	public void prepareSetupMessage(Message message) {
		// add the registration ID, the GCM authentication key and the map update interval
		message.buffer(RelayUtils.encodeString(registrationId));
		message.intValue(peerMapUpdateInterval());
		
		if(gcmServers != null && !gcmServers.isEmpty()) {
			// provide gcm servers at startup, later they will be updated using the map update task
			message.neighborsSet(new NeighborSet(-1, gcmServers));
		}
	}

	@Override
	public void prepareMapUpdateMessage(Message message) {
		// if GCM servers changed, send them again to the relay
		if(gcmServersChanged.get()) {
			LOG.debug("Sending updated GCM server list as well");
			message.neighborsSet(new NeighborSet(-1, gcmServers));
			gcmServersChanged.set(false);
		}
	}

	@Override
	public BaseRelayClient createClient(PeerConnection connection, Peer peer) {
		return new AndroidRelayClient(connection.remotePeer(), peer);
	}
}
