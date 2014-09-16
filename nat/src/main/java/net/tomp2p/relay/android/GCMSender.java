package net.tomp2p.relay.android;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;

/**
 * Manages the mapping between a peer address and the registration id. The registration id is sent by the mobile device when the relay is set up.
 * @author Nico Rutishauser
 *
 */
public class GCMSender {

	private static final Logger LOG = LoggerFactory.getLogger(GCMSender.class);
	private final int retries = 5; // TODO make configurable if requested
	private final Sender sender;
	private final Map<PeerAddress, String> registrationMap;

	public GCMSender(String authToken) {
		this.sender = new Sender(authToken);
		this.registrationMap = new ConcurrentHashMap<PeerAddress, String>();
	}

	/**
	 * Tickle the device with the given registration id.
	 * 
	 * @param registrationId
	 */
	// TODO make asynchronous
	public void tickle(PeerAddress peerAddress) {
		String registrationId = registrationMap.get(peerAddress);
		if(registrationId == null) {
			LOG.error("Cannot tickle {} because it's not registered", peerAddress);
			return;
		}
		
		Message message = new Message.Builder().build();
		try {
			Result result = sender.send(message, registrationId, retries);
		} catch (IOException e) {
			LOG.error("Cannot send tickle message to device {}", registrationId);
		}
	}
	
	public void registerDevice(PeerAddress peerAddress, String registrationId) {
		LOG.debug("Adding a new registration ID to the map: '{}'", registrationId);
		registrationMap.put(peerAddress, registrationId);
	}
}
