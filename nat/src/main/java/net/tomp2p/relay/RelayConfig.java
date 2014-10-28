package net.tomp2p.relay;

import net.tomp2p.relay.android.GCMServerCredentials;
import net.tomp2p.relay.android.MessageBufferConfiguration;

/**
 * Holds multiple relay types with their configuration
 * 
 * @author Nico Rutishauser
 *
 */
public class RelayConfig {

	private final RelayType type;
	private final GCMServerCredentials gcmServerCredentials;
	private final MessageBufferConfiguration bufferConfiguration;

	// configurable
	private int peerMapUpdateInterval;
	private int gcmSendRetries;

	/**
	 * Creates a TCP relay configuration
	 * 
	 * @return
	 */
	public static RelayConfig OpenTCP() {
		return new RelayConfig(RelayType.OPENTCP, 15, null, 0, null);
	}

	/**
	 * Creates an Android relay configuration. The messages at the relayed peer are not buffered.
	 * 
	 * @param gcmServerCredentials to be able to communicate over Google Cloud Messaging
	 * @return
	 */
	public static RelayConfig Android(GCMServerCredentials gcmServerCredentials) {
		return Android(gcmServerCredentials, null);
	}

	/**
	 * Creates an Android relay configuration having a buffer for received GCM messages.
	 * 
	 * @param gcmServerCredentials to be able to communicate over Google Cloud Messaging
	 * @param bufferConfiguration to buffer received GCM messages. Having multiple relays, this can help to
	 *            reduce battery consumption.
	 * @return
	 */
	public static RelayConfig Android(GCMServerCredentials gcmServerCredentials,
			MessageBufferConfiguration bufferConfiguration) {
		return new RelayConfig(RelayType.ANDROID, 60, gcmServerCredentials, 5, bufferConfiguration);
	}

	private RelayConfig(RelayType type, int peerMapUpdateInterval, GCMServerCredentials gcmServerCredentials,
			int gcmSendRetries, MessageBufferConfiguration bufferConfiguration) {
		this.type = type;
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		this.gcmServerCredentials = gcmServerCredentials;
		this.gcmSendRetries = gcmSendRetries;
		this.bufferConfiguration = bufferConfiguration;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(type.toString());
		sb.append("[Interval:").append(peerMapUpdateInterval).append("s");
		if(bufferConfiguration != null) {
			sb.append(", GCM-Buffered");
		}
		sb.append("]");
		return sb.toString();
	}

	/**
	 * @return the relay type
	 */
	public RelayType type() {
		return type;
	}

	/**
	 * Get the peer map update interval
	 */
	public int peerMapUpdateInterval() {
		return peerMapUpdateInterval;
	}

	/**
	 * Defines the time interval of sending the peer map of the unreachable peer
	 * to its relays. The routing requests are not relayed to the unreachable
	 * peer but handled by the relay peers. Therefore, the relay peers should
	 * always have an up-to-date peer map of the relayed peer
	 * 
	 * @param peerMapUpdateInterval the interval of updating the own peer map at the relay
	 * @return this instance
	 */
	public RelayConfig peerMapUpdateInterval(int peerMapUpdateInterval) {
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		return this;
	}

	/**
	 * <strong>Only used for {@link RelayConfig#ANDROID}</strong><br>
	 * 
	 * @return the {@link GCMServerCredentials}. If this peer is an unreachable
	 *         Android device, these credentials need to be provided.
	 */
	public GCMServerCredentials gcmServerCredentials() {
		return gcmServerCredentials;
	}

	/**
	 * <strong>Only used for {@link RelayConfig#ANDROID}</strong><br>
	 * 
	 * @return the number of retires sending a GCM message
	 */
	public int gcmSendRetries() {
		return gcmSendRetries;
	}

	/**
	 * <strong>Only used for {@link RelayConfig#ANDROID}</strong><br>
	 * 
	 * @param gcmSendRetries the number of retries sending a GCM message
	 * @return this instance
	 */
	public RelayConfig gcmSendRetries(int gcmSendRetries) {
		this.gcmSendRetries = gcmSendRetries;
		return this;
	}

	/**
	 * <strong>Only used for {@link RelayConfig#ANDROID}</strong><br>
	 * 
	 * @return the configuration for the GCM message buffer at the mobile device.
	 */
	public MessageBufferConfiguration bufferConfiguration() {
		return bufferConfiguration;
	}
}
