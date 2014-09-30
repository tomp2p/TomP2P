package net.tomp2p.relay.android;

/**
 * The Android device needs to know a GCM server and needs to register itself there.
 * After the registration at the GCM server with a given senderID, the device receives a registrationID.
 * 
 * @author Nico Rutishauser
 *
 */
public class GCMServerCredentials {

	private long senderId;
	private String senderAuthenticationKey;
	private String registrationId;

	public String registrationId() {
		return registrationId;
	}

	/**
	 * Sets the GCM registration ID of the Android device.
	 * 
	 * @param registrationId the registraion id
	 * @return this instance
	 */
	public GCMServerCredentials registrationId(String registrationId) {
		this.registrationId = registrationId;
		return this;
	}

	public long senderId() {
		return senderId;
	}

	/**
	 * Sets the GCM sender Id. This is required to obtrain a registraionId and to distinguish multiple senders
	 * apart.
	 * 
	 * @param senderId the sender id.
	 * @return this instance
	 */
	public GCMServerCredentials senderId(long senderId) {
		this.senderId = senderId;
		return this;
	}

	public String senderAuthenticationKey() {
		return senderAuthenticationKey;
	}

	/**
	 * Set the authentication key, which is used by relay peers to send messages. Although this key needs to
	 * be kept secret, Android devices need to know it and then send it to the relay peers. This is a design
	 * decision. The reason for it is that the Android devices 'need' the help of the relay peers. Relays may
	 * be distributed over the world and probably don't know anything about a specific GCM instance. Thus,
	 * Android devices povide all necessary data for relays to be able to send GCM messages to them.
	 * 
	 * @param senderAuthenticationKey the api key / authentication token for Google Cloud Messaging. The key
	 *            can be obtained through Google's developer console
	 * @return this instance
	 */
	public GCMServerCredentials senderAuthenticationKey(String senderAuthenticationKey) {
		this.senderAuthenticationKey = senderAuthenticationKey;
		return this;
	}
}
