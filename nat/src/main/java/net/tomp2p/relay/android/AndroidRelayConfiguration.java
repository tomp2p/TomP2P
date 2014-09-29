package net.tomp2p.relay.android;

/**
 * Configure multiple parameters of relay nodes being able to serve unreachable Android devices. This
 * configuration need only to be set on the relay peer, not on the android device itself.
 * 
 * @author Nico Rutishauser
 *
 */
public class AndroidRelayConfiguration {

	private int bufferCountLimit = 10;
	private long bufferSizeLimit = Long.MAX_VALUE;
	private long bufferAgeLimit = 5 * 60 * 1000; // 5 minutes
	private String gcmAuthenticationToken;
	private int gcmSendRetries = 5;

	public int bufferCountLimit() {
		return bufferCountLimit;
	}

	/**
	 * Configures the maximal number of messages in the buffer. If the buffer is full, the device will be
	 * notified to download the messages.
	 * 
	 * @param bufferCountLimit the maximal message count
	 */
	public AndroidRelayConfiguration bufferCountLimit(int bufferCountLimit) {
		this.bufferCountLimit = bufferCountLimit;
		return this;
	}

	public long bufferSizeLimit() {
		return bufferSizeLimit;
	}

	/**
	 * Configures the maximum size of the message buffer in bytes. If the buffer is full, the device will be
	 * notified to download the messages.
	 * 
	 * @param bufferSizeLimit the maximal buffer size in bytes
	 */
	public AndroidRelayConfiguration bufferSizeLimit(long bufferSizeLimit) {
		this.bufferSizeLimit = bufferSizeLimit;
		return this;
	}

	public long bufferAgeLimit() {
		return bufferAgeLimit;
	}

	/**
	 * Configures the maximum age of the first message in the buffer. If the first message is put in at time 0
	 * and the age is 60'000, the buffer will notify the device after 1 minute.
	 * 
	 * @param bufferAgeLimit the maximum age of the content in the buffer in milliseconds
	 */
	public AndroidRelayConfiguration bufferAgeLimit(long bufferAgeLimit) {
		this.bufferAgeLimit = bufferAgeLimit;
		return this;
	}

	public String gcmAuthenticationToken() {
		return gcmAuthenticationToken;
	}

	/**
	 * The authentication token  (api key) provided by Google Cloud Messaging. Keep this key private.
	 * 
	 * @param gcmAuthenticationToken the api key
	 */
	public AndroidRelayConfiguration gcmAuthenticationToken(String gcmAuthenticationToken) {
		this.gcmAuthenticationToken = gcmAuthenticationToken;
		return this;
	}

	public int gcmSendRetries() {
		return gcmSendRetries;
	}

	/**
	 * The number of retries to send a message over GCM to the mobile device.
	 * 
	 * @param gcmSendRetries the maximum number of attempts to try to reach the mobile device
	 */
	public AndroidRelayConfiguration gcmSendRetries(int gcmSendRetries) {
		this.gcmSendRetries = gcmSendRetries;
		return this;
	}

}
