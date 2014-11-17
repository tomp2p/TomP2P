package net.tomp2p.relay;

public enum RelayType {

	/**
	 * Data exchange will happen over an open TCP connection
	 */
	OPENTCP(true, 5, false),

	/**
	 * Data exchange will take place over Google Cloud Messaging
	 */
	ANDROID(false, 4, true);

	private final boolean keepConnectionOpen;
	private final int maxRelayCount;
	private final boolean isSlow;

	private RelayType(boolean keepConnectionOpen, int maxRelayCount, boolean isSlow) {
		this.keepConnectionOpen = keepConnectionOpen;
		this.maxRelayCount = maxRelayCount;
		this.isSlow = isSlow;
	}

	/**
	 * Returns whether a peer connection should be kept open or not.
	 * 
	 * @return
	 */
	public boolean keepConnectionOpen() {
		return keepConnectionOpen;
	}

	/**
	 * Returns the maximum allowed number of relays.
	 */
	public int maxRelayCount() {
		return maxRelayCount;
	}

	/**
	 * Returns whether this type is 'slow' or not.
	 * 
	 * @return
	 */
	public boolean isSlow() {
		return isSlow;
	}
}
