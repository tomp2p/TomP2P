package net.tomp2p.relay;

public enum RelayType {

	/**
	 * Data exchange will happen over an open TCP connection
	 */
	OPENTCP(true, 5),

	/**
	 * Data exchange will take place over Google Cloud Messaging
	 */
	ANDROID(false, 4);
	
	private final boolean keepConnectionOpen;
	private final int maxRelayCount;

	private RelayType(boolean keepConnectionOpen, int maxRelayCount) {
		this.keepConnectionOpen = keepConnectionOpen;
		this.maxRelayCount = maxRelayCount;
	}

	public boolean keepConnectionOpen() {
		return keepConnectionOpen;
	}
	
	public int maxRelayCount() {
		return maxRelayCount;
	}
}
