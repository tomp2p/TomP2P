package net.tomp2p.relay;

public enum RelayType {

	/**
	 * Data exchange will happen over an open TCP connection
	 */
	OPENTCP(true),

	/**
	 * Data exchange will take place over Google Cloud Messaging
	 */
	ANDROID(false);
	
	private final boolean keepConnectionOpen;

	private RelayType(boolean keepConnectionOpen) {
		this.keepConnectionOpen = keepConnectionOpen;
	}

	public boolean keepConnectionOpen() {
		return keepConnectionOpen;
	}
}
