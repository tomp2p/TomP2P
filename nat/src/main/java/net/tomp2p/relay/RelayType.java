package net.tomp2p.relay;

public enum RelayType {

	/**
	 * Data exchange will happen over an open TCP connection
	 */
	NORMAL(),

	/**
	 * Data exchange will take place over Google Cloud Messaging
	 */
	ANDROID();
}
