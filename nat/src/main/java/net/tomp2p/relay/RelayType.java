package net.tomp2p.relay;

import net.tomp2p.message.Message.Type;

public enum RelayType {

	/**
	 * Data exchange will happen over an open TCP connection
	 */
	NORMAL(Type.REQUEST_1),

	/**
	 * Data exchange will take place over Google Cloud Messaging
	 */
	ANDROID(Type.REQUEST_4);

	private final Type setupMessage;

	private RelayType(Type setupMessage) {
		this.setupMessage = setupMessage;
	}

	public Type getSetupMessage() {
		return setupMessage;
	}
}
