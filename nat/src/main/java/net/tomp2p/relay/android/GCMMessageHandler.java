package net.tomp2p.relay.android;

public interface GCMMessageHandler {

	/**
	 * Call this when a GCM message arrives
	 * 
	 * @param collapseKey the collapse key of the GCM message is the same as the relay's peer id
	 */
	void onGCMMessageArrival(String collapseKey);
}
