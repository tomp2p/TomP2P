package net.tomp2p.relay.android.gcm;

public interface IGCMSender {

	/**
	 * Tickle the device through Google Cloud Messaging
	 * 
	 * @param futureGCM the tickle request containing also the buffered messages
	 */
	void send(FutureGCM futureGCM);
}
