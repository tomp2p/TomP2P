package net.tomp2p.relay.android;

/**
 * @author Nico Rutishauser
 */
public interface AndroidOfflineListener {

	/**
	 * Is called when the {@link AndroidForwarderRPC} detects that the device is now offline.
	 */
	void onAndroidOffline();
}
