package net.tomp2p.relay;

import net.tomp2p.peers.PeerAddress;

/**
 * @author Nico Rutishauser
 */
public interface OfflineListener {

	/**
	 * Is called when the {@link BaseRelayServer} detects that the unreachable peer is now offline.
	 * 
	 * @param unreachablePeer the peer that went offline
	 * @param the server that held the connection to the unreachable peer
	 */
	void onUnreachableOffline(PeerAddress unreachablePeer, BaseRelayServer server);
}
