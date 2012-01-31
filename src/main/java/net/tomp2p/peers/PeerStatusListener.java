package net.tomp2p.peers;

public interface PeerStatusListener
{
	/**
	 * The reason NOT_REACHABLE means that the peer is offline and cannot be
	 * contacted, while REMOVED_FROM_MAP means that this peer has been removed
	 * from the neigbhor list, but may still be reachable.
	 */
	public enum Reason
	{
		REMOVED_FROM_MAP, NOT_REACHABLE
	}

	/**
	 * Called if the peer does not send multiple answer in time. This peer is
	 * considered offline
	 * 
	 * @param peerAddress
	 *            The address of the peer that went offline
	 */
	public void peerOffline(PeerAddress peerAddress, Reason reason);

	/**
	 * Called if the peer does not send answer in time. The peer may be busy, so
	 * there is a chance of seeing this peer again.
	 * 
	 * @param peerAddress
	 *            The address of the peer that failed
	 * @param force Set to true if we are sure that the peer died.
	 */
	public void peerFail(PeerAddress peerAddress, boolean force);

	/**
	 * Called if the peer is online and we verified it. This method may get
	 * called many times, for each successful request.
	 * 
	 * @param peerAddress
	 *            The address of the peer that is online.
	 */
	public void peerOnline(PeerAddress peerAddress);
}
