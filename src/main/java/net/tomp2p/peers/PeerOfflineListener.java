package net.tomp2p.peers;
public interface PeerOfflineListener
{
	public void peerOffline(PeerAddress peerAddress);

	public void peerFail(PeerAddress peerAddress);
}
