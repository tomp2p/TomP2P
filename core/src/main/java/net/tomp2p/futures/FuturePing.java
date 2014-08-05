package net.tomp2p.futures;

import net.tomp2p.peers.PeerAddress;

public class FuturePing extends BaseFutureImpl<FuturePing> {

	private PeerAddress remotePeer;

	/**
	 * Constructor.
	 */
	public FuturePing() {
		self(this);
	}

	/**
	 * Gets called if the ping was a success and an other peer could ping us
	 * with TCP and UDP.
	 * 
	 * @param reporter
	 *            The peerAddress of the peer that reported our address
	 */
	public FuturePing done(final PeerAddress remotePeer) {
		synchronized (lock) {
			if (!completedAndNotify()) {
				return this;
			}
			this.type = FutureType.OK;
			this.remotePeer = remotePeer;
		}
		notifyListeners();
		return this;
	}

	/**
	 * @return The remotePeer whit the peerId of the remote peer.
	 */
	public PeerAddress remotePeer() {
		synchronized (lock) {
			return remotePeer;
		}
	}

}
