package net.tomp2p.futures;


public class FutureAnnounce extends BaseFutureImpl<FutureAnnounce> {
	/**
	 * Constructor.
	 */
	public FutureAnnounce() {
		self(this);
	}

	/**
	 * Gets called if the ping was a success and an other peer could ping us
	 * with TCP and UDP.
	 * 
	 * @param reporter
	 *            The peerAddress of the peer that reported our address
	 */
	public FutureAnnounce done() {
		synchronized (lock) {
			if (!completedAndNotify()) {
				return this;
			}
			this.type = FutureType.OK;
		}
		notifyListeners();
		return this;
	}
}
