package net.tomp2p.relay;

import net.tomp2p.futures.BaseFutureImpl;

public class RelayFuture extends BaseFutureImpl<RelayFuture> {

	private RelayManager relayManager;

	public RelayFuture() {
		self(this);
	}

	public RelayManager relayManager() {
		synchronized (lock) {
			return relayManager;
		}
	}

	public RelayFuture relayManager(RelayManager relayManager) {
		synchronized (lock) {
			if (!setCompletedAndNotify()) {
				return this;
			}
			this.relayManager = relayManager;
			if (relayManager != null && relayManager.getRelayAddresses().size() > 0) {
				type = FutureType.OK;
			} else {
				type = FutureType.FAILED;
			}
		}
		notifyListeners();
		return this;
	}
}
