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
			this.relayManager = relayManager;
		}
		return this;
	}

	public void done() {
		synchronized (lock) {
			if (!setCompletedAndNotify()) {
				return;
			}
			if (relayManager != null && relayManager.getRelayAddresses().size() > 0) {
				type = FutureType.OK;
			} else {
				type = FutureType.FAILED;
			}
		}
		notifyListeners();
	}
}
