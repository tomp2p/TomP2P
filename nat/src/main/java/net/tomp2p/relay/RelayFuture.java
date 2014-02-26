package net.tomp2p.relay;

import net.tomp2p.futures.BaseFutureImpl;

public class RelayFuture extends BaseFutureImpl<RelayFuture> {
	
	private RelayManager relayManager;
	
	public RelayFuture() {
		self(this);
	}
	
	public RelayManager relayManager() {
		return relayManager;
	}
	
	public void relayManager(RelayManager relayManager) {
        this.relayManager = relayManager;
    }

	public boolean done() {
		if(relayManager != null && relayManager.getRelayAddresses().size() > 0) {
			type = FutureType.OK;
		}
		synchronized (lock) {
			setCompletedAndNotify();
		}
		notifyListeners();
		return true;
	}

}
