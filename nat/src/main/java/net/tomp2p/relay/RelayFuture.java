package net.tomp2p.relay;

import net.tomp2p.futures.BaseFutureImpl;

public class RelayFuture extends BaseFutureImpl<RelayFuture> {
	
	private RelayManager relayManager;
	
	public RelayFuture(RelayManager relayManager) {
		self(this);
		this.relayManager = relayManager;
	}
	
	public RelayManager relayManager() {
		return relayManager;
	}

	public boolean done() {
		if(relayManager.getRelayAddresses().size() > 0) {
			type = FutureType.OK;
		}
		synchronized (lock) {
			setCompletedAndNotify();
		}
		notifyListeners();
		return true;
	}

}
