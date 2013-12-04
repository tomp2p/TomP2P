package relay;

import net.tomp2p.futures.BaseFutureImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayFuture extends BaseFutureImpl<RelayFuture> {
	
	private final static Logger logger = LoggerFactory.getLogger(RelayFuture.class);
	
	private RelayPeersManager relayManager;
	
	public RelayFuture(RelayPeersManager relayManager) {
		self(this);
		this.relayManager = relayManager;
	}
	
	public RelayPeersManager relayPeersManager() {
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
