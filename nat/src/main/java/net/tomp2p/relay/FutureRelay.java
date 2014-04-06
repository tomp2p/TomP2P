package net.tomp2p.relay;

import java.util.Collection;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureImpl;

public class FutureRelay extends BaseFutureImpl<FutureRelay> {

    /**
     * Defines how many relays have to be set up to consider it a success
     */
    private final int minRelays;
    
    private Collection<PeerConnection> relays;
    
    private DistributedRelay distributedRelay;

    public FutureRelay(int minRelays) {
        self(this);
        this.minRelays = minRelays;
    }
    
    public int minRelays() {
    	return minRelays;
    }

    
    public FutureRelay nothingTodo() {
    	synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return this;
            }
            type = FutureType.OK;
    	}
        notifyListeners();
        return this;
    }

	public FutureRelay setDone(Collection<PeerConnection> relays) {
		synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return this;
            }
            type = FutureType.OK;
            this.relays = relays;
    	}
        notifyListeners();
        return this;
    }
	
	public Collection<PeerConnection> relays() {
		synchronized (lock) {
			return relays;
        }
	}

	public FutureRelay distributedRelay(DistributedRelay distributedRelay) {
		synchronized (lock) {
			this.distributedRelay = distributedRelay;
		}
	    return this;
    }
	
	public DistributedRelay distributedRelay() {
		synchronized (lock) {
			return distributedRelay;
		}
	    
    }
}
