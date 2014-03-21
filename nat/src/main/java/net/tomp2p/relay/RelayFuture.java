package net.tomp2p.relay;

import net.tomp2p.futures.BaseFutureImpl;

public class RelayFuture extends BaseFutureImpl<RelayFuture> {

    /**
     * Defines how many relays have to be set up to consider it a success
     */
    private final int minRelays;

    private RelayManager relayManager;

    public RelayFuture(int minRelays) {
        self(this);
        this.minRelays = minRelays;
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
            if (relayManager != null && relayManager.getRelayAddresses().size() >= minRelays) {
                type = FutureType.OK;
            } else {
                type = FutureType.FAILED;
                setFailed("Not enough relay connections could be set up");
            }
        }
        
        notifyListeners();
        return this;
    }
}
