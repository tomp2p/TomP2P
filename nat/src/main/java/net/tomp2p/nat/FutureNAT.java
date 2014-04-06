package net.tomp2p.nat;

import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.peers.PeerAddress;

public class FutureNAT extends BaseFutureImpl<FutureNAT> {
	
	private PeerAddress ourPeerAddress;
    private PeerAddress reporter;

	public FutureNAT() {
        self(this);
    }

	public void done(final PeerAddress ourPeerAddress, final PeerAddress reporter) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.type = FutureType.OK;
            this.ourPeerAddress = ourPeerAddress;
            this.reporter = reporter;
        }
        notifyListeners();
    }
	
	/**
     * The peerAddress where we are reachable.
     * 
     * @return The new un-firewalled peerAddress of this peer
     */
    public PeerAddress getPeerAddress() {
        synchronized (lock) {
            return ourPeerAddress;
        }
    }

    /**
     * @return The reporter that told us what peer address we have
     */
    public PeerAddress getReporter() {
        synchronized (lock) {
            return reporter;
        }
    }
}
