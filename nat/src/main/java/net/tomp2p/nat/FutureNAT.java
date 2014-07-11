package net.tomp2p.nat;

import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.peers.PeerAddress;

public class FutureNAT extends BaseFutureImpl<FutureNAT> {
	
	private PeerAddress ourPeerAddress;
    private PeerAddress reporter;
    private PeerAddress discoverPeer;

	public FutureNAT() {
        self(this);
    }

	public FutureNAT done(final PeerAddress ourPeerAddress, final PeerAddress reporter) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return this;
            }
            this.type = FutureType.OK;
            this.ourPeerAddress = ourPeerAddress;
            this.reporter = reporter;
        }
        notifyListeners();
        return this;
    }
	
	/**
     * The peerAddress where we are reachable.
     * 
     * @return The new un-firewalled peerAddress of this peer
     */
    public PeerAddress peerAddress() {
        synchronized (lock) {
            return ourPeerAddress;
        }
    }

    /**
     * @return The reporter that told us what peer address we have
     */
    public PeerAddress reporter() {
        synchronized (lock) {
            return reporter;
        }
    }
    
    public FutureNAT discoverPeer(PeerAddress discoverPeer) {
        synchronized (lock) {
            this.discoverPeer = discoverPeer;
        }
        return this;
    }
    
    public PeerAddress discoverPeer() {
        synchronized (lock) {
            return discoverPeer;
        }
    }

}
