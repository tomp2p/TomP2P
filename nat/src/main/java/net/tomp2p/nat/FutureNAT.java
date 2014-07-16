package net.tomp2p.nat;

import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.peers.PeerAddress;

public class FutureNAT extends BaseFutureImpl<FutureNAT> {
	
	private PeerAddress ourPeerAddress;
    private PeerAddress reporter;

	public FutureNAT() {
        self(this);
    }

	public FutureNAT done(final PeerAddress ourPeerAddress, final PeerAddress reporter) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return this;
            }
            this.type = FutureType.OK;
            if(this.reporter != null) {
    			if(!this.reporter.equals(reporter)) {
    				this.type = FutureType.FAILED;
    				this.reason = "the previously reported peer ("+this.reporter+") does not match the peer reported now ("+reporter+")";
    			}
            }
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
    
    public FutureNAT reporter(PeerAddress reporter) {
        synchronized (lock) {
        	if(this.reporter != null) {
    			if(!this.reporter.equals(reporter)) {
    				throw new IllegalArgumentException("cannot change reporter once its set");
    			}
    		}
            this.reporter = reporter;
        }
        return this;
    }
}
