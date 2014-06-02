package net.tomp2p.tracker;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.IdentityManagement;
import net.tomp2p.tracker.DistributedTracker;
import net.tomp2p.tracker.Maintenance;
import net.tomp2p.tracker.PeerExchangeRPC;
import net.tomp2p.tracker.TrackerRPC;
import net.tomp2p.tracker.TrackerStorage;



public class PeerTracker {
	private DistributedTracker distributedTracker;
    private PeerExchangeRPC peerExchangeRPC;
    private TrackerRPC trackerRPC;
    private TrackerStorage trackerStorage = null;
    
    public PeerTracker(Peer peer) {
    	if (trackerStorage == null) {
			trackerStorage = new TrackerStorage(new IdentityManagement(peerBean.serverPeerAddress()), 300,
			        peerBean.getReplicationTracker(), new Maintenance());
		}

		peerBean.trackerStorage(trackerStorage);
		
		
		if (isEnablePeerExchangeRPC()) {
			PeerExchangeRPC peerExchangeRPC = new PeerExchangeRPC(peerBean, connectionBean);
			peer.setPeerExchangeRPC(peerExchangeRPC);
		}

		if (isEnableTrackerRPC()) {
			TrackerRPC trackerRPC = new TrackerRPC(peerBean, connectionBean);
			peer.setTrackerRPC(trackerRPC);
		}
		
		if (isEnableRouting() && isEnableTrackerRPC() && isEnablePeerExchangeRPC()) {
			DistributedTracker tracker = new DistributedTracker(peerBean, peer.distributedRouting(),
			        peer.getTrackerRPC(), peer.getPeerExchangeRPC());
			peer.setDistributedTracker(tracker);
		}
    }
    
    public PeerExchangeRPC getPeerExchangeRPC() {
        if (peerExchangeRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return peerExchangeRPC;
    }

    public void setPeerExchangeRPC(PeerExchangeRPC peerExchangeRPC) {
        this.peerExchangeRPC = peerExchangeRPC;
    }

    public TrackerRPC getTrackerRPC() {
        if (trackerRPC == null) {
            throw new RuntimeException("Not enabled, please enable this RPC in PeerMaker");
        }
        return trackerRPC;
    }

    public void setTrackerRPC(TrackerRPC trackerRPC) {
        this.trackerRPC = trackerRPC;
    }
    
    public DistributedTracker getDistributedTracker() {
        if (distributedTracker == null) {
            throw new RuntimeException("Not enabled, please enable this P2P function in PeerMaker");
        }
        return distributedTracker;
    }

    public void setDistributedTracker(DistributedTracker distributedTracker) {
        this.distributedTracker = distributedTracker;
    }
    
    public AddTrackerBuilder addTracker(Number160 locationKey) {
        return new AddTrackerBuilder(this, locationKey);
    }

    public GetTrackerBuilder getTracker(Number160 locationKey) {
        return new GetTrackerBuilder(this, locationKey);
    }
}
