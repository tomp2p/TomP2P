package net.tomp2p.tracker;

import java.util.concurrent.ScheduledFuture;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;

public class PeerTracker {

	private final Peer peer;
	private final ScheduledFuture<?> scheduledFuture;
	private final TrackerRPC trackerRPC;
	private final TrackerStorage trackerStorage;
	private final PeerExchange peerExchange;
	private final DistributedTracker distributedTracker;

	public PeerTracker(Peer peer, ScheduledFuture<?> scheduledFuture, TrackerRPC trackerRPC,
	        TrackerStorage trackerStorage, PeerExchange peerExchange, DistributedTracker distributedTracker) {
		this.peer = peer;
		this.scheduledFuture = scheduledFuture;
		this.trackerRPC = trackerRPC;
		this.trackerStorage = trackerStorage;
		this.peerExchange = peerExchange;
		this.distributedTracker = distributedTracker;
	}

	public TrackerRPC trackerRPC() {
		return trackerRPC;
	}

	public DistributedTracker distributedTracker() {
		return distributedTracker;
	}

	public TrackerStorage trackerStorage() {
		return trackerStorage;
	}

	public PeerExchange peerExchange() {
		return peerExchange;
	}

	public PeerMap peerMap() {
		return peer.peerBean().peerMap();
	}

	public PeerAddress peerAddress() {
		return peer.peerAddress();
	}

	public Peer peer() {
		return peer;
	}

	public AddTrackerBuilder addTracker(Number160 locationKey) {
		return new AddTrackerBuilder(this, locationKey);
	}

	public GetTrackerBuilder getTracker(Number160 locationKey) {
		return new GetTrackerBuilder(this, locationKey);
	}

	public void shutdown() {
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
		}
	}

}
