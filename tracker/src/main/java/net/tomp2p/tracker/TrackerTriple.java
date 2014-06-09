package net.tomp2p.tracker;

import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;

public class TrackerTriple {

	private PeerAddress remotePeer;
	private Number320 key;
	private TrackerData data;

	public PeerAddress remotePeer() {
		return remotePeer;
	}

	public TrackerTriple remotePeer(PeerAddress remotePeer) {
		this.remotePeer = remotePeer;
		return this;
	}

	public Number320 key() {
		return key;
	}

	public TrackerTriple key(Number320 key) {
		this.key = key;
		return this;
	}

	public TrackerData data() {
		return data;
	}

	public TrackerTriple data(TrackerData data) {
		this.data = data;
		return this;
	}

}
