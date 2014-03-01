package net.tomp2p.relay;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

public class RelayPeer {
	private final Peer peer;
	private final RelayRPC relayRPC;
	private final RelayManager relayManager;
	
	private int maxRelays = PeerAddress.MAX_RELAYS;
	
	public RelayPeer(Peer peer) {
		this.peer = peer;
		this.relayRPC = RelayRPC.setup(peer);
		this.relayManager = new RelayManager(peer, maxRelays, relayRPC);
	}
	
	public Peer peer() {
		return peer;
	}
	
	public RelayRPC relayRPC() {
		return relayRPC;
	}
	
	public RelayManager relayManager() {
		return relayManager;
	}
	
	/**
     * Sets the maximum number of peers. A peer can currently have up to 5 relay
     * peers (specified in {@link PeerAddress#MAX_RELAYS}). Any number higher
     * than 5 will result in 5 relay peers.
     * 
     * @param maxRelays
     *            maximum number of relay peers (maximum specified in
     *            {@link PeerAddress#MAX_RELAYS}). Currently up to 5 relay peers
     *            are allowed
     * @return this instance
     */
    public RelayPeer maxRelays(int maxRelays) {
        this.maxRelays = maxRelays;
        return this;
    }
}
