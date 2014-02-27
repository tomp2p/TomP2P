package net.tomp2p.relay;

import net.tomp2p.p2p.Peer;

public class RelayPeer {
	private final Peer peer;
	private final RelayRPC relayRPC;
	public RelayPeer(Peer peer) {
		this.peer = peer;
		this.relayRPC = RelayRPC.setup(peer);
	}
	
	public Peer peer() {
		return peer;
	}
	
	public RelayRPC relayRPC() {
		return relayRPC;
	}
}
