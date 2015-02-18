package net.tomp2p.connection;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.PeerAddress;

public interface NATTypeDetection {

	public FutureDone<NATTypeDetection> refreshNATType(PeerAddress relayPeerAddress);
}
