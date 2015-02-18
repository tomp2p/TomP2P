package net.tomp2p.holep;

import net.tomp2p.connection.NATTypeDetection;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.PeerAddress;

/**
 * 
 * @author jonaswagner
 *
 */

public enum NATType implements NATTypeDetection{
	UNKNOWN,
	NO_NAT,
	PORT_PRESERVING, //NAT takes the same port as source port on peer
	NON_PRESERVING_SEQUENTIAL, //NAT assigns new port for each mapping starting at a defined number, and increasing by one (e.g. 1234).
	NON_PRESERVING_OTHER //NAT assigns a new (random or other) port for each mapping.
;

	@Override
	public FutureDone<NATTypeDetection> refreshNATType(PeerAddress relayPeerAddress) {
		// TODO Auto-generated method stub
		return null;
	}
}
