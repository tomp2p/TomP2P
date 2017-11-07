package net.tomp2p.p2p;

import net.tomp2p.peers.PeerAddress;

/**
 * Filter the slow peers.<br>
 * 
 * @author Nico Rutishauser
 *
 */
public class SlowPeerFilter implements PostRoutingFilter {

	@Override
	public boolean rejectPotentialHit(PeerAddress peerAddress) {
		return false; //peerAddress.slow();
	}

	@Override
	public boolean rejectDirectHit(PeerAddress peerAddress) {
		return false; //peerAddress.slow();
	}

}
