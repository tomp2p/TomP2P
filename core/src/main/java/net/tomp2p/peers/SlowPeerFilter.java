package net.tomp2p.peers;

import java.util.Collection;

/**
 * Filter the slow peers.<br>
 * 
 * @author Nico Rutishauser
 *
 */
public class SlowPeerFilter implements PeerFilter {

	@Override
	public boolean reject(PeerAddress peerAddress, Collection<PeerAddress> all, Number160 target) {
		return peerAddress.isSlow();
	}

}
