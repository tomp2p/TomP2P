package net.tomp2p.p2p;

import net.tomp2p.peers.PeerAddress;

/**
 * Filters potential and direct hits from the result set.
 * 
 * @author Nico Rutishauser
 *
 */
public interface PostRoutingFilter {

	/**
	 * @return <code>true</code> to reject / ignore a <strong>potential</strong> hit, otherwise
	 *         <code>false</code>
	 */
	boolean rejectPotentialHit(PeerAddress peerAddress);

	/**
	 * @return <code>true</code> to reject / ignore a <strong>direct</strong> hit, otherwise
	 *         <code>false</code>
	 */
	boolean rejectDirectHit(PeerAddress peerAddress);
}
