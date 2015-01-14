package net.tomp2p.p2p;

import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerFilter;

/**
 * Filters potential and direct hits from the result set.
 * In contrast to the {@link PeerFilter}, this filter is applied <strong>after</strong> the routing.
 * 
 * @author Nico Rutishauser
 *
 */
public interface RoutingFilter {

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
