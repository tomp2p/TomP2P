package net.tomp2p.relay.android;

import net.tomp2p.p2p.SlowPeerFilter;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.Assert;

/**
 * Not only checks the slow flag, but also checks if the request targets this very same peer. If it does, the
 * request is allowed.
 * 
 * @author Nico
 *
 */
public class AssertingSlowPeerFilter extends SlowPeerFilter {

	private final Number160 slowPeer;

	public AssertingSlowPeerFilter(Number160 slowPeer) {
		this.slowPeer = slowPeer;
	}

	@Override
	public boolean rejectDirectHit(PeerAddress peerAddress) {
		boolean reject = super.rejectDirectHit(peerAddress);
		if (!reject && peerAddress.peerId().equals(slowPeer)) {
			Assert.fail("Should have filtered slow peer");
		}
		return reject;
	}

	@Override
	public boolean rejectPotentialHit(PeerAddress peerAddress) {
		boolean reject = super.rejectPotentialHit(peerAddress);
		if (!reject && peerAddress.peerId().equals(slowPeer)) {
			Assert.fail("Should have filtered slow peer");
		}
		return reject;
	}
}
