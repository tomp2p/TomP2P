package net.tomp2p.holep;

import net.tomp2p.connection.HolePInitiator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.holep.strategy.HolePStrategy;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jonas Wagner
 * 
 *         This class is created if "new PeerNAT()" is called on a {@link Peer}.
 *         This class makes sure that hole punching is possible.
 */
public class HolePInitiatorImpl implements HolePInitiator {

	private static final Logger LOG = LoggerFactory.getLogger(HolePInitiatorImpl.class);
	private final NATTypeDetection natTypeDetection;
	private final Peer peer;
	private boolean testCase = false;
	private FutureDone<NATType> future;

	public HolePInitiatorImpl(final Peer peer) {
		this.peer = peer;
		this.natTypeDetection = new NATTypeDetection(peer);
	}

	@Override
	public FutureDone<Message> handleHolePunch(final int idleUDPSeconds, final FutureResponse futureResponse, final Message originalMessage) {
		//this is called from the sender, we start hole punching here.
		final FutureDone<Message> futureDone = new FutureDone<Message>();
		//final HolePStrategy holePuncher = natType().holePuncher(peer, peer.peerBean().holePNumberOfHoles(), idleUDPSeconds, originalMessage);
		//return holePuncher.initiateHolePunch(futureDone, futureResponse);
		return null;
	}

	/**
	 * CheckNatType will trigger the {@link NATTypeDetection} object to ping the
	 * given relay peer in order to find out the {@link NATType} of this
	 * {@link Peer}.
	 * 
	 * @param peerAddress
	 */
	public FutureDone<NATType> checkNatType(final PeerAddress peerAddress) {
		future = natTypeDetection.checkNATType(peerAddress);
		return future;
	}


	public boolean isTestCase() {
		return testCase;
	}

	public void testCase(final boolean testCase) {
		this.testCase = testCase;
	}

}
