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
	private Peer peer;
	private boolean testCase = false;

	public HolePInitiatorImpl(Peer peer) {
		this.peer = peer;
		this.natTypeDetection = new NATTypeDetection(peer);
	}

	@Override
	public FutureDone<Message> handleHolePunch(int idleUDPSeconds, FutureResponse futureResponse,
			Message originalMessage) {
		FutureDone<Message> futureDone = new FutureDone<Message>();
		
		if (natTypeDetection.natType() == NATType.NON_PRESERVING_OTHER) {
			LOG.error("A symmetric NAT can't be traversed. No HolePunching possible!");
			return futureDone.failed("A symmetric NAT can't be traversed. No HolePunching possible!");
		}
		
		HolePStrategy holePuncher = natType().getHolePuncher(peer, HolePInitiator.NUMBER_OF_HOLES, idleUDPSeconds, originalMessage);
		return holePuncher.initiateHolePunch(futureDone, futureResponse);
	}
	
	public void checkNatType(PeerAddress peerAddress) {
		natTypeDetection.checkNATType(peerAddress);
	}
	
	public NATType natType() {
		return natTypeDetection.natType();
	}
	
	public boolean isTestCase() {
		return testCase;
	}
	
	public void testCase(final boolean testCase) {
		this.testCase = testCase;
	}
	
}
