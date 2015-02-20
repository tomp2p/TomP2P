package net.tomp2p.holep;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.HolePunchInitiator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
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
public class HolePunchInitiatorImpl implements HolePunchInitiator {

	private static final Logger LOG = LoggerFactory.getLogger(HolePunchInitiatorImpl.class);
	private final NATTypeDetection natTypeDetection;
	private Peer peer;

	public HolePunchInitiatorImpl(Peer peer) {
		this.peer = peer;
		this.natTypeDetection = new NATTypeDetection(peer);
	}

	@Override
	public FutureDone<Message> handleHolePunch(ChannelCreator channelCreator, int idleUDPSeconds, FutureResponse futureResponse,
			Message originalMessage) {
		FutureDone<Message> futureDone = new FutureDone<Message>();
		
		if (natTypeDetection.natType() == NATType.NON_PRESERVING_OTHER) {
			LOG.error("A symmetric NAT can't be traversed. No HolePunching possible!");
			return futureDone.failed("A symmetric NAT can't be traversed. No HolePunching possible!");
		}
		
		HolePuncherStrategy holePuncher = natTypeDetection.natType().getHolePuncher(peer, HolePunchInitiator.NUMBER_OF_HOLES, idleUDPSeconds, originalMessage);
		return holePuncher.initiateHolePunch(futureDone, channelCreator, futureResponse);
	}
	
	public void checkNatType(PeerAddress peerAddress) {
		natTypeDetection.checkNATType(peerAddress);
	}
	
	public NATType natType() {
		return natTypeDetection.natType();
	}
}
