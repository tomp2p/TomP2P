package net.tomp2p.holep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.HolePunchInitiator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;

/**
 * @author Jonas Wagner
 * 
 *         This class is created if "new PeerNAT()" is called on a {@link Peer}.
 *         This class makes sure that hole punching is possible.
 */
public class HolePunchInitiatorImpl implements HolePunchInitiator {

	private static final Logger LOG = LoggerFactory.getLogger(HolePunchInitiatorImpl.class);
	private Peer peer;
	private NATType natType;

	public HolePunchInitiatorImpl(Peer peer) {
		this.peer = peer;
		this.natType = NATType.UNKNOWN;
	}

	@Override
	public FutureDone<Message> handleHolePunch(ChannelCreator channelCreator, int idleUDPSeconds, FutureResponse futureResponse,
			Message originalMessage) {
		FutureDone<Message> futureDone = new FutureDone<Message>();
		
		if (natType == NATType.NON_PRESERVING_OTHER) {
			LOG.error("A symmetric NAT can't be traversed. No HolePunching possible!");
			return futureDone.failed("A symmetric NAT can't be traversed. No HolePunching possible!");
		}
		
		HolePuncher holePuncher = new HolePuncher(peer, HolePunchInitiator.NUMBER_OF_HOLES, idleUDPSeconds, originalMessage);
		return holePuncher.initiateHolePunch(futureDone, channelCreator, futureResponse, natType);
	}
	
	public void natType(Object natType) {
		if (natType instanceof NATType) {
			this.natType = (NATType) natType;
		} else {
			this.natType = null;
		}
	}
	
	public NATType natType() {
		return this.natType;
	}
}
