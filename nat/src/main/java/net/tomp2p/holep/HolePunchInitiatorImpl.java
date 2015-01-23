package net.tomp2p.holep;

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

	private Peer peer;
	@SuppressWarnings("unused")
	private NATType natType;

	public HolePunchInitiatorImpl(Peer peer) {
		this.peer = peer;
	}

	@Override
	public FutureDone<Message> handleHolePunch(ChannelCreator channelCreator, int idleUDPSeconds, FutureResponse futureResponse,
			Message originalMessage) {
		HolePuncher holePuncher = new HolePuncher(peer, HolePunchInitiator.NUMBER_OF_HOLES, idleUDPSeconds, originalMessage);
		return holePuncher.initiateHolePunch(channelCreator, futureResponse);
	}
	
	public void natType(Object natType) {
		if (natType instanceof NATType) {
			this.natType = (NATType) natType;
		} else {
			this.natType = null;
		}
	}
}
