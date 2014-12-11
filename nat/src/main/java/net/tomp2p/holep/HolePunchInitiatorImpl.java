package net.tomp2p.holep;

import io.netty.channel.SimpleChannelInboundHandler;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.HolePunchInitiator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;

public class HolePunchInitiatorImpl implements HolePunchInitiator {
	
	private Peer peer;
	
	public HolePunchInitiatorImpl(Peer peer) {
		this.peer = peer;
	}

	@Override
	public FutureDone<Message> handleHolePunch(ChannelCreator channelCreator, int idleUDPSeconds,
			FutureResponse futureResponse, Message originalMessage) {
		HolePuncher holePuncher = new HolePuncher(peer, HolePunchInitiator.NUMBER_OF_HOLES, idleUDPSeconds, originalMessage);
		return holePuncher.initiateHolePunch(channelCreator, futureResponse);
	}
	
	

	
}
