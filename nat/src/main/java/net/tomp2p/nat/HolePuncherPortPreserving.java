package net.tomp2p.nat;

import io.netty.channel.ChannelHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.holep.DuplicatesHandler;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.Pair;

public class HolePuncherPortPreserving extends AbstractHolePuncher {
	
	private static final NATType NAT_TYPE = NATType.PORT_PRESERVING;
	
	public HolePuncherPortPreserving(Peer peer, int numberOfHoles, int idleUDPSeconds, Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}


	@Override
	protected List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> prepareHandlers(
			FutureResponse originalFutureResponse, final boolean initiator) {
		List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlerList = new ArrayList<Map<String, Pair<EventExecutorGroup, ChannelHandler>>>(
				numberOfHoles);
		SimpleChannelInboundHandler<Message> inboundHandler;
		Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;

		if (initiator) {
			for (int i = 0; i < numberOfHoles; i++) {
				inboundHandler = createAfterHolePHandler();
				handlers = peer.connectionBean().sender()
						.configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
				handlerList.add(handlers);
			}
		} else {
			inboundHandler = new DuplicatesHandler(peer.connectionBean().dispatcher());
			for (int i = 0; i < numberOfHoles; i++) {
				handlers = peer.connectionBean().sender()
						.configureHandlers(inboundHandler, originalFutureResponse, idleUDPSeconds, false);
				handlerList.add(handlers);
			}
		}

		return handlerList;
	}

	@Override
	protected SimpleChannelInboundHandler<Message> createAfterHolePHandler() {
		// TODO Auto-generated method stub
		return null;
	}


}
