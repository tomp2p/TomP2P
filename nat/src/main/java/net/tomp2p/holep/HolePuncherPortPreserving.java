package net.tomp2p.holep;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.Dispatcher;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.Pair;

public class HolePuncherPortPreserving extends AbstractHolePuncher {
	
	private static final NATType NAT_TYPE = NATType.PORT_PRESERVING;
	private static final Logger LOG = LoggerFactory.getLogger(HolePuncherPortPreserving.class);
	
	public HolePuncherPortPreserving(Peer peer, int numberOfHoles, int idleUDPSeconds, Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}


	/**
	 * This method does two things. If the initiating peer calls it, he gets
	 * back a {@link List} of new {@link SimpleInboundHandler} to deal with the
	 * replies of the replying peer. If a replying peer is calling this method
	 * it will return a {@link List} of default {@link SimpleInboundHandler}s
	 * from the {@link Dispatcher}.
	 * 
	 * @param originalFutureResponse
	 * @return handlerList
	 */
	private List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> prepareHandlers(
			FutureResponse originalFutureResponse, final boolean initiator, final FutureDone<Message> futureDone) {
		List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlerList = new ArrayList<Map<String, Pair<EventExecutorGroup, ChannelHandler>>>(
				numberOfHoles);
		SimpleChannelInboundHandler<Message> inboundHandler;
		Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers;

		if (initiator) {
			for (int i = 0; i < numberOfHoles; i++) {
				inboundHandler = createAfterHolePHandler(futureDone);
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
	protected SimpleChannelInboundHandler<Message> createAfterHolePHandler(final FutureDone<Message> futureDone) {
		final SimpleChannelInboundHandler<Message> inboundHandler = new SimpleChannelInboundHandler<Message>() {

			@Override
			protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
				if (Message.Type.OK == msg.type() && originalMessage.command() == msg.command()) {
					LOG.debug("Successfully transmitted the original message to peer:[" + msg.sender().toString()
							+ "]. Now here's the reply:[" + msg.toString() + "]");
					futureDone.done(msg);
					// TODO jwa does this work?
					ctx.close();
				} else if (Message.Type.REQUEST_3 == msg.type() && Commands.HOLEP.getNr() == msg.command()) {
					LOG.debug("Holes successfully punched with ports = {localPort = " + msg.recipient().udpPort()
							+ " , remotePort = " + msg.sender().udpPort() + "}!");
				} else {
					LOG.debug("Holes punche not punched with ports = {localPort = " + msg.recipient().udpPort()
							+ " , remotePort = " + msg.sender().udpPort() + "} yet!");
				}
			}
		};
		return inboundHandler;
	}


}
