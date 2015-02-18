package net.tomp2p.holep;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.HolePunchInitiator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.Pair;

/**
 * DO NOT INSTANCIATE THIS CLASS!
 * 
 * @author Jonas Wagner
 *
 */
public abstract class AbstractHolePuncher {
	
	private static final Logger LOG = LoggerFactory.getLogger(AbstractHolePuncher.class);
	protected static final boolean BROADCAST_VALUE = HolePunchInitiator.BROADCAST;
	protected static final boolean FIRE_AND_FORGET_VALUE = false;
	protected final Peer peer;
	protected final int numberOfHoles;
	protected final int idleUDPSeconds;
	protected final Message originalMessage;
	
	
	public AbstractHolePuncher(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		this.peer = peer;
		this.numberOfHoles = numberOfHoles;
		this.idleUDPSeconds = idleUDPSeconds;
		this.originalMessage = originalMessage;
		
		LOG.trace("new HolePuncher created, originalMessage {}", originalMessage.toString());
	}
	
	/**
	 * This is a generic method which creates a number of {@link ChannelFuture}s
	 * and calls the associated {@link FutureDone} as soon as they're done.
	 * 
	 * @param originalFutureResponse
	 * @param handlersList
	 * @return fDoneChannelFutures
	 */
	protected final FutureDone<List<ChannelFuture>> createChannelFutures(final FutureResponse originalFutureResponse,
			final List<Map<String, Pair<EventExecutorGroup, ChannelHandler>>> handlersList, final FutureDone<Message> mainFutureDone) {

		final FutureDone<List<ChannelFuture>> fDoneChannelFutures = new FutureDone<List<ChannelFuture>>();
		final AtomicInteger countDown = new AtomicInteger(numberOfHoles);
		final List<ChannelFuture> channelFutures = new ArrayList<ChannelFuture>();

		for (int i = 0; i < numberOfHoles; i++) {
			final Map<String, Pair<EventExecutorGroup, ChannelHandler>> handlers = handlersList.get(i);

			FutureChannelCreator fcc = peer.connectionBean().reservation().create(1, 0);
			fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						ChannelFuture cF = future.channelCreator().createUDP(BROADCAST_VALUE, handlers,
								originalFutureResponse);
						cF.addListener(new GenericFutureListener<ChannelFuture>() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (future.isSuccess()) {
									channelFutures.add(future);
								} else {
									handleFail("Error while creating the ChannelFutures!", mainFutureDone);
								}
								countDown.decrementAndGet();
								if (countDown.get() == 0) {
									fDoneChannelFutures.done(channelFutures);
								}
							}
						});
					} else {
						countDown.decrementAndGet();
						handleFail("Error while creating the ChannelFutures!", mainFutureDone);
					}
				}
			});
		}
		return fDoneChannelFutures;
	}
	
	/**
	 * This method creates the inboundHandler for the replyMessage of the peer
	 * that we want to send a message to.
	 * 
	 * @return inboundHandler
	 */
	protected abstract SimpleChannelInboundHandler<Message> createAfterHolePHandler(FutureDone<Message> futureDone);
	
	protected void handleFail(final String failMessage, final FutureDone<Message> futureDone) {
		futureDone.failed(failMessage);
	}
}
