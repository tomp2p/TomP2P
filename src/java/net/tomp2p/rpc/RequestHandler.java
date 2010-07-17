/*
 * Copyright 2009 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.rpc;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerException;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerMap;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultExceptionEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Is able to send messages (as a request) and processes incoming replies.
 * 
 * @author Thomas Bocek
 * 
 */
public class RequestHandler extends SimpleChannelHandler
{
	final private static Logger logger = LoggerFactory.getLogger(RequestHandler.class);
	// The future response which is currently be waited for
	final private FutureResponse futureResponse;
	// The node this request handler is associated with
	final private PeerBean peerBean;
	final private ConnectionBean connectionBean;
	final private Message message;
	final private AtomicBoolean handlingMessage = new AtomicBoolean(false);
	final private MessageID sendMessageID;

	public RequestHandler(PeerBean peerBean, ConnectionBean connectionBean, Message message)
	{
		this(new FutureResponse(message), peerBean, connectionBean, message);
	}

	/**
	 * 
	 * @param objectHolder the bean representing the node this handler belongs
	 *        to
	 */
	public RequestHandler(FutureResponse futureResponse, PeerBean peerBean,
			ConnectionBean connectionBean, Message message)
	{
		this.peerBean = peerBean;
		this.connectionBean = connectionBean;
		this.futureResponse = futureResponse;
		this.message = message;
		this.sendMessageID = new MessageID(message);
	}

	public FutureResponse getFutureResponse()
	{
		return futureResponse;
	}

	public FutureResponse sendTCP()
	{
		connectionBean.getSender().sendTCP(message, this);
		return futureResponse;
	}

	public FutureResponse sendUDP()
	{
		connectionBean.getSender().sendUDP(message, this);
		return futureResponse;
	}

	public FutureResponse sendBroadcastUDP()
	{
		connectionBean.getSender().sendBroadcastUDP(message, this);
		return futureResponse;
	}

	public FutureResponse fireAndForgetUDP()
	{
		connectionBean.getSender().fireAndForgetUDP(message);
		return futureResponse;
	}

	public FutureResponse fireAndForgetTCP()
	{
		connectionBean.getSender().fireAndForgetTCP(message);
		return futureResponse;
	}

	protected PeerMap getPeerMap()
	{
		return peerBean.getPeerMap();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
	{
		if (handlingMessage.compareAndSet(false, true))
		{
			futureResponse.cancelTimeout();
			ctx.getChannel().close();
		}
		if (logger.isDebugEnabled())
		{
			logger.debug("Error originating from: " + futureResponse.getRequest());
			for (StackTraceElement t : Thread.currentThread().getStackTrace())
				logger.debug("\t" + t);
			e.getCause().printStackTrace();
		}
		if (futureResponse.isCompleted())
		{
			logger.warn("Got exception, but ignored "
					+ "(future response completed), still shown below. "
					+ futureResponse.getFailedReason());
		}
		else
		{
			// e.getCause().printStackTrace();
			logger.debug("exception caugth, but handled properly: " + e.toString());
			futureResponse.setFailed(e.toString());
			if (e.getCause() instanceof PeerException)
			{
				PeerException pe = (PeerException) e.getCause();
				if (pe.getAbortCause() != PeerException.AbortCause.USER_ABORT)
				{
					boolean added = getPeerMap().peerOffline(
							futureResponse.getRequest().getRecipient(), false);
					if (added)
						logger.warn("Peer exception (" + System.currentTimeMillis() + ") "
								+ e.getCause() + " msg " + message + " for "
								+ futureResponse.getRequest().getRecipient());
					else if (logger.isDebugEnabled())
						logger.debug(pe.getMessage() + message);
				}
				else if (logger.isWarnEnabled())
				{
					logger.debug("error in request");
					e.getCause().printStackTrace();
				}
			}
			else
			{
				getPeerMap().peerOffline(futureResponse.getRequest().getRecipient(), true);
			}
		}
		ctx.sendUpstream(e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		if (handlingMessage.compareAndSet(false, true))
		{
			futureResponse.cancelTimeout();
			ctx.getChannel().close();
		}
		if (e.getMessage() instanceof Message)
		{
			Message message = (Message) e.getMessage();
			MessageID recvMessageID = new MessageID(message);
			if (message.getType() == Message.Type.UNKNOWN_ID)
			{
				String msg = "Message was not delivered successfully: " + this.message;
				exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
						PeerException.AbortCause.PEER_ABORT, msg)));
			}
			else if (message.getType() == Message.Type.EXCEPTION)
			{
				String msg = "Message caused an exception on the other side, handle as peer_abort: " + this.message;
				exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
						PeerException.AbortCause.PEER_ABORT, msg)));
			}
			else if (!sendMessageID.equals(recvMessageID))
			{
				String msg = "Message [" + message
						+ "] sent to the node is not the same as we expect. We sent ["
						+ this.message + "]";
				if (logger.isWarnEnabled())
					logger.warn(msg);
				exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
						PeerException.AbortCause.PEER_ABORT, msg)));
			}
			else
			{
				// We got a good answer, let's mark the sender as alive
				if (message.isOk() || message.isNotOk())
					getPeerMap().peerOnline(message.getSender(), null);
				futureResponse.setResponse(message);
			}
		}
		else
		{
			String msg = "Message [" + e.getMessage() + "] is not of type Message";
			logger.error(msg);
			exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
					PeerException.AbortCause.PEER_ABORT, msg)));
		}
		ctx.sendUpstream(e);
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
	{
		if (futureResponse.isCompleted())
		{
			futureResponse.cancelTimeout();
			ctx.getChannel().close();
		}
		ctx.sendUpstream(e);
	}
	// IDEA reduce timeout to 1sec if channel closed
	/*
	 * @Override public void channelClosed(ChannelHandlerContext ctx,
	 * ChannelStateEvent e) throws Exception { if (ctx.getAttachment()==null &&
	 * handlingMessage.compareAndSet(false, true)) {
	 * futureResponse.cancelTimeout();
	 * getPeerMap().peerOffline(futureResponse.getRequest().getRecipient(),
	 * false);
	 * futureResponse.setFailed("Closed channel before answer received "+
	 * futureResponse.isCompleted()); throw new RuntimeException("why"); }
	 * ctx.sendUpstream(e); }
	 */
}

interface Release
{
	public void release();
}