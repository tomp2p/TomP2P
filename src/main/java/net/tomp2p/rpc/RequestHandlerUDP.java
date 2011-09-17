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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerException;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerMap;

import org.jboss.netty.channel.ChannelHandlerContext;
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
public class RequestHandlerUDP extends SimpleChannelHandler
{
	final private static Logger logger = LoggerFactory.getLogger(RequestHandlerUDP.class);
	// The future response which is currently be waited for
	final private FutureResponse futureResponse;
	// The node this request handler is associated with
	final private PeerBean peerBean;
	final private ConnectionBean connectionBean;
	final private Message message;
	final private AtomicBoolean handlingMessage = new AtomicBoolean(false);
	final private MessageID sendMessageID;

	/**
	 * 
	 * @param objectHolder the bean representing the node this handler belongs
	 *        to
	 */
	public RequestHandlerUDP(FutureResponse futureResponse, PeerBean peerBean,
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

	public FutureResponse sendUDP(ChannelCreator channelCreator)
	{
		connectionBean.getSender().sendUDP(this, futureResponse, message, channelCreator);
		return futureResponse;
	}

	public FutureResponse sendBroadcastUDP(ChannelCreator channelCreator)
	{
		connectionBean.getSender().sendBroadcastUDP(this, futureResponse, message, channelCreator);
		return futureResponse;
	}

	public FutureResponse fireAndForgetUDP(ChannelCreator channelCreator)
	{
		connectionBean.getSender().sendUDP(null, futureResponse, message, channelCreator);
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
			logger.warn("Got exception, but ignored " + "(future response completed): "
					+ futureResponse.getFailedReason());
			//if (logger.isDebugEnabled())
				e.getCause().printStackTrace();
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
					logger.warn("error in request " + e.toString());
					if (logger.isDebugEnabled())
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
				String msg = "Message caused an exception on the other side, handle as peer_abort: "
						+ this.message;
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
				if (logger.isDebugEnabled())
					logger.debug("perfect: " + message);
				// We got a good answer, let's mark the sender as alive
				if (message.isOk() || message.isNotOk())
					getPeerMap().peerFound(message.getSender(), null);
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
	/*
	 * public void closeRequested(ChannelHandlerContext ctx, ChannelStateEvent
	 * e) throws Exception { ctx.sendDownstream(e); try { throw new
	 * RuntimeException(""); } catch(RuntimeException ee) {
	 * ee.printStackTrace(); } logger.error("CLOSE CALLED"); }
	 */
}