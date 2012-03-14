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

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultExceptionEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Is able to send TCP messages (as a request) and processes incoming replies. It is
 * important that this class handles close() because if we shutdown the
 * connections, the we need to notify the futures. In case of error set the peer
 * to offline. A similar class is {@link RequestHandlerUDP}, which is used for UDP.
 * 
 * @author Thomas Bocek
 * 
 */
public class RequestHandlerTCP<K extends FutureResponse> extends SimpleChannelHandler
{
	final private static Logger logger = LoggerFactory.getLogger(RequestHandlerTCP.class);
	// The future response which is currently be waited for
	final private K futureResponse;
	// The node this request handler is associated with
	final private PeerBean peerBean;
	final private ConnectionBean connectionBean;
	final private Message message;
	final private MessageID sendMessageID;
	final private AtomicBoolean reported = new AtomicBoolean(false);

	/**
	 * 
	 * @param objectHolder the bean representing the node this handler belongs
	 *        to
	 */
	public RequestHandlerTCP(K futureResponse, PeerBean peerBean,
			ConnectionBean connectionBean, Message message)
	{
		this.peerBean = peerBean;
		this.connectionBean = connectionBean;
		this.futureResponse = futureResponse;
		this.message = message;
		this.sendMessageID = new MessageID(message);
	}

	public K getFutureResponse()
	{
		return futureResponse;
	}

	public K sendTCP(ChannelCreator channelCreator)
	{
		return sendTCP(channelCreator, connectionBean.getConfiguration().getIdleTCPMillis());
	}

	public K sendTCP(ChannelCreator channelCreator, int idleTCPMillis)
	{
		connectionBean.getSender().sendTCP(this, futureResponse, message, channelCreator,
				idleTCPMillis);
		return futureResponse;
	}

	public K fireAndForgetTCP(ChannelCreator channelCreator)
	{
		connectionBean.getSender().sendTCP(null, futureResponse, message, channelCreator,
				connectionBean.getConfiguration().getIdleTCPMillis());
		return futureResponse;
	}

	protected PeerMap getPeerMap()
	{
		return peerBean.getPeerMap();
	}

	public void setKeepAlive(boolean isKeepAlive)
	{
		message.setKeepAlive(isKeepAlive);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Error originating from: " + futureResponse.getRequest());
			e.getCause().printStackTrace();
		}
		if (futureResponse.isCompleted())
		{
			logger.warn("Got exception, but ignored " + "(future response completed): "
					+ futureResponse.getFailedReason());
			if (logger.isDebugEnabled())
			{
				e.getCause().printStackTrace();
			}
		}
		else
		{
			if (logger.isDebugEnabled())
			{
				logger.debug("exception caugth, but handled properly: " + e.toString());
			}
			reportFail(e.toString(), ctx.getChannel(), futureResponse);
			if (e.getCause() instanceof PeerException)
			{
				PeerException pe = (PeerException) e.getCause();
				if (pe.getAbortCause() != PeerException.AbortCause.USER_ABORT)
				{
					boolean force = pe.getAbortCause() != PeerException.AbortCause.TIMEOUT;
					// do not force if we ran into a timeout, the peer may be
					// busy
					boolean added = getPeerMap().peerOffline(
							futureResponse.getRequest().getRecipient(), force);
					if (added)
					{
						logger.warn("removed from map, cause: " + pe.toString() + " msg: " + message);
					}
					else if (logger.isDebugEnabled())
					{
						logger.debug(pe.toString() + " msg: " + message);
					}
				}
				else if (logger.isWarnEnabled())
				{
					logger.warn("error in request " + e.toString());
					if (logger.isDebugEnabled())
					{
						e.getCause().printStackTrace();
					}
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
		if (!(e.getMessage() instanceof Message))
		{
			String msg = "Message received, but not of type Message: " + e.getMessage();
			exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
					PeerException.AbortCause.PEER_ABORT, msg)));
			return;
		}
		final Message responseMessage = (Message) e.getMessage();
		MessageID recvMessageID = new MessageID(responseMessage);
		if (responseMessage.getType() == Message.Type.UNKNOWN_ID)
		{
			String msg = "Message was not delivered successfully: " + this.message;
			exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
					PeerException.AbortCause.PEER_ABORT, msg)));
			return;
		}
		else if (responseMessage.getType() == Message.Type.EXCEPTION)
		{
			String msg = "Message caused an exception on the other side, handle as peer_abort: "
					+ this.message;
			exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
					PeerException.AbortCause.PEER_ABORT, msg)));
			return;
		}
		else if (!sendMessageID.equals(recvMessageID))
		{
			String msg = "Message [" + responseMessage
					+ "] sent to the node is not the same as we expect (TCP). We sent ["
					+ this.message + "]";
			if (logger.isWarnEnabled())
			{
				logger.warn(msg);
			}
			exceptionCaught(ctx, new DefaultExceptionEvent(ctx.getChannel(), new PeerException(
					PeerException.AbortCause.PEER_ABORT, msg)));
			return;
		}
		
		if (logger.isDebugEnabled())
		{
			logger.debug("perfect: " + responseMessage);
		}
		// We got a good answer, let's mark the sender as alive
		if (responseMessage.isOk() || responseMessage.isNotOk())
		{
			boolean retVal = getPeerMap().peerFound(responseMessage.getSender(), null);
			if (logger.isDebugEnabled() && !retVal)
			{
				logger.debug(responseMessage.getSender() + "not stored in peermap");
			}
		}
		// connection is closed by other peer
		reportResult(ctx.getChannel(), futureResponse, responseMessage);
		ctx.sendUpstream(e);
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
	{
		if (!reported.compareAndSet(false, true))
		{
			return;
		}
		// this needs to be the last listener added
		ctx.getChannel().getCloseFuture().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture arg0) throws Exception
			{
				if(logger.isDebugEnabled())
				{
					logger.debug("channel close, set failure for request message: "+message);
				}
				futureResponse.setFailed("Channel closed event");
			}
		});
		ctx.sendUpstream(e);
	}

	private void reportFail(final String cause, final Channel channel,
			final FutureResponse futureResponse)
	{
		if (!reported.compareAndSet(false, true))
		{
			return;
		}
		// this needs to be the last listener added
		channel.close().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture arg0) throws Exception
			{
				futureResponse.setFailed(cause);
			}
		});
	}

	private void reportResult(final Channel channel, final K futureResponse,
			final Message responseMessage)
	{
		if (!reported.compareAndSet(false, true))
		{
			return;
		}
		if (message.isKeepAlive())
		{
			futureResponse.setResponse(responseMessage);
		}
		else
		{
			// the other side closes the channel
			// this needs to be the last listener added
			channel.getCloseFuture().addListener(new ChannelFutureListener()
			{
				@Override
				public void operationComplete(ChannelFuture arg0) throws Exception
				{
					futureResponse.setResponse(responseMessage);
				}
			});
		}
	}
}