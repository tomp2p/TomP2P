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
package net.tomp2p.connection;
import net.tomp2p.futures.Cancellable;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles sending of messages
 * 
 * @author Thomas Bocek
 * 
 */
public class Sender
{
	final private static Logger logger = LoggerFactory.getLogger(Sender.class);
	// Timer used for ReplyTimeout
	final private Timer timer;
	final private ConnectionConfiguration configuration;
	private volatile boolean shutdown = false;

	public Sender(final ConnectionConfiguration configuration, Timer timer)
	{
		this.configuration = configuration;
		this.timer = timer;
	}

	public void sendTCP(final RequestHandlerTCP handler, final FutureResponse futureResponse, final Message message, ChannelCreator channelCreator, final int idleTCPMillis)
	{
		if(shutdown) {
			futureResponse.setFailed("Shutdown in sender");
		}
		else {
			sendTCP(message.getRecipient(), handler, futureResponse, message, channelCreator, idleTCPMillis);
		}
	}

	public void sendUDP(final RequestHandlerUDP handler, final FutureResponse futureResponse, final Message message, ChannelCreator channelCreator)
	{
		if(shutdown) {
			futureResponse.setFailed("Shutdown in sender");
		}
		else {
			sendUDP(message.getRecipient(), handler, futureResponse, message, false, channelCreator);
		}
	}

	public void sendBroadcastUDP(final RequestHandlerUDP handler, final FutureResponse futureResponse, final Message message, ChannelCreator channelCreator)
	{
		if(shutdown)  {
			futureResponse.setFailed("Shutdown in sender");
		}
		else {
			sendUDP(message.getRecipient(), handler, futureResponse, message, true, channelCreator);
		}
	}

	private void sendTCP(final PeerAddress remoteNode, final RequestHandlerTCP requestHandler, 
			final FutureResponse futureResponse, final Message message, final ChannelCreator channelCreator, final int idleTCPMillis)
	{
		if (logger.isDebugEnabled())
			logger.debug("send TCP " + Thread.currentThread().getName());
		sendTCP0(remoteNode, requestHandler, futureResponse,  message, channelCreator, idleTCPMillis);
	}

	private void sendUDP(final PeerAddress remoteNode, final RequestHandlerUDP requestHandler,
			final FutureResponse futureResponse, final Message message, 
			final boolean broadcast, final ChannelCreator channelCreator)
	{
		if (logger.isDebugEnabled())
			logger.debug("send UDP " + Thread.currentThread().getName());
		sendUDP0(remoteNode, requestHandler, futureResponse, message, broadcast, channelCreator);
	}

	

	private void sendTCP0(final PeerAddress remoteNode, final RequestHandlerTCP requestHandler, final FutureResponse futureResponse, final Message message, final ChannelCreator channelCreator, final int idleTCPMillis)
	{
		if (futureResponse.isCompleted())
			return;
		try
		{
			ReplyTimeoutHandler replyTimeoutHandler = null;
			//no need for timeout if its fire and forget, since we close the connection anyway after writing
			if (requestHandler != null)
			{
				replyTimeoutHandler = new ReplyTimeoutHandler(timer, configuration.getIdleUDPMillis(),
						remoteNode);
				futureResponse.setReplyTimeoutHandler(replyTimeoutHandler);
			}
			else if (message.getType()!=Type.REQUEST_FF_1)
			{
				throw new RuntimeException("This send needs to be a fire and forget request");
			}
			final ChannelFuture channelFutureConnect = channelCreator.createTCPChannel(replyTimeoutHandler, futureResponse,   
					configuration.getConnectTimeoutMillis(), configuration.getIdleTCPMillis(), 
					message, requestHandler);
			if (channelFutureConnect == null)
			{
				futureResponse.setFailed("could not get channel in "
						+ configuration.getConnectTimeoutMillis() + "ms");
				return;
			}
			final Cancellable cancel = new Cancellable()
			{
				@Override
				public void cancel()
				{
					channelFutureConnect.cancel();
				}
			};
			futureResponse.addCancellation(cancel);
			channelFutureConnect.addListener(new ChannelFutureListener()
			{
				@Override
				public void operationComplete(final ChannelFuture future)
				{
					futureResponse.removeCancellation(cancel);
					if (future.isSuccess() && !channelFutureConnect.isCancelled())
					{
						if (logger.isDebugEnabled())
							logger.debug("send TCP message " + message);
						final ChannelFuture writeFuture = future.getChannel().write(message);
						afterSend(writeFuture, futureResponse, true, message, requestHandler == null);
					}
					else
					{
						//most likely its closed, but just to be sure
						future.getChannel().close();
						if (channelFutureConnect.isCancelled())
						{
							futureResponse.cancel();
						}
						else
						{
							logger.warn("Failed to connect channel " 
									+ future.getChannel().isBound() + "/"
									+ future.getChannel().isConnected() + "/"
									+ future.getChannel().isOpen() + " / " + future.isCancelled()
									+ " /ch:" + channelFutureConnect.getChannel());
							futureResponse.setFailed("Connect failed " + future.getCause());
							if (logger.isDebugEnabled() && future.getCause()!=null)
							{
								future.getCause().printStackTrace();
							}
							
						}
					}
				}
			});
		}
		catch (Exception ce)
		{
			futureResponse.setFailed("Could not get channel " + ce.toString());
			if (logger.isWarnEnabled())
				logger.warn(ce.toString());
			if (logger.isDebugEnabled())
				ce.printStackTrace();
			return;
		}
	}

	private void sendUDP0(final PeerAddress remoteNode, final RequestHandlerUDP requestHandler, 
			final FutureResponse futureResponse, final Message message, final boolean broadcast, 
			final ChannelCreator channelCreator)
	{
		if (futureResponse.isCompleted())
			return;
		ReplyTimeoutHandler replyTimeoutHandler = null;
		if (requestHandler != null)
		{
			replyTimeoutHandler = new ReplyTimeoutHandler(timer, configuration.getIdleUDPMillis(),
					remoteNode);
			futureResponse.setReplyTimeoutHandler(replyTimeoutHandler);
		}
		else if (message.getType() != Type.REQUEST_FF_1 && message.getType() != Type.REQUEST_FF_2)
		{
			throw new RuntimeException("This send needs to be a fire and forget request");
		}
		try
		{
			final Channel channel = channelCreator.createUDPChannel(replyTimeoutHandler,
					requestHandler, futureResponse, broadcast);
			final ChannelFuture writeFuture = channel.write(message, remoteNode.createSocketUDP());
			afterSend(writeFuture, futureResponse, false, message, requestHandler == null);
		}
		catch (Exception ce)
		{
			futureResponse.setFailed("Could not get channel " + ce.toString());
			logger.warn(ce.toString());
			if(logger.isDebugEnabled())
			{
				ce.printStackTrace();
			}
			return;
		}
		if (logger.isDebugEnabled())
			logger.debug("send UDP message " + message);
	}

	private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse,
			final boolean tcp, final Message message, final boolean isFireAndForget)
	{
		final Cancellable cancel = new Cancellable()
		{
			@Override
			public void cancel()
			{
				writeFuture.cancel();
			}
		};
		futureResponse.addCancellation(cancel);
		writeFuture.addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(final ChannelFuture writeFuture)
			{
				futureResponse.removeCancellation(cancel);
				if (!writeFuture.isSuccess())
				{
					//most likely its closed, but just to be sure
					writeFuture.getChannel().close();
					if (writeFuture.isCancelled())
					{
						futureResponse.cancel();
					}
					else
					{
						futureResponse.setFailed("Write failed");
						logger.warn("Failed to write channel the request "
								+ futureResponse.getRequest());
					}
				}
				if (isFireAndForget)
				{
					futureResponse.setResponse(null);
					writeFuture.getChannel().close();
				}
			}
		});
	}

	public ConnectionConfiguration getConfiguration()
	{
		return configuration;
	}
	
	public void shutdownAndWait()
	{
		shutdown = true;
	}
}