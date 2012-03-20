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
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancellable;
import net.tomp2p.futures.FutureChannel;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.ConnectionConfiguration;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles sending of messages. In order to send messages one needs to provide a
 * channel creator. This can be obtained via the connection reservation, that
 * keeps track how many connections have been opened.
 * 
 * @author Thomas Bocek
 * 
 */
public class SenderNetty implements Sender
{
	final private static Logger logger = LoggerFactory.getLogger(SenderNetty.class);
	// Timer used for ReplyTimeout
	final private Timer timer;
	final private ConnectionConfiguration configuration;
	private volatile boolean shutdown = false;
	/**
	 * The sender is shared for all master and child peers
	 * 
	 * @param configuration ConnectionConfigurationBean
	 * @param timer Timer
	 */
	public SenderNetty(final ConnectionConfiguration configuration, Timer timer)
	{
		this.configuration = configuration;
		this.timer = timer;
	}

	/* (non-Javadoc)
	 * @see net.tomp2p.connection.Sender#sendTCP(net.tomp2p.rpc.RequestHandlerTCP, net.tomp2p.futures.FutureResponse, net.tomp2p.message.Message, net.tomp2p.connection.ChannelCreator, int)
	 */
	@Override
	public void sendTCP(final RequestHandlerTCP<? extends BaseFuture> handler, final FutureResponse futureResponse,
			final Message message, final ChannelCreator channelCreator, final int idleTCPMillis)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("send TCP " + Thread.currentThread().getName());
		}
		if (shutdown)
		{
			futureResponse.setFailed("shutdown in progres");
			return;
		}
		sendTCP0(message.getRecipient(), handler, futureResponse, message, channelCreator,
				idleTCPMillis);
	}

	/* (non-Javadoc)
	 * @see net.tomp2p.connection.Sender#sendUDP(net.tomp2p.rpc.RequestHandlerUDP, net.tomp2p.futures.FutureResponse, net.tomp2p.message.Message, net.tomp2p.connection.ChannelCreator)
	 */
	@Override
	public void sendUDP(final RequestHandlerUDP<? extends BaseFuture> handler, final FutureResponse futureResponse,
			final Message message, final ChannelCreator channelCreator)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("send UDP " + Thread.currentThread().getName());
		}
		if (shutdown)
		{
			futureResponse.setFailed("shutdown in progres");
			return;
		}
		sendUDP0(message.getRecipient(), handler, futureResponse, message, false, channelCreator);
	}

	/* (non-Javadoc)
	 * @see net.tomp2p.connection.Sender#sendBroadcastUDP(net.tomp2p.rpc.RequestHandlerUDP, net.tomp2p.futures.FutureResponse, net.tomp2p.message.Message, net.tomp2p.connection.ChannelCreator)
	 */
	@Override
	public void sendBroadcastUDP(final RequestHandlerUDP<? extends BaseFuture> handler, final FutureResponse futureResponse, 
			final Message message, final ChannelCreator channelCreator)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("send UDP " + Thread.currentThread().getName());
		}
		if (shutdown)
		{
			futureResponse.setFailed("shutdown in progres");
			return;
		}
		sendUDP0(message.getRecipient(), handler, futureResponse, message, true, channelCreator);
	}

	/**
	 * Internal send.
	 * 
	 * @param remotePeer PeerAddress
	 * @param requestHandler RequestHandlerTCP
	 * @param futureResponse FutureResponse
	 * @param message Message
	 * @param channelCreator ChannelCreator
	 * @param idleTCPMillis Timeout when a connection is considered idle (no data send or receivedF)
	 */
	private void sendTCP0(final PeerAddress remotePeer, final RequestHandlerTCP<? extends BaseFuture> requestHandler,
			final FutureResponse futureResponse, final Message message,
			final ChannelCreator channelCreator, final int idleTCPMillis)
	{
		if (futureResponse.isCompleted())
			return;
		try
		{
			ReplyTimeoutHandler replyTimeoutHandler = null;
			// no need for timeout if its fire and forget, since we close the
			// connection anyway after writing
			if (requestHandler != null)
			{
				replyTimeoutHandler = new ReplyTimeoutHandler(timer, idleTCPMillis, remotePeer);
				futureResponse.setReplyTimeoutHandler(replyTimeoutHandler);
			}
			else if (message.getType() != Type.REQUEST_FF_1)
			{
				throw new RuntimeException("This send needs to be a fire and forget request");
			}
			final FutureChannel channelFutureConnect = channelCreator.createTCPChannel(
					replyTimeoutHandler, requestHandler, configuration.getConnectTimeoutMillis(), 
					message.getRecipient().createSocketTCP());
			final Cancellable cancel = new Cancellable()
			{
				@Override
				public void cancel()
				{
					channelFutureConnect.cancel();
				}
			};
			futureResponse.addCancellation(cancel);
			channelFutureConnect.addListener(new BaseFutureAdapter<FutureChannel>()
			{
				@Override
				public void operationComplete(FutureChannel future) throws Exception
				{
					futureResponse.removeCancellation(cancel);
					if(future.isSuccess())
					{
						if (logger.isDebugEnabled())
						{
							logger.debug("send TCP message " + message);
						}
						final ChannelFuture writeFuture = future.getChannel().write(message);
						afterSend(writeFuture, futureResponse, requestHandler == null);
					}
					else
					{
						futureResponse.setFailed(future);
						if(logger.isWarnEnabled())
						{
							logger.warn("Failed to connect channel:"+message);
						}
					}
				}
			});
		}
		catch (Exception ce)
		{
			futureResponse.setFailed("Could not get channel " + ce.toString());
			if (logger.isWarnEnabled())
			{
				logger.warn(ce.toString());
			}
			if (logger.isDebugEnabled())
			{
				ce.printStackTrace();
			}
			return;
		}
	}

	/**
	 * Internal send.
	 * 
	 * @param remotePeer PeerAddress
	 * @param requestHandler RequestHandlerUDP
	 * @param futureResponse FutureResponse
	 * @param message Message
	 * @param broadcast True if message should be broadcasted (layer 2)
	 * @param channelCreator ChannelCreator
	 */
	private void sendUDP0(final PeerAddress remotePeer, final RequestHandlerUDP<? extends BaseFuture> requestHandler,
			final FutureResponse futureResponse, final Message message, final boolean broadcast,
			final ChannelCreator channelCreator)
	{
		if (futureResponse.isCompleted())
			return;
		ReplyTimeoutHandler replyTimeoutHandler = null;
		if (requestHandler != null)
		{
			replyTimeoutHandler = new ReplyTimeoutHandler(timer, configuration.getIdleUDPMillis(),
					remotePeer);
			futureResponse.setReplyTimeoutHandler(replyTimeoutHandler);
		}
		else if (message.getType() != Type.REQUEST_FF_1 && message.getType() != Type.REQUEST_FF_2)
		{
			throw new RuntimeException("This send needs to be a fire and forget request");
		}
		try
		{
			final FutureChannel futureChannelCreation = channelCreator.createUDPChannel(replyTimeoutHandler,
					requestHandler, broadcast);
			futureChannelCreation.addListener(new BaseFutureAdapter<FutureChannel>()
			{
				@Override
				public void operationComplete(FutureChannel future) throws Exception
				{
					if(future.isSuccess())
					{
						final ChannelFuture writeFuture = future.getChannel().write(message, remotePeer.createSocketUDP());
						afterSend(writeFuture, futureResponse, requestHandler == null);
					}
					else
					{
						futureResponse.setFailed("shutdown in progres");
					}
				}
			});
		}
		catch (Exception ce)
		{
			futureResponse.setFailed("Could not get channel " + ce.toString());
			logger.warn(ce.toString());
			if (logger.isDebugEnabled())
			{
				ce.printStackTrace();
			}
			return;
		}
		if (logger.isDebugEnabled())
		{
			logger.debug("send UDP message " + message);
		}
	}

	/**
	 * Waits until the write operation is complete and fails if necessary, or
	 * closes the channel in case of fire and forget.
	 * 
	 * @param writeFuture ChannelFuture
	 * @param futureResponse FutureResponse
	 * @param isFireAndForget True if we don't expect an answer
	 */
	private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse,
			final boolean isFireAndForget)
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
					// most likely its closed, but just to be sure
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
						if(logger.isWarnEnabled() && writeFuture.getCause() !=null)
						{
							writeFuture.getCause().printStackTrace();
						}
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

	@Override
	public void shutdown()
	{
		shutdown = true;
	}
}