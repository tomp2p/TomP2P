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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.Cancellable;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
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
	final private ConnectionCollector connectionCollector;
	final private ConnectionConfiguration configuration;
	final private BlockingQueue<Runnable> sendTaskQueue = new LinkedBlockingQueue<Runnable>();
	final private Thread senderThread;
	final private TCPChannelChache channelChache;
	volatile private boolean running = true;

	public Sender(final ConnectionCollector connectionCollector,
			final ConnectionConfiguration configuration, TCPChannelChache channelChache, Timer timer)
	{
		this.channelChache = channelChache;
		this.connectionCollector = connectionCollector;
		this.configuration = configuration;
		this.timer = timer;
		this.senderThread = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				while (running)
				{
					try
					{
						Runnable runner = sendTaskQueue.take();
						try
						{
							runner.run();
						}
						catch (Exception e)
						{
							logger.error("Error while sending " + e.toString());
							if (logger.isDebugEnabled())
								e.printStackTrace();
						}
						synchronized (sendTaskQueue)
						{
							sendTaskQueue.notifyAll();
						}
					}
					catch (InterruptedException e)
					{
						// check running flag fast
					}
				}
			}
		});
		this.senderThread.start();
	}

	// TODO: fire and forget does not work!!
	public void fireAndForgetUDP(final Message message)
	{
		sendUDP(message.getRecipient(), null, message, false);
	}

	public void fireAndForgetTCP(final Message message)
	{
		sendTCP(TCPChannelChache.DEFAULT_CHANNEL_NAME, message.getRecipient(), null, message);
	}

	public void sendTCP(final Message message, final RequestHandlerTCP handler)
	{
		sendTCP(TCPChannelChache.DEFAULT_CHANNEL_NAME, message, handler);
	}

	public void sendTCP(final String channelName, final Message message,
			final RequestHandlerTCP handler)
	{
		sendTCP(channelName, message.getRecipient(), handler, message);
	}

	public void sendUDP(final Message message, final RequestHandlerUDP handler)
	{
		sendUDP(message.getRecipient(), handler, message, false);
	}

	public void sendBroadcastUDP(final Message message, final RequestHandlerUDP handler)
	{
		sendUDP(message.getRecipient(), handler, message, true);
	}

	public void shutdown()
	{
		running = false;
		senderThread.interrupt();
		connectionCollector.shutdown();
	}

	public ConnectionCollector getConnectionCollector()
	{
		return connectionCollector;
	}

	private void sendTCP(final String channelName, final PeerAddress remoteNode,
			final RequestHandlerTCP replyHandler, final Message message)
	{
		// do not block if we came from the netty thread
		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
		{
			logger
					.debug("we are TCP from " + Thread.currentThread().getName()
							+ ", do not block! ");
			sendTaskQueue.offer(new Runnable()
			{
				@Override
				public void run()
				{
					sendTCP0(channelName, replyHandler, message);
				}
			});
		}
		else
		{
			logger.debug("here TCP we can block! " + Thread.currentThread().getName());
			// this may block if its from the user directly
			if (waitForConnection(replyHandler.getFutureResponse()))
			{
				if(logger.isDebugEnabled()) logger.debug("send TCP " + Thread.currentThread().getName());
				sendTCP0(channelName, replyHandler, message);
			}
		}
	}

	private void sendUDP(final PeerAddress remoteNode, final RequestHandlerUDP replyHandler,
			final Message message, final boolean broadcast)
	{
		// do not block if we came from the netty thread
		if (Thread.currentThread().getName().startsWith(ConnectionHandler.THREAD_NAME))
		{
			logger
					.debug("we are UDP from " + Thread.currentThread().getName()
							+ ", do not block! ");
			sendTaskQueue.offer(new Runnable()
			{
				@Override
				public void run()
				{
					sendUDP0(remoteNode, replyHandler, message, broadcast);
				}
			});
		}
		else
		{
			logger.debug("here UDP we can block! " + Thread.currentThread().getName());
			// this may block if its from the user directly
			if (waitForConnection(replyHandler.getFutureResponse()))
			{
				if(logger.isDebugEnabled()) logger.debug("send UDP " + Thread.currentThread().getName());
				sendUDP0(remoteNode, replyHandler, message, broadcast);
			}
		}
	}

	private boolean waitForConnection(FutureResponse futureResponse)
	{
		// why 100? well its a nice number. The queue size with 100 can go up to
		// 100*exploding factor. The exploding factor is how much threads a main
		// thread can start. for example for bootstrap it is 2 x routing, which
		// is 2 x parallel. So for parallel = 3, the queue can get up to
		// 3*2*100=600. So if you create from a NioWorker new threads, then you
		// might want to adjust this.
		while (sendTaskQueue.size() > 100)
		{
			synchronized (sendTaskQueue)
			{
				try
				{
					if (logger.isDebugEnabled())
						logger.debug("slow down, the queue size is " + sendTaskQueue.size());
					sendTaskQueue.wait();
				}
				catch (InterruptedException e)
				{
					logger.error("error in waitforconn");
					e.printStackTrace();
					if (futureResponse != null)
						futureResponse.setFailed("Interrupted");
					return false;
				}
			}
		}
		return true;
	}

	private void sendTCP0(String channelName, final RequestHandlerTCP replyHandler, final Message message)
	{
		final FutureResponse futureResponse = replyHandler.getFutureResponse();
		if (futureResponse.isCompleted())
			return;
		try
		{
		  
		  final PeerAddress remoteNode=message.getRecipient();
		  final PeerAddress localNode=message.getSender();
		  IdleStateHandler timeoutHandler = new IdleStateHandler(timer, 0, 0, configuration
					.getIdleTCPMillis(), TimeUnit.MILLISECONDS);
			final ChannelFuture channelFuture = channelChache.getChannel(remoteNode.getID(),localNode.getID(),
					remoteNode.createSocketTCP(), channelName, timeoutHandler, futureResponse,
					configuration.getConnectTimeoutMillis(), configuration.getIdleTCPMillis());
			if(channelFuture==null) {
			  futureResponse.setFailed("could not get channel in time");
			  return;
			}
			DispatcherReply dispatcherReply = (DispatcherReply) channelFuture.getChannel()
					.getPipeline().get("reply");
			dispatcherReply.add(message, replyHandler);
			final Cancellable cancel1 = new Cancellable()
			{
				@Override
				public void cancel()
				{
					channelFuture.cancel();
				}
			};
			futureResponse.addCancellation(cancel1);
			channelFuture.addListener(new ChannelFutureListener()
			{
				@Override
				public void operationComplete(final ChannelFuture future)
				{
					futureResponse.removeCancellation(cancel1);
					if (future.isSuccess() && !channelFuture.isCancelled())
					{
						if (logger.isDebugEnabled())
							logger.debug("send TCP message " + message);
						final ChannelFuture writeFuture = future.getChannel().write(message);
						afterSend(writeFuture, futureResponse, replyHandler, true);
					}
					else
					{
						future.getChannel().close();
						if (channelFuture.isCancelled())
							futureResponse.cancel();
						else
						{
							logger.warn("Failed to connect channel " + connectionCollector + "/"
									+ future.getChannel().isBound() + "/"
									+ future.getChannel().isConnected() + "/"
									+ future.getChannel().isOpen() + " / " + future.isCancelled());
							if (future.getCause() != null)
								future.getCause().printStackTrace();
							futureResponse.setFailed("Connect failed " + future);
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
			if(logger.isDebugEnabled())
				ce.printStackTrace();
			return;
		}
	}

	private void sendUDP0(final PeerAddress remoteNode, final RequestHandlerUDP replyHandler,
			final Message message, final boolean broadcast)
	{
		final FutureResponse futureResponse = replyHandler.getFutureResponse();
		if (futureResponse.isCompleted())
			return;
		ReplyTimeoutHandler replyTimeoutHandler = null;
		if (replyHandler != null)
		{
			replyTimeoutHandler = new ReplyTimeoutHandler(timer, configuration.getIdleUDPMillis(),
					remoteNode);
			futureResponse.setReplyTimeoutHandler(replyTimeoutHandler);
		}
		try
		{
			final Channel channel = connectionCollector.channelUDP(replyTimeoutHandler,
					replyHandler, broadcast);
			final ChannelFuture writeFuture = channel.write(message, remoteNode.createSocketUDP());
			afterSend(writeFuture, futureResponse, replyHandler, false);
		}
		catch (Exception ce)
		{
			futureResponse.setFailed("Could not get channel " + ce.toString());
			logger.warn(ce.toString());
			return;
		}
		if (logger.isDebugEnabled())
			logger.debug("send UDP message " + message);
	}

	private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse,
			final Object handler, boolean tcp)
	{
		final Cancellable cancel2 = new Cancellable()
		{
			@Override
			public void cancel()
			{
				writeFuture.cancel();
			}
		};
		futureResponse.addCancellation(cancel2);
		writeFuture.addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(final ChannelFuture writeFuture)
			{
				futureResponse.removeCancellation(cancel2);
				if (!writeFuture.isSuccess())
				{
					writeFuture.getChannel().close();
					if (writeFuture.isCancelled())
						futureResponse.cancel();
					else
					{
						futureResponse.setFailed("Write failed");
						logger.warn("Failed to write channel the request "
								+ futureResponse.getRequest());
					}
				}
				else
				{
					futureResponse.setReplyTimeout(System.currentTimeMillis()
							+ configuration.getTimeoutTCPMillis());
				}
				if (handler == null)
					futureResponse.setResponse(null);
			}
		});
	}

	public ConnectionConfiguration getConfiguration()
	{
		return configuration;
	}
}