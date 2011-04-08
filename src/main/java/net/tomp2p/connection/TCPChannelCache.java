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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPChannelCache
{
	final public static String DEFAULT_CHANNEL_NAME = "any";
	final private static Logger logger = LoggerFactory.getLogger(TCPChannelCache.class);
	final private ConcurrentMap<Identifier, ChannelFuture> cache = new ConcurrentHashMap<Identifier, ChannelFuture>();
	final private ConnectionCollector connectionCollector;
	final private Timer timer;
	final private ChannelGroup channelGroup;
	private DispatcherRequest dispatcherRequest;

	public TCPChannelCache(ConnectionCollector connectionCollector, Timer timer,
			ChannelGroup channelGroup)
	{
		this.connectionCollector = connectionCollector;
		this.timer = timer;
		this.channelGroup = channelGroup;
	}

	public void addChannel(Number160 recipientID, Number160 senderID, InetAddress inetAddress,
			Channel channel)
	{
		Identifier identifier = new Identifier(recipientID, senderID, inetAddress,
				DEFAULT_CHANNEL_NAME);
		ChannelFuture future = new DefaultChannelFuture(channel, false);
		future.setSuccess();
		if (cache.putIfAbsent(identifier, future) != null)
		{
			if (logger.isDebugEnabled())
				logger.debug("add to TCP cache (add) " + identifier + "/" + channel);
		}
	}

	public ChannelFuture getChannel(ChannelHandler timeoutHandler, FutureResponse futureResponse,
			int connectTimeoutMillis, int tcpIdleTimeoutMillis, Message message,
			RequestHandlerTCP requestHandler) throws InterruptedException
	{
		return getChannel(DEFAULT_CHANNEL_NAME, timeoutHandler, futureResponse,
				connectTimeoutMillis, tcpIdleTimeoutMillis, message, requestHandler);
	}

	public ChannelFuture getChannel(String channelName, ChannelHandler timeoutHandler,
			FutureResponse futureResponse, int connectTimeoutMillis, int tcpIdleTimeoutMillis,
			Message message, RequestHandlerTCP requestHandler) throws InterruptedException
	{
		final PeerAddress remoteNode = message.getRecipient();
		final Number160 recipientID = remoteNode.getID();
		final Number160 senderID = message.getSender().getID();
		final InetSocketAddress recipientAddress = remoteNode.createSocketTCP();
		Identifier identifier = new Identifier(recipientID, senderID,
				recipientAddress.getAddress(), channelName);
		ChannelFuture future = cache.get(identifier);
		if (future != null)
		{
			if (logger.isDebugEnabled())
				logger.debug("reuse connection " + future.getChannel());
			IdleStateHandler timeoutHandlerOld = (IdleStateHandler) future.getChannel()
					.getPipeline().get("timeout");
			timeoutHandlerOld.reset();
			DispatcherReplyTCP dispatcherReply = (DispatcherReplyTCP) future.getChannel().getPipeline()
					.get("reply");
			dispatcherReply.add(message, requestHandler);
			// the channel could have timeout in the meantime, check for
			// it
			if (!future.getChannel().isOpen())
			{
				dispatcherReply = new DispatcherReplyTCP(timer, tcpIdleTimeoutMillis,
						getDispatcherRequest(), channelGroup);
				dispatcherReply.add(message, requestHandler);
				return createNewChannel(recipientID, recipientAddress, timeoutHandler,
						connectTimeoutMillis, tcpIdleTimeoutMillis, identifier, dispatcherReply);
			}
			return future;
		}
		else
		{
			DispatcherReplyTCP dispatcherReply = new DispatcherReplyTCP(timer, tcpIdleTimeoutMillis,
					getDispatcherRequest(), channelGroup);
			dispatcherReply.add(message, requestHandler);
			return createNewChannel(recipientID, recipientAddress, timeoutHandler,
					connectTimeoutMillis, tcpIdleTimeoutMillis, identifier, dispatcherReply);
		}
	}

	private ChannelFuture createNewChannel(Number160 recipientID,
			InetSocketAddress recipientAddress, ChannelHandler timeoutHandler,
			int connectTimeoutMillis, int tcpIdleTimeoutMillis, final Identifier identifier,
			DispatcherReplyTCP dispatcherReply) throws InterruptedException
	{
		if (logger.isDebugEnabled())
			logger.debug("no cached channel found, create one to " + recipientID + ", "
					+ recipientAddress);
		// create channel
		ChannelFuture future = connectionCollector.channelTCP(timeoutHandler, dispatcherReply,
				recipientAddress, connectTimeoutMillis, this);
		if (future == null)
			return null;
		if (logger.isDebugEnabled())
			logger.debug("created channel " + future.getChannel());
		future.getChannel().getCloseFuture().addListener(new ChannelFutureListener()
		{
			@Override
			public void operationComplete(ChannelFuture future) throws Exception
			{
				if (logger.isDebugEnabled())
					logger.debug("connection was closed, so remove " + future.getChannel());
				// here we need to check if the channel has been reinserted
				// due to a reopen
				ChannelFuture future2 = cache.get(identifier);
				if (future2 != null && !future2.getChannel().isOpen())
					cache.remove(identifier);
			}
		});
		if (logger.isDebugEnabled())
			logger.debug("add to TCP cache (get) " + identifier);
		cache.put(identifier, future);
		return future;
	}

	public boolean expireCache()
	{
		for (ChannelFuture future : cache.values())
		{
			Channel channel = future.getChannel();
			DispatcherReplyTCP dispatcherReply = (DispatcherReplyTCP) channel.getPipeline().get("reply");
			// TODO: search for the longest idle one...
			if (!dispatcherReply.isWaiting() && channel.isOpen())
			{
				if (logger.isDebugEnabled())
					logger.debug("expire channel " + channel);
				channel.close();
				return true;
			}
		}
		if (logger.isDebugEnabled())
			logger.debug("could not expire any channel");
		return false;
	}

	public void setDispatcherRequest(DispatcherRequest dispatcherRequest)
	{
		this.dispatcherRequest = dispatcherRequest;
	}

	public DispatcherRequest getDispatcherRequest()
	{
		return dispatcherRequest;
	}
	private static class Identifier
	{
		final private Number160 recipientId;
		final private Number160 senderId;
		final private Number160 both;
		final private InetAddress inetAddress;
		final private String channelName;

		public Identifier(Number160 recipientId, Number160 senderId, InetAddress inetAddress,
				String channelName)
		{
			this.recipientId = recipientId;
			this.senderId = senderId;
			this.both = senderId.xor(recipientId);
			this.inetAddress = inetAddress;
			this.channelName = channelName;
		}

		@Override
		public int hashCode()
		{
			return both.hashCode() ^ inetAddress.hashCode() ^ channelName.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (!(obj instanceof Identifier))
				return false;
			Identifier i = (Identifier) obj;
			return i.both.equals(both) && i.inetAddress.equals(inetAddress)
					&& i.channelName.equals(channelName);
		}

		@Override
		public String toString()
		{
			StringBuilder sb = new StringBuilder("recipientID:");
			sb.append(recipientId).append(",senderID:").append(senderId).append(",inet:").append(
					inetAddress).append(",name:").append(channelName);
			return sb.toString();
		}
	}
}
