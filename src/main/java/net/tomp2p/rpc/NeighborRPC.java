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
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Content;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.utils.Utils;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NeighborRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(NeighborRPC.class);
	final public static int NEIGHBOR_SIZE = 20;

	public NeighborRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.NEIGHBORS_STORAGE, Command.NEIGHBORS_TRACKER);
	}

	/**
	 * 
	 * @param remoteNode
	 * @param locationKey
	 * @param domainKey
	 * @param contentKeys
	 * @param neighborType
	 * @param requestType
	 * @param forceSocket
	 * @return
	 */
	public FutureResponse closeNeighbors(PeerAddress remoteNode, Number160 locationKey,
			Number160 domainKey, Collection<Number160> contentKeys, Command command,
			boolean isDigest, boolean forceSocket, ChannelCreator channelCreator)
	{
		nullCheck(remoteNode, locationKey);
		if (command != Command.NEIGHBORS_TRACKER && command != Command.NEIGHBORS_STORAGE)
			throw new IllegalArgumentException("command not of type neighbor");
		Message message = createMessage(remoteNode, command, isDigest ? Type.REQUEST_1
				: Type.REQUEST_2);
		message.setKeyKey(locationKey, domainKey == null ? Number160.ZERO : domainKey);
		if (contentKeys != null)
			message.setKeys(contentKeys);
		if (!forceSocket)
		{
			FutureResponse futureResponse = new FutureResponse(message);
			NeighborsRequestUDP request = new NeighborsRequestUDP(futureResponse, peerBean, connectionBean, message);
			return request.sendUDP(channelCreator);
		}
		else
		{
			FutureResponse futureResponse = new FutureResponse(message);
			NeighborsRequestTCP request = new NeighborsRequestTCP(futureResponse, peerBean, connectionBean, message);
			return request.sendTCP(channelCreator);
		}
	}

	@Override
	public boolean checkMessage(Message message)
	{
		return message.getKey1() != null
				&& message.getContentType1() == Content.KEY_KEY
				&& (message.getContentType2() == Content.EMPTY || message.getContentType2() == Content.SET_KEYS)
				&& (message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2)
				&& (message.getCommand() == Command.NEIGHBORS_STORAGE || message.getCommand() == Command.NEIGHBORS_TRACKER);
	}

	@Override
	public Message handleResponse(Message message, boolean sign) throws IOException
	{
		if (logger.isDebugEnabled())
			logger.debug("handleResponse for " + message);
		Number160 locationKey = message.getKey1();
		Number160 domainKey = message.getKey2();
		// Create response message and set neighbors
		final Message responseMessage = createMessage(message.getSender(), message.getCommand(), Type.OK);
		if(sign) {
    		responseMessage.setPublicKeyAndSign(peerBean.getKeyPair());
    	}
		responseMessage.setMessageId(message.getMessageId());
		Collection<PeerAddress> neighbors = peerBean.getPeerMap().closePeers(locationKey,
				NEIGHBOR_SIZE);
		responseMessage.setNeighbors(neighbors, NEIGHBOR_SIZE);
		// check for fastget, -1 if, no domain provided, so we cannot
		// check content length, 0 for content not here , > 0 content here
		// int contentLength = -1;
		Collection<Number160> contentKeys = message.getKeys();
		// it is important to set an integer if a value is present
		boolean isDigest = message.getType() == Type.REQUEST_1;
		if (isDigest)
		{
			if (message.getCommand() == Command.NEIGHBORS_STORAGE)
			{
				// TODO make difference between get and put
				// boolean withDigest = message.getType() == Type.REQUEST_1;
				// if (withDigest)
				{
					DigestInfo digestInfo = Utils.digest(peerBean.getStorage(), locationKey,
							domainKey, contentKeys);
					responseMessage.setInteger(digestInfo.getSize());
					responseMessage.setKey(digestInfo.getKeyDigest());
				}
			}
			else if (message.getCommand() == Command.NEIGHBORS_TRACKER)
			{
				DigestInfo digest = peerBean.getTrackerStorage().digest(
						new Number320(locationKey, domainKey));
				int size = digest.getSize();
				if (logger.isDebugEnabled())
					logger.debug("found trackre size " + size);
				responseMessage.setInteger(size);
			}
			else
				throw new RuntimeException("Implement new type");
		}
		return responseMessage;
	}

	private void preHandleMessage(Message message, PeerMap peerMap, PeerAddress referrer)
	{
		if (message.getType() == Type.OK
				&& (message.getCommand() == Command.NEIGHBORS_STORAGE || message.getCommand() == Command.NEIGHBORS_TRACKER))
		{
			Collection<PeerAddress> tmp = message.getNeighbors();
			if (tmp != null)
			{
				Iterator<PeerAddress> iterator = tmp.iterator();
				while (iterator.hasNext())
				{
					PeerAddress addr = iterator.next();
					// if peer is removed due to failure, don't consider
					// that peer for routing anymore
					if (peerMap.isPeerRemovedTemporarly(addr))
					{
						iterator.remove();
					}
					// otherwise try to add it to the map
					else
						peerMap.peerFound(addr, referrer);
				}
			}
			else
				logger.warn("Neighbor message received, but does not contain any neighbors.");
		}
		else
			logger.warn("Message not of type Neighbor, ignoring");
	}
	private class NeighborsRequestTCP extends RequestHandlerTCP
	{
		final private Message message;

		public NeighborsRequestTCP(FutureResponse futureResponse, PeerBean peerBean, ConnectionBean connectionBean, Message message)
		{
			super(futureResponse, peerBean, connectionBean, message);
			this.message = message;
		}

		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent ce) throws Exception
		{
			preHandleMessage(message, getPeerMap(), this.message.getRecipient());
			super.handleUpstream(ctx, ce);
		}
	}
	private class NeighborsRequestUDP extends RequestHandlerUDP
	{
		final private Message message;

		public NeighborsRequestUDP(FutureResponse futureResponse, PeerBean peerBean, ConnectionBean connectionBean, Message message)
		{
			super(futureResponse, peerBean, connectionBean, message);
			this.message = message;
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
		{
			Object object = e.getMessage();
			if (object instanceof Message)
				preHandleMessage((Message) object, getPeerMap(), this.message.getRecipient());
			else
				logger.error("Response received, but not a message: " + object);
			super.messageReceived(ctx, e);
		}
	}
}