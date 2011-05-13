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
import java.security.PublicKey;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.TrackerData;
import net.tomp2p.storage.TrackerStorage;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackerRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(TrackerRPC.class);
	final public static int MAX_MSG_SIZE_UDP = 35;

	/**
	 * 
	 * @param peerBean
	 * @param atLeastTrackerSize
	 *            Upper size is 27, lower size can be specified.
	 * @param dataSize
	 */
	public TrackerRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.TRACKER_ADD, Command.TRACKER_GET);
	}

	public PeerAddress getPeerAddress()
	{
		return peerBean.getServerPeerAddress();
	}

	public FutureResponse addToTracker(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final byte[] attachement, boolean signMessage, boolean primary,
			Set<Number160> knownPeers)
	{
		if (attachement == null)
			return addToTracker(remoteNode, locationKey, domainKey, null, 0, 0, signMessage, primary, knownPeers);
		else
			return addToTracker(remoteNode, locationKey, domainKey, attachement, 0, attachement.length, signMessage,
					primary, knownPeers);
	}

	public static boolean isPrimary(FutureResponse response)
	{
		return response.getRequest().getType() == Type.REQUEST_3;
	}

	public static boolean isSecondary(FutureResponse response)
	{
		return response.getRequest().getType() == Type.REQUEST_1;
	}

	public FutureResponse addToTracker(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final byte[] attachement, int offset, int legth, boolean signMessage,
			boolean primary, Set<Number160> knownPeers)
	{
		nullCheck(remoteNode, locationKey, domainKey);
		final Message message = createMessage(remoteNode, Command.TRACKER_ADD, primary ? Type.REQUEST_3
				: Type.REQUEST_1);
		message.setKeyKey(locationKey, domainKey);
		if (knownPeers != null && (knownPeers instanceof SimpleBloomFilter))
			message.setPayload(ChannelBuffers.wrappedBuffer(((SimpleBloomFilter<Number160>) knownPeers).toByteArray()));
		else
			message.setPayload(ChannelBuffers.EMPTY_BUFFER);
		if (signMessage)
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		if (attachement != null)
		{
			final TrackerRequestTCP requestHandler = new TrackerRequestTCP(peerBean, connectionBean, message,
					locationKey, domainKey);
			message.setPayload(ChannelBuffers.wrappedBuffer(attachement, offset, legth));
			return requestHandler.sendTCP();
		}
		else
		{
			final TrackerRequestUDP requestHandler = new TrackerRequestUDP(peerBean, connectionBean, message,
					locationKey, domainKey);
			return requestHandler.sendUDP();
		}
	}

	public FutureResponse getFromTracker(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, boolean expectAttachement, boolean signMessage, Set<Number160> knownPeers)
	{
		nullCheck(remoteNode, locationKey, domainKey);
		final Message message = createMessage(remoteNode, Command.TRACKER_GET, Type.REQUEST_1);
		message.setKeyKey(locationKey, domainKey);
		if (knownPeers != null && (knownPeers instanceof SimpleBloomFilter))
			message.setPayload(ChannelBuffers.wrappedBuffer(((SimpleBloomFilter<Number160>) knownPeers).toByteArray()));
		else
			message.setPayload(ChannelBuffers.EMPTY_BUFFER);
		if (signMessage)
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		if (expectAttachement)
		{
			final TrackerRequestTCP requestHandler = new TrackerRequestTCP(peerBean, connectionBean, message,
					locationKey, domainKey);
			return requestHandler.sendTCP();
		}
		else
		{
			final TrackerRequestUDP requestHandler = new TrackerRequestUDP(peerBean, connectionBean, message,
					locationKey, domainKey);
			return requestHandler.sendUDP();
		}
	}

	@Override
	public boolean checkMessage(Message message)
	{
		return (message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_3)
				&& message.getKey1() != null && message.getKey2() != null;
	}

	@Override
	public Message handleResponse(Message message) throws Exception
	{
		final Message responseMessage = createMessage(message.getSender(), message.getCommand(), Type.OK);
		responseMessage.setMessageId(message.getMessageId());
		// get data
		Number160 locationKey = message.getKey1();
		Number160 domainKey = message.getKey2();
		SimpleBloomFilter<Number160> knownPeers = null;
		if (message.getPayload1() == null)
			throw new RuntimeException("BF data may be empty but it has to be there.");
		ChannelBuffer buffer = message.getPayload1();
		int length = buffer.writerIndex();
		if(length>0) {
			knownPeers = new SimpleBloomFilter<Number160>(buffer.array(), buffer.arrayOffset(), length);
		}
		byte[] attachement = null;
		if (message.getPayload2() != null)
		{
			buffer = message.getPayload2();
			attachement = new byte[buffer.readableBytes()];
			// make a copy of the attachement
			buffer.readBytes(attachement);
		}
		PublicKey publicKey = message.getPublicKey();
		//
		final TrackerStorage trackerStorage = peerBean.getTrackerStorage();
		TrackerDataResult trackerData1 = trackerStorage.getSelection(locationKey, domainKey,
				TrackerRPC.MAX_MSG_SIZE_UDP, knownPeers);
		if (trackerData1.couldProvideMoreData())
		{
			responseMessage.setType(Message.Type.PARTIALLY_OK);
		}
		Map<Number160, TrackerData> peerDataMap = trackerData1.getPeerDataMap();
		responseMessage.setTrackerData(peerDataMap.values());
		PeerAddress senderAddress = message.getSender();
		if (message.getCommand() == Command.TRACKER_ADD)
		{
			if (!trackerStorage.put(locationKey, domainKey, senderAddress, publicKey, attachement))
			{
				responseMessage.setType(Message.Type.DENIED);
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("tracker put on(" + peerBean.getServerPeerAddress() + ") locationKey:" + locationKey
							+ ", domainKey:" + domainKey + ", address:" + senderAddress + "size: "
							+ trackerStorage.size(locationKey, domainKey));
			}
			int currentSize = trackerStorage.size(locationKey, domainKey);
			if (currentSize >= trackerStorage.getTrackerStoreSizeMax(locationKey, domainKey))
			{
				if (logger.isDebugEnabled())
					logger.debug("tracker NOT put on(" + peerBean.getServerPeerAddress() + ") locationKey:"
							+ locationKey + ", domainKey:" + domainKey + ", address:" + senderAddress
							+ ", current size: " + currentSize);
				responseMessage.setType(Message.Type.DENIED);
			}
		}
		else
		{
			if (logger.isDebugEnabled())
				logger.debug("tracker get on(" + peerBean.getServerPeerAddress() + ") locationKey:" + locationKey
						+ ", domainKey:" + domainKey + ", address:" + senderAddress + " returning: "
						+ (peerDataMap == null ? "0" : peerDataMap.size()));
		}
		return responseMessage;
	}

	private class TrackerRequestTCP extends RequestHandlerTCP
	{
		final private Message message;
		final private Number160 locationKey;
		final private Number160 domainKey;

		public TrackerRequestTCP(PeerBean peerBean, ConnectionBean connectionBean, Message message,
				Number160 locationKey, Number160 domainKey)
		{
			super(peerBean, connectionBean, message);
			this.message = message;
			this.locationKey = locationKey;
			this.domainKey = domainKey;
		}

		@Override
		public void messageReceived(Message message) throws Exception
		{
			preHandleMessage(message, peerBean.getTrackerStorage(), this.message.getRecipient(), locationKey, domainKey);
			super.messageReceived(message);
		}
	}

	private class TrackerRequestUDP extends RequestHandlerUDP
	{
		final private Message message;
		final private Number160 locationKey;
		final private Number160 domainKey;

		public TrackerRequestUDP(PeerBean peerBean, ConnectionBean connectionBean, Message message,
				Number160 locationKey, Number160 domainKey)
		{
			super(peerBean, connectionBean, message);
			this.message = message;
			this.locationKey = locationKey;
			this.domainKey = domainKey;
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
		{
			Object object = e.getMessage();
			if (object instanceof Message)
				preHandleMessage((Message) object, peerBean.getTrackerStorage(), this.message.getRecipient(),
						locationKey, domainKey);
			else
				logger.error("Response received, but not a message: " + object);
			super.messageReceived(ctx, e);
		}
	}

	private void preHandleMessage(Message message, TrackerStorage trackerStorage, PeerAddress referrer,
			Number160 locationKey, Number160 domainKey) throws IOException, ClassNotFoundException
	{
		// Since I might become a tracker as well, we keep this information
		// about those trackers.
		Collection<TrackerData> tmp = message.getTrackerData();
		if (tmp == null || tmp.size() == 0)
			return;
		for (TrackerData data : tmp)
		{
			// we don't know the public key, since this is not first hand
			// information.
			// TTL will be set in tracker storage, so don't worry about it here.
			PeerAddress peerAddress = data.getPeerAddress();
			trackerStorage.putReferred(locationKey, domainKey, peerAddress, referrer, data.getAttachement(),
					data.getOffset(), data.getLength());
		}
	}
}
