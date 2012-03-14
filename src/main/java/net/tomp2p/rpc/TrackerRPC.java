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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.P2PConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.TrackerData;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.storage.TrackerStorage.ReferrerType;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackerRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(TrackerRPC.class);
	final public static int MAX_MSG_SIZE_UDP = 35;
	final private P2PConfiguration p2pConfiguration;

	/**
	 * 
	 * @param peerBean
	 * @param connectionBean
	 */
	public TrackerRPC(PeerBean peerBean, ConnectionBean connectionBean, P2PConfiguration p2pConfiguration)
	{
		super(peerBean, connectionBean);
		this.p2pConfiguration = p2pConfiguration;
		registerIoHandler(Command.TRACKER_ADD, Command.TRACKER_GET);
	}

	public PeerAddress getPeerAddress()
	{
		return getPeerBean().getServerPeerAddress();
	}

	public FutureResponse addToTracker(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final byte[] attachement, boolean signMessage, boolean primary,
			Set<Number160> knownPeers, ChannelCreator channelCreator, boolean forceUDP, boolean forceTCP)
	{
		if (attachement == null)
		{
			return addToTracker(remotePeer, locationKey, domainKey, null, 0, 0, signMessage, primary, knownPeers, channelCreator, forceUDP, forceTCP);
		}
		else
		{
			return addToTracker(remotePeer, locationKey, domainKey, attachement, 0, attachement.length, signMessage,
					primary, knownPeers, channelCreator, forceUDP, forceTCP);
		}
	}

	public static boolean isPrimary(FutureResponse response)
	{
		return response.getRequest().getType() == Type.REQUEST_3;
	}

	public static boolean isSecondary(FutureResponse response)
	{
		return response.getRequest().getType() == Type.REQUEST_1;
	}

	public FutureResponse addToTracker(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final byte[] attachement, int offset, int legth, boolean signMessage,
			boolean primary, Set<Number160> knownPeers, ChannelCreator channelCreator, boolean forceUDP, boolean forceTCP)
	{
		nullCheck(remotePeer, locationKey, domainKey);
		final Message message = createMessage(remotePeer, Command.TRACKER_ADD, primary ? Type.REQUEST_3
				: Type.REQUEST_1);
		if (signMessage) {
			message.setPublicKeyAndSign(getPeerBean().getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		if (knownPeers != null && (knownPeers instanceof SimpleBloomFilter))
			message.setPayload(ChannelBuffers.wrappedBuffer(((SimpleBloomFilter<Number160>) knownPeers).toByteArray()));
		else
			message.setPayload(ChannelBuffers.EMPTY_BUFFER);
		
		if ((attachement != null || forceTCP) && !forceUDP)
		{
			FutureResponse futureResponse = new FutureResponse(message);
			final TrackerRequestTCP<FutureResponse> requestHandler = new TrackerRequestTCP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message,
					locationKey, domainKey);
			message.setPayload(ChannelBuffers.wrappedBuffer(attachement, offset, legth));
			return requestHandler.sendTCP(channelCreator);
		}
		else
		{
			FutureResponse futureResponse = new FutureResponse(message);
			final TrackerRequestUDP<FutureResponse> requestHandler = new TrackerRequestUDP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message,
					locationKey, domainKey);
			return requestHandler.sendUDP(channelCreator);
		}
	}
	
	public FutureResponse getFromTracker(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, boolean expectAttachement, boolean signMessage, Set<Number160> knownPeers, 
			ChannelCreator channelCreator)
	{
		return getFromTracker(remotePeer, locationKey, domainKey, expectAttachement, 
				signMessage, knownPeers, channelCreator, false, false);
	}

	public FutureResponse getFromTracker(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, boolean expectAttachement, boolean signMessage, Set<Number160> knownPeers, 
			ChannelCreator channelCreator, boolean forceUDP, boolean forceTCP)
	{
		nullCheck(remotePeer, locationKey, domainKey);
		final Message message = createMessage(remotePeer, Command.TRACKER_GET, Type.REQUEST_1);
		if (signMessage) {
			message.setPublicKeyAndSign(getPeerBean().getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		if (knownPeers != null && (knownPeers instanceof SimpleBloomFilter))
			message.setPayload(ChannelBuffers.wrappedBuffer(((SimpleBloomFilter<Number160>) knownPeers).toByteArray()));
		else
			message.setPayload(ChannelBuffers.EMPTY_BUFFER);
		
		if ((expectAttachement || forceTCP) && !forceUDP)
		{
			FutureResponse futureResponse = new FutureResponse(message);
			final TrackerRequestTCP<FutureResponse> requestHandler = new TrackerRequestTCP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message,
					locationKey, domainKey);
			return requestHandler.sendTCP(channelCreator);
		}
		else
		{
			FutureResponse futureResponse = new FutureResponse(message);
			final TrackerRequestUDP<FutureResponse> requestHandler = new TrackerRequestUDP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message,
					locationKey, domainKey);
			return requestHandler.sendUDP(channelCreator);
		}
	}

	@Override
	public Message handleResponse(Message message, boolean sign) throws Exception
	{
		if(!((message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_3)
				&& message.getKeyKey1() != null && message.getKeyKey2() != null))
		{
			throw new IllegalArgumentException("Message content is wrong");
		}
		final Message responseMessage = createResponseMessage(message, Type.OK);
		if(sign) 
		{
    		responseMessage.setPublicKeyAndSign(getPeerBean().getKeyPair());
    	}
		// get data
		Number160 locationKey = message.getKeyKey1();
		Number160 domainKey = message.getKeyKey2();
		SimpleBloomFilter<Number160> knownPeers = null;
		if (message.getPayload1() == null)
			throw new RuntimeException("BF data may be empty but it has to be there.");
		ChannelBuffer buffer = message.getPayload1();
		int length = buffer.writerIndex();
		if (length > 0)
		{
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
		final TrackerStorage trackerStorage = getPeerBean().getTrackerStorage();

		Map<Number160, TrackerData> meshPeers = trackerStorage.meshPeers(locationKey, domainKey);
		if(knownPeers != null){
			meshPeers = Utils.disjunction(meshPeers, knownPeers);
		}
		int size = meshPeers.size();
		if(p2pConfiguration.isLimitTracker())
		{
			meshPeers = Utils.limit(meshPeers, TrackerRPC.MAX_MSG_SIZE_UDP);
		}
		boolean couldProvideMoreData = size > meshPeers.size();
		if (couldProvideMoreData)
		{
			responseMessage.setType(Message.Type.PARTIALLY_OK);
		}
		responseMessage.setTrackerData(meshPeers.values());
		PeerAddress senderAddress = message.getSender();
		if (message.getCommand() == Command.TRACKER_ADD)
		{
			if (!trackerStorage.put(locationKey, domainKey, senderAddress, publicKey, attachement))
			{
				responseMessage.setType(Message.Type.DENIED);
				if (logger.isDebugEnabled())
					logger.debug("tracker NOT put on(" + getPeerBean().getServerPeerAddress() + ") locationKey:"
							+ locationKey + ", domainKey:" + domainKey + ", address:" + senderAddress);
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("tracker put on(" + getPeerBean().getServerPeerAddress() + ") locationKey:" + locationKey
							+ ", domainKey:" + domainKey + ", address:" + senderAddress + "sizeP: "
							+ trackerStorage.sizePrimary(locationKey, domainKey));
			}

		}
		else
		{
			if (logger.isDebugEnabled())
				logger.debug("tracker get on(" + getPeerBean().getServerPeerAddress() + ") locationKey:" + locationKey
						+ ", domainKey:" + domainKey + ", address:" + senderAddress + " returning: "
						+ (meshPeers == null ? "0" : meshPeers.size()));
		}
		return responseMessage;
	}

	private class TrackerRequestTCP<K extends FutureResponse> extends RequestHandlerTCP<K>
	{
		final private Message message;
		final private Number160 locationKey;
		final private Number160 domainKey;

		public TrackerRequestTCP(K futureResponse, PeerBean peerBean, ConnectionBean connectionBean, Message message,
				Number160 locationKey, Number160 domainKey)
		{
			super(futureResponse, peerBean, connectionBean, message);
			this.message = message;
			this.locationKey = locationKey;
			this.domainKey = domainKey;
		}

		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent ce) throws Exception {
			preHandleMessage(message, getPeerBean().getTrackerStorage(), this.message.getRecipient(), locationKey, domainKey);
			super.handleUpstream(ctx, ce);
		}
	}

	private class TrackerRequestUDP<K extends FutureResponse> extends RequestHandlerUDP<K>
	{
		final private Message message;
		final private Number160 locationKey;
		final private Number160 domainKey;

		public TrackerRequestUDP(K futureResponse, PeerBean peerBean, ConnectionBean connectionBean, Message message,
				Number160 locationKey, Number160 domainKey)
		{
			super(futureResponse, peerBean, connectionBean, message);
			this.message = message;
			this.locationKey = locationKey;
			this.domainKey = domainKey;
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
		{
			Object object = e.getMessage();
			if (object instanceof Message)
				preHandleMessage((Message) object, getPeerBean().getTrackerStorage(), this.message.getRecipient(),
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
		// no data found
		if (tmp == null || tmp.size() == 0)
			return;
		for (TrackerData data : tmp)
		{
			// we don't know the public key, since this is not first hand
			// information.
			// TTL will be set in tracker storage, so don't worry about it here.
			trackerStorage.putReferred(locationKey, domainKey, data.getPeerAddress(), referrer, data.getAttachement(),
					data.getOffset(), data.getLength(), ReferrerType.MESH);
		}
	}
}