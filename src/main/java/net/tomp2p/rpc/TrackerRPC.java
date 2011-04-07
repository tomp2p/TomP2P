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
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.MessageCodec;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
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
	// final private TrackerStorage trackerStorage;
	// final private int trackerSize;
	// final private int ttlMillis;
	/**
	 * 
	 * @param peerBean
	 * @param atLeastTrackerSize Upper size is 27, lower size can be specified.
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

	public FutureResponse addToTrackerReplication(PeerAddress remoteNode, Number160 locationKey,
			Number160 domainKey, Data data, boolean signMessage)
	{
		nullCheck(remoteNode, locationKey, domainKey);
		final Message message = createMessage(remoteNode, Command.TRACKER_ADD, Type.REQUEST_2);
		message.setKeyKey(locationKey, domainKey);
		if (signMessage)
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		final TrackerRequestTCP requestHandler = new TrackerRequestTCP(peerBean, connectionBean, message, locationKey, domainKey);
		Map<Number160, Data> c = new HashMap<Number160, Data>(1);
		c.put(data.getHash(), data);
		message.setDataMap(c);
		return requestHandler.sendTCP();
	}

	public FutureResponse addToTracker(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final Data attachement, boolean signMessage, boolean primary, SimpleBloomFilter<Number160> knownPeers)
	{
		nullCheck(remoteNode, locationKey, domainKey);
		final Message message = createMessage(remoteNode, Command.TRACKER_ADD, primary
				? Type.REQUEST_3 : Type.REQUEST_1);
		message.setKeyKey(locationKey, domainKey);
		message.setPayload(ChannelBuffers.wrappedBuffer(knownPeers.toByteArray()));
		if (signMessage)
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		if (attachement != null)
		{
			final TrackerRequestTCP requestHandler = new TrackerRequestTCP(peerBean, connectionBean, message, locationKey, domainKey);
			Map<Number160, Data> c = new HashMap<Number160, Data>(1);
			c.put(attachement.getHash(), attachement);
			message.setDataMap(c);
			return requestHandler.sendTCP();
		}
		else
		{
			final TrackerRequestUDP requestHandler = new TrackerRequestUDP(peerBean, connectionBean, message, locationKey, domainKey);
			return requestHandler.sendUDP();
		}
	}

	public FutureResponse getFromTracker(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, boolean expectAttachement, boolean signMessage, SimpleBloomFilter<Number160> knownPeers)
	{
		nullCheck(remoteNode, locationKey, domainKey);
		final Message message = createMessage(remoteNode, Command.TRACKER_GET, Type.REQUEST_1);
		message.setKeyKey(locationKey, domainKey);
		message.setPayload(ChannelBuffers.wrappedBuffer(knownPeers.toByteArray()));
		if (signMessage)
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		if (expectAttachement)
		{
			final TrackerRequestTCP requestHandler = new TrackerRequestTCP(peerBean, connectionBean, message, locationKey, domainKey);
			return requestHandler.sendTCP();
		}
		else
		{
			final TrackerRequestUDP requestHandler = new TrackerRequestUDP(peerBean, connectionBean, message, locationKey, domainKey);
			return requestHandler.sendUDP();
		}
	}

	@Override
	public boolean checkMessage(Message message)
	{
		return (message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2 || message
				.getType() == Type.REQUEST_3)
				&& message.getKey1() != null && message.getKey2() != null;
	}

	@Override
	public Message handleResponse(Message message) throws Exception
	{
		boolean direct = message.getType() == Type.REQUEST_1;
		// boolean replication= message.getType() == Type.REQUEST_2;
		boolean primary = message.getType() == Type.REQUEST_3;
		final Message responseMessage = createMessage(message.getSender(), message.getCommand(),
				Type.OK);
		responseMessage.setMessageId(message.getMessageId());
		final TrackerStorage trackerStorage = peerBean.getTrackerStorage();
		Number160 locationKey = message.getKey1();
		Number160 domainKey = message.getKey2();
		if (direct || primary)
		{
			SimpleBloomFilter<Number160> knownPeers = null;
			if(message.getPayload()!=null)
			{
				ChannelBuffer buffer=message.getPayload();
				int length=buffer.writerIndex();
				knownPeers=new SimpleBloomFilter<Number160>(buffer.array(), buffer.arrayOffset(), length);
			}
			TrackerData trackerData1 = trackerStorage.getSelection(locationKey, domainKey, trackerStorage.getTrackerSize(), knownPeers);
			if(trackerData1.couldProvideMoreData())
				responseMessage.setType(Message.Type.PARTIALLY_OK);
			SortedMap<Number480, Data> peerDataMap=trackerData1.getPeerDataMap();
			// fill up with trackers provided by others peers
			/*TrackerData trackerData2 = trackerStorage.getSelection(locationKey, domainKey.xor(Number160.ZERO), trackerStorage.getTrackerSize(), knownPeers);
			for(Map.Entry<Number480, Data> entry:trackerData2.getPeerDataMap().entrySet())
			{
				if(trackerData1.getPeerDataMap().size()>=trackerStorage.getTrackerSize())
					break;
				trackerData1.getPeerDataMap().put(entry.getKey(), entry.getValue());
			}*/
			responseMessage.setDataMapConvert(trackerData1.getPeerDataMap());
			PeerAddress senderAddress = message.getSender();
			if (message.getCommand() == Command.TRACKER_ADD)
			{
				int currentSize = trackerStorage.size(locationKey, domainKey);
				if (currentSize >= trackerStorage
						.getTrackerStoreSize(locationKey, domainKey))
				{
					if (logger.isDebugEnabled())
						logger.debug("tracker NOT put on(" + peerBean.getServerPeerAddress()
								+ ") locationKey:" + locationKey + ", domainKey:" + domainKey
								+ ", address:" + senderAddress+", current size: "+currentSize);
					responseMessage.setType(Message.Type.DENIED);
				}
				else
				{
					if (logger.isDebugEnabled())
						logger.debug("tracker put on(" + peerBean.getServerPeerAddress()
								+ ") locationKey:" + locationKey + ", domainKey:" + domainKey
								+ ", address:" + senderAddress+ "size: "+trackerStorage.size(locationKey, domainKey));
					// here we set the map with the close peers. If we get data
					// by a sender
					// and the sender is closer than us, we assume that the
					// sender has the
					// data and we don't need to transfer data to the closest
					// (sender) peer.
					if (primary && peerBean.getReplicationTracker() != null)
					{
						peerBean.getReplicationTracker().updatePeerMapIfCloser(locationKey,
								message.getSender().getID());
					}
					Map<Number160, Data> dataMap = message.getDataMap();
					final Data attachement = (dataMap != null && dataMap.size() >= 1) ? dataMap
							.values().iterator().next() : new Data(MessageCodec.EMPTY_BYTE_ARRAY,
							null);
					attachement.setPeerAddress(senderAddress);
					// public key is not set in the data, but in the message
					PublicKey publicKey = message.getPublicKey();
					if (!trackerStorage.put(locationKey, domainKey, publicKey, attachement))
						responseMessage.setType(Message.Type.DENIED);
					else
					{
						// check the responsibility of the newly added data, do
						// something
						// (notify) if we are responsible
						if (primary && peerBean.getReplicationTracker() != null)
							peerBean.getReplicationTracker().checkResponsibility(locationKey);
					}
				}
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("tracker get on(" + peerBean.getServerPeerAddress()
							+ ") locationKey:" + locationKey + ", domainKey:" + domainKey
							+ ", address:" + senderAddress +" returning: "+(peerDataMap==null?"0":peerDataMap.size()));
				if (peerDataMap == null)
					responseMessage.setType(Message.Type.NOT_FOUND);
			}
		}
		// this is for replication. If we got something here, the other peer
		// thinks that I'm responsible
		else
		{
			// here we set the map with the close peers. If we get data
			// by a sender
			// and the sender is closer than us, we assume that the
			// sender has the
			// data and we don't need to transfer data to the closest
			// (sender) peer.
			if (peerBean.getReplicationTracker() != null)
				peerBean.getReplicationTracker().updatePeerMapIfCloser(locationKey,
						message.getSender().getID());
			if (logger.isDebugEnabled())
				logger.debug("tracker replication on(" + peerBean.getServerPeerAddress()
						+ ") locationKey:" + locationKey + ", domainKey:" + domainKey
						+ ", address:" + message.getSender());
			Map<Number160, Data> dataMap = message.getDataMap();
			Data data = dataMap.values().iterator().next();
			if (!trackerStorage.put(locationKey, domainKey, null, data))
				responseMessage.setType(Message.Type.DENIED);
			else
			{
				// check the responsibility of the newly added data, do
				// something
				// (notify) if we are responsible
				if (peerBean.getReplicationTracker() != null)
					peerBean.getReplicationTracker().checkResponsibility(locationKey);
			}
		}
		return responseMessage;
	}
	
	private class TrackerRequestTCP extends RequestHandlerTCP
	{
		final private Message message;
		final private Number160 locationKey;
		final private Number160 domainKey;

		public TrackerRequestTCP(PeerBean peerBean, ConnectionBean connectionBean, Message message, Number160 locationKey, Number160 domainKey)
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

		public TrackerRequestUDP(PeerBean peerBean, ConnectionBean connectionBean, Message message, Number160 locationKey, Number160 domainKey)
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
				preHandleMessage((Message) object, peerBean.getTrackerStorage(), this.message.getRecipient(), locationKey, domainKey);
			else
				logger.error("Response received, but not a message: " + object);
			super.messageReceived(ctx, e);
		}
	}
	
	private void preHandleMessage(Message message, TrackerStorage trackerStorage, PeerAddress referrer, Number160 locationKey, Number160 domainKey) throws IOException
	{
		//Since I might become a tracker as well, we keep this information about those trackers.
		Map<Number160, Data> tmp=message.getDataMap();
		if(tmp == null)
		{
			//TODO: find out why we send null messages
			return;
		}
		for(Data data:tmp.values())
		{
			//we don't know the public key, since this is not first hand information.
			//TTL will be set in trackerstorage, so don't worry about it here.
			trackerStorage.putReferred(locationKey, domainKey, data);
			//System.err.println("i want to add "+data.getPeerAddress());
		}
	}
}
