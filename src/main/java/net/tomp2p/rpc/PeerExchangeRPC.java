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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.TrackerData;
import net.tomp2p.storage.TrackerStorage.ReferrerType;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

public class PeerExchangeRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(PeerExchangeRPC.class);
	// since PEX is push based, each peer needs to keep track what was sent to
	// whom.
	final private Map<Number160, Set<Number160>> sentPeers;
	final private static int DAY = 60 * 60 * 24;

	public PeerExchangeRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.PEX);
		sentPeers = new MapMaker().concurrencyLevel(1).expireAfterAccess(DAY, TimeUnit.SECONDS).makeMap();
	}
	
	@Deprecated
	public FutureResponse peerExchange(final PeerAddress remotePeer, Number160 locationKey, Number160 domainKey,
			boolean isReplication, ChannelCreator channelCreator)
	{
		return peerExchange(remotePeer, locationKey, domainKey, isReplication, channelCreator, false);
	}

	/**
	 * Peer exchange (PEX) information about other peers from the swarm, to not
	 * ask the primary trackers too often. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to send this request
	 * @param locationKey The location key
	 * @param domainKey The domain key
	 * @param isReplication Set to true if the PEX is started as replication.
	 *        This means that this peer learned that an other peer is closer and
	 *        sends tracker information to that peer.
	 * @param channelCreator The channel creator that creates connections
	 * @param forceTCP Set to true if the communication should be TCP, default
	 *        is UDP
	 * @return The future response to keep track of future events
	 */
	public FutureResponse peerExchange(final PeerAddress remotePeer, Number160 locationKey, Number160 domainKey,
			boolean isReplication, ChannelCreator channelCreator, boolean forceTCP)
	{
		final Message message = createMessage(remotePeer, Command.PEX, isReplication ? Type.REQUEST_FF_2 : Type.REQUEST_FF_1);
		Set<Number160> tmp1;
		
		//Can run concurrently
		synchronized(sentPeers)
		{
			tmp1 = sentPeers.get(remotePeer.getID());
			if (tmp1 == null)
			{
				tmp1 = new HashSet<Number160>();
				sentPeers.put(remotePeer.getID(), tmp1);
			}
		}

		Map<Number160, TrackerData> peers;
		if (isReplication)
		{
			peers = peerBean.getTrackerStorage().meshPeers(locationKey, domainKey);
			if (logger.isDebugEnabled())
				logger.debug("we got stored meshPeers size:" + peers.size());
		}
		else
		{
			peers = peerBean.getTrackerStorage().activePeers(locationKey, domainKey);
			if (logger.isDebugEnabled())
				logger.debug("we got stored activePeers size:" + peers.size());
		}

		peers = Utils.subtract(peers, tmp1);
		peers = Utils.limit(peers, TrackerRPC.MAX_MSG_SIZE_UDP);

		// add to our map that we sent the following information to this peer
		tmp1.addAll(peers.keySet());

		message.setKeyKey(locationKey, domainKey);

		// offline peers notification
		// TODO: enable it again...
		// if(removed.size() > 0)
		// message.setKeys(removed);
		// active peers notification
		if (peers.size() > 0)
			message.setTrackerData(peers.values());
		if (peers.size() > 0) // || removed.size() > 0)
		{
			if (logger.isDebugEnabled())
				logger.debug("sent (" + message.getSender().getID() + ") to " + remotePeer.getID() + " / "
						+ peers.size());
			FutureResponse futureResponse = new FutureResponse(message);
			if(!forceTCP)
			{
				final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
				return requestHandler.fireAndForgetUDP(channelCreator);
			}
			else
			{
				final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
				return requestHandler.fireAndForgetTCP(channelCreator);
			}
		}
		else
		{
			// we have nothing to deliver
			FutureResponse futureResponse = new FutureResponse(message);
			futureResponse.setResponse();
			return futureResponse;
		}
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		return (message.getType() == Type.REQUEST_FF_1 || message.getType() == Type.REQUEST_FF_2)
				&& message.getCommand() == Command.PEX;
	}

	@Override
	public Message handleResponse(final Message message, boolean sign) throws Exception
	{
		if (logger.isDebugEnabled())
			logger.debug("Received Peer Exchange Message " + message);
		Collection<TrackerData> tmp = message.getTrackerData();
		Number160 locationKey = message.getKeyKey1();
		Number160 domainKey = message.getKeyKey2();
		Collection<Number160> removedKeys = message.getKeys();
		if (tmp != null && tmp.size() > 0 && locationKey != null && domainKey != null)
		{
			final PeerAddress referrer = message.getSender();
			for (TrackerData data : tmp)
			{
				PeerAddress trackerEntry = data.getPeerAddress();
				peerBean.getTrackerStorage().putReferred(locationKey, domainKey, trackerEntry, referrer,
						data.getAttachement(), data.getOffset(), data.getLength(),
						message.getType() == Type.REQUEST_FF_1 ? ReferrerType.ACTIVE : ReferrerType.MESH);
				if (logger.isDebugEnabled())
					logger.debug("Adding " + data.getPeerAddress() + " to the map. I'm " + message.getRecipient());
			}
			if (removedKeys != null)
			{
				for (Number160 key : removedKeys)
					peerBean.getTrackerStorage().removeReferred(locationKey, domainKey, key, referrer);
			}
		}
		return message;
	}
}
