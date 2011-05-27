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
import java.util.HashMap;
import java.util.HashSet;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerExchangeRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(PeerExchangeRPC.class);
	// since PEX is push based, each peer needs to keep track what was sent to
	// whom.
	final private Map<Number160, Set<Number160>> sentPeers = new HashMap<Number160, Set<Number160>>();

	public PeerExchangeRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.PEX);
	}

	public FutureResponse peerExchange(final PeerAddress remoteNode, Number160 locationKey, Number160 domainKey)
	{
		final Message message = createMessage(remoteNode, Command.PEX, Type.REQUEST_1);
		Set<Number160> tmp1 = sentPeers.get(remoteNode.getID());
		TrackerDataResult trackerData1 = peerBean.getTrackerStorage().getSelection(locationKey, domainKey,
				TrackerRPC.MAX_MSG_SIZE_UDP, tmp1);
		
		if (tmp1 == null)
		{
			tmp1 = new HashSet<Number160>();
			sentPeers.put(remoteNode.getID(), tmp1);
		}
		if(logger.isDebugEnabled())
			logger.debug("we got stored size:"+tmp1.size()+", and we found in our tracker:"+trackerData1.getPeerDataMap().size());
		tmp1.addAll(trackerData1.getPeerDataMap().keySet());
		Map<Number160, TrackerData> tmp2 = trackerData1.getPeerDataMap();
		Set<Number160> removed = peerBean.getTrackerStorage().getAndClearOfflinePrimary();
		message.setKeyKey(locationKey, domainKey);
		if(removed.size() > 0)
			message.setKeys(removed);
		if (tmp2.size() > 0)
			message.setTrackerData(tmp2.values());
		if(tmp2.size() > 0 || removed.size() > 0)
		{
			if(logger.isDebugEnabled())
				logger.debug("sent ("+message.getSender().getID()+") to "+remoteNode.getID()+" / "+tmp2.size());
			final RequestHandlerUDP requestHandler = new RequestHandlerUDP(peerBean, connectionBean, message);
			return requestHandler.fireAndForgetUDP();
		}
		else
		{
			FutureResponse futureResponse = new FutureResponse(message);
			futureResponse.setResponse();
			return futureResponse;
		}
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		return message.getType() == Type.REQUEST_1 && message.getCommand() == Command.PEX;
	}

	@Override
	public Message handleResponse(final Message message) throws Exception
	{
		if (logger.isDebugEnabled())
			logger.debug("Received Peer Exchange Message " + message);
		Collection<TrackerData> tmp = message.getTrackerData();
		Number160 locationKey = message.getKey1();
		Number160 domainKey = message.getKey2();
		Collection<Number160> removedKeys = message.getKeys();
		if (tmp != null && tmp.size() > 0 && locationKey != null && domainKey != null)
		{
			PeerAddress referrer = message.getSender();
			for (TrackerData data : tmp)
			{
				PeerAddress trackerEntry = data.getPeerAddress();
				peerBean.getTrackerStorage().putReferred(locationKey, domainKey, trackerEntry, referrer,
						data.getAttachement(), data.getOffset(), data.getLength());
				if (logger.isDebugEnabled())
					logger.debug("Adding " + data.getPeerAddress() + " to the map. I'm " + message.getRecipient());
			}
			if (removedKeys != null)
			{
				for (Number160 key : removedKeys)
					peerBean.getTrackerStorage().removeReferred(locationKey, domainKey, key, referrer);
			}
			peerBean.getTrackerStorage().peerOnline(referrer);
		}
		return message;
	}
}
