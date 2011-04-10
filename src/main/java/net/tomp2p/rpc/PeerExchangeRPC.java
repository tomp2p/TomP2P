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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageCodec;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class PeerExchangeRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(PeerExchangeRPC.class);
	//since PEX is push based, each peer needs to keep track what was sent to whom.
	final private Map<Number160, Set<Number160>> sentPeers = new HashMap<Number160, Set<Number160>>();
	
	public PeerExchangeRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.PEX);
	}

	public FutureResponse peerExchange(final PeerAddress remoteNode, Number160 locationKey, Number160 domainKey)
	{
		final Message message = createMessage(remoteNode, Command.PEX, Type.REQUEST_1);
		Set<Number160> tmp1=sentPeers.get(remoteNode.getID());
		TrackerData trackerData1 = peerBean.getTrackerStorage().getSelection(locationKey, domainKey, peerBean.getTrackerStorage().getTrackerSize(), tmp1);
		if(tmp1==null)
		{
			tmp1=new HashSet<Number160>();
			sentPeers.put(remoteNode.getID(), tmp1);
		}
		SortedMap<Number480, Data> tmp2 = trackerData1.getPeerDataMap();
		for(Iterator<Number480> it=tmp2.keySet().iterator();it.hasNext();)
		{
			Number480 next=it.next();
			if(tmp1.contains(next.getContentKey()))
				it.remove();
			else
				tmp1.add(next.getContentKey());	
		}
		if(tmp2.size()>0)
		{
			message.setDataMapConvert(tmp2);
			message.setKeyKey(locationKey, domainKey);
			final RequestHandlerUDP requestHandler = new RequestHandlerUDP(peerBean, connectionBean, message);
			return requestHandler.fireAndForgetUDP();
		}
		else
		{
			FutureResponse futureResponse=new FutureResponse(message);
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
		if(logger.isDebugEnabled())
			logger.debug("Received Peer Exchange Message "+message);
		Map<Number160, Data> tmp=message.getDataMap();
		Number160 locationKey=message.getKey1();
		Number160 domainKey=message.getKey2();
		if(tmp!=null && tmp.size()>0 && locationKey!=null && domainKey!=null)
		{
			for(Data data:tmp.values())
			{
				peerBean.getTrackerStorage().putReferred(locationKey, domainKey, data, message.getSender());
				if(logger.isDebugEnabled())
					logger.debug("Adding "+data.getPeerAddress()+" to the map. I'm "+message.getRecipient());
			}
			//we know that this tracker is alive and serving, so add it to the primary list
			Data data=new Data(MessageCodec.EMPTY_BYTE_ARRAY, message.getSender());
			peerBean.getTrackerStorage().put(locationKey, domainKey, null, data);
		}
		return message;
	}
}
