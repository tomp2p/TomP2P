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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(StorageRPC.class);

	// final private ServerCache serverCache = new ServerCache();
	public StorageRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.COMPARE_PUT, Command.PUT, Command.GET, Command.ADD, Command.REMOVE);
	}

	public PeerAddress getPeerAddress()
	{
		return peerBean.getServerPeerAddress();
	}

	public FutureResponse put(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final Map<Number160, Data> dataMap, boolean protectDomain, boolean protectEntry,
			boolean signMessage, ChannelCreator channelCreator)
	{
		final Type request;
		if (protectDomain)
		{
			request = Type.REQUEST_2;
		}
		else
		{
			request = Type.REQUEST_1;
		}
		return put(remoteNode, locationKey, domainKey, dataMap, request, protectDomain
				|| signMessage || protectEntry, channelCreator);
	}

	public FutureResponse putIfAbsent(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final Map<Number160, Data> dataMap, boolean protectDomain, boolean protectEntry,
			boolean signMessage, ChannelCreator channelCreator)
	{
		final Type request;
		if (protectDomain)
		{
			request = Type.REQUEST_4;
		}
		else
		{
			request = Type.REQUEST_3;
		}
		return put(remoteNode, locationKey, domainKey, dataMap, request, protectDomain
				|| signMessage || protectEntry, channelCreator);
	}
	
	public FutureResponse compareAndPut(final PeerAddress remotePeer, final Number160 locationKey, final Number160 domainKey,
			final Map<Number160, HashData> hashDataMap, boolean protectDomain, boolean protectEntry, boolean signMessage, boolean partialPut,
			ChannelCreator channelCreator)
	{
		nullCheck(remotePeer, locationKey, domainKey, hashDataMap);
		
		final Message message;
		if (protectDomain)
		{
			message = createMessage(remotePeer, Command.COMPARE_PUT, partialPut? Type.REQUEST_4: Type.REQUEST_2);
		}
		else
		{
			message = createMessage(remotePeer, Command.COMPARE_PUT, partialPut? Type.REQUEST_3: Type.REQUEST_1);
		}
			
		if (signMessage) {
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		message.setHashDataMap(hashDataMap);
		
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandlerTCP request = new RequestHandlerTCP(futureResponse, peerBean, connectionBean, message);
		return request.sendTCP(channelCreator);
		
	}

	private FutureResponse put(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final Map<Number160, Data> dataMap, final Type type,
			boolean signMessage, ChannelCreator channelCreator)
	{
		nullCheck(remoteNode, locationKey, domainKey, dataMap);
		final Message message = createMessage(remoteNode, Command.PUT, type);
		if (signMessage) {
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		message.setDataMap(dataMap);
		
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandlerTCP request = new RequestHandlerTCP(futureResponse, peerBean, connectionBean, message);
		return request.sendTCP(channelCreator);
	}

	public FutureResponse add(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final Collection<Data> dataSet, boolean protectDomain,
			boolean signMessage, ChannelCreator channelCreator)
	{
		final Type type;
		if (protectDomain)
		{
			type = Type.REQUEST_2;
		}
		else
		{
			type = Type.REQUEST_1;
		}
		nullCheck(remoteNode, locationKey, domainKey, dataSet);
		Map<Number160, Data> dataMap = new HashMap<Number160, Data>(dataSet.size());
		for (Data data : dataSet)
			dataMap.put(data.getHash(), data);
		final Message message = createMessage(remoteNode, Command.ADD, type);
		if (protectDomain || signMessage) {
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		message.setDataMap(dataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandlerTCP request = new RequestHandlerTCP(futureResponse, peerBean, connectionBean, message);
		return request.sendTCP(channelCreator);
	}

	/**
	 * Starts an RPC to get the data from a remote peer.
	 * 
	 * @param remoteNode The remote peer to send this request
	 * @param locationKey The location key 
	 * @param domainKey The domain key
	 * @param contentKeys The content keys or null if requested all
	 * @param protectedDomains Add the public key to protect the domain. In order to make this work, the message needs to be signed
	 * @param signMessage Adds a public key and signs the message
	 * @param digest Returns a list of hashes of the data stored on this peer
	 * @param channelCreator The channel creator that creates connections. Typically we need one connection here.
	 * @return The future response to keep track of future events 
	 */
	public FutureResponse get(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final Collection<Number160> contentKeys,
			PublicKey protectedDomains, boolean signMessage, boolean digest, ChannelCreator channelCreator)
	{
		nullCheck(remoteNode, locationKey, domainKey);
		final Message message = createMessage(remoteNode, Command.GET, digest ? Type.REQUEST_2 : Type.REQUEST_1);
		if (signMessage) {
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		if (contentKeys != null)
			message.setKeys(contentKeys);
		if (protectedDomains != null)
			message.setPublicKey(protectedDomains);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandlerTCP request = new RequestHandlerTCP(futureResponse, peerBean, connectionBean, message);
		return request.sendTCP(channelCreator);
	}

	public FutureResponse remove(final PeerAddress remoteNode, final Number160 locationKey,
			final Number160 domainKey, final Collection<Number160> contentKeys,
			final boolean sendBackResults, final boolean signMessage, ChannelCreator channelCreator)
	{
		nullCheck(remoteNode, locationKey, domainKey);
		final Message message = createMessage(remoteNode, Command.REMOVE, sendBackResults
				? Type.REQUEST_2 : Type.REQUEST_1);
		if (signMessage) {
			message.setPublicKeyAndSign(peerBean.getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		if (contentKeys != null)
			message.setKeys(contentKeys);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandlerTCP request = new RequestHandlerTCP(futureResponse, peerBean, connectionBean, message);
		return request.sendTCP(channelCreator);
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		if (message.isRequest())
		{
			switch (message.getCommand())
			{
				case ADD:
				case PUT:
					return message.getKeyKey1() != null && message.getKeyKey2() != null
							&& message.getDataMap() != null;
				case GET:
				case REMOVE:
					return message.getKeyKey1() != null && message.getKeyKey2() != null;
				case COMPARE_PUT:
					return message.getHashDataMap() != null; 
			}
		}
		return false;
	}

	@Override
	public Message handleResponse(final Message message, boolean sign) throws IOException
	{
		if (logger.isDebugEnabled())
			logger.debug("handle " + message);
		final Message responseMessage = createMessage(message.getSender(), message.getCommand(),
				Type.OK);
		if(sign) {
    		responseMessage.setPublicKeyAndSign(peerBean.getKeyPair());
    	}
		responseMessage.setMessageId(message.getMessageId());
		switch (message.getCommand())
		{
			case PUT:
				return handlePut(message, responseMessage, isStoreIfAbsent(message),
						isDomainProtected(message));
			case GET:
				return handleGet(message, responseMessage);
			case ADD:
				return handleAdd(message, responseMessage, isDomainProtected(message));
			case REMOVE:
				return handleRemove(message, responseMessage, message.getType() == Type.REQUEST_2);
			case COMPARE_PUT:
				return handleCompareAndPut(message, responseMessage, isPartial(message),
						isDomainProtected(message));
			default:
				return null;
		}
	}

	private boolean isDomainProtected(final Message message)
	{
		boolean protectDomain = message.getPublicKey() != null
				&& (message.getType() == Type.REQUEST_2 || message.getType() == Type.REQUEST_4);
		return protectDomain;
	}

	private boolean isStoreIfAbsent(final Message message)
	{
		boolean absent = message.getType() == Type.REQUEST_3 || message.getType() == Type.REQUEST_4;
		return absent;
	}
	
	private boolean isPartial(final Message message)
	{
		boolean partial = message.getType() == Type.REQUEST_3 || message.getType() == Type.REQUEST_4;
		return partial;
	}

	private Message handlePut(final Message message, final Message responseMessage,
			final boolean putIfAbsent, final boolean protectDomain) throws IOException
	{
		final Number160 locationKey = message.getKeyKey1();
		final Number160 domainKey = message.getKeyKey2();
		final Map<Number160, Data> toStore = message.getDataMap();
		final PublicKey publicKey = message.getPublicKey();
		// here we set the map with the close peers. If we get data by a sender
		// and the sender is closer than us, we assume that the sender has the
		// data and we don't need to transfer data to the closest (sender) peer.
		if (peerBean.getReplicationStorage() != null)
			peerBean.getReplicationStorage().updatePeerMapIfCloser(locationKey,
					message.getSender().getID());
		Collection<Number160> result = new HashSet<Number160>();
		for (Map.Entry<Number160, Data> entry : toStore.entrySet())
		{
			if (peerBean.getStorage().put(new Number480(locationKey, domainKey, entry.getKey()),
					entry.getValue(), publicKey, putIfAbsent, protectDomain)) {
				if(logger.isDebugEnabled()) {
					logger.debug("put data with key "+locationKey+" on "+peerBean.getServerPeerAddress());
				}
				result.add(entry.getKey());
			}
			else {
				if(logger.isDebugEnabled()) {
					logger.debug("could not add "+locationKey+" on "+peerBean.getServerPeerAddress());
				}
				peerBean.getStorage().put(new Number480(locationKey, domainKey, entry.getKey()),
						entry.getValue(), publicKey, putIfAbsent, protectDomain);
			}
		}
		// check the responsibility of the newly added data, do something
		// (notify) if we are responsible
		if (result.size() > 0 && peerBean.getReplicationStorage() != null)
			peerBean.getReplicationStorage().checkResponsibility(locationKey);
		if (result.size() == 0 && !putIfAbsent)
			responseMessage.setType(Type.DENIED);
		else if(result.size() == 0 && putIfAbsent)
		{
			// put if absent does not return an error if it did not work!
			responseMessage.setType(Type.OK);
			responseMessage.setKeys(result);
		}
		else
		{
			responseMessage.setKeys(result);
			if (result.size() != toStore.size())
				responseMessage.setType(Type.PARTIALLY_OK);
		}
		return responseMessage;
	}

	private Message handleAdd(final Message message, final Message responseMessage,
			final boolean protectDomain)
	{
		final Number160 locationKey = message.getKeyKey1();
		final Number160 domainKey = message.getKeyKey2();
		final Map<Number160, Data> data = message.getDataMap();
		final PublicKey publicKey = message.getPublicKey();
		// here we set the map with the close peers. If we get data by a sender
		// and the sender is closer than us, we assume that the sender has the
		// data and we don't need to transfer data to the closest (sender) peer.
		if (peerBean.getReplicationStorage() != null)
			peerBean.getReplicationStorage().updatePeerMapIfCloser(locationKey,
					message.getSender().getID());
		Collection<Number160> result = new HashSet<Number160>();
		for (Map.Entry<Number160, Data> entry : data.entrySet())
		{
			if (peerBean.getStorage().put(new Number480(locationKey, domainKey, entry.getKey()),
					entry.getValue(), publicKey, false, protectDomain)) {
				if(logger.isDebugEnabled()) {
					logger.debug("add data with key "+locationKey+" on "+peerBean.getServerPeerAddress());
				}
				result.add(entry.getKey());
			}
		}
		// check the responsibility of the newly added data, do something
		// (notify) if we are responsible
		if (result.size() > 0 && peerBean.getReplicationStorage() != null)
			peerBean.getReplicationStorage().checkResponsibility(locationKey);
		responseMessage.setKeys(result);
		return responseMessage;
	}

	private Message handleGet(final Message message, final Message responseMessage)
	{
		final Number160 locationKey = message.getKeyKey1();
		final Number160 domainKey = message.getKeyKey2();
		final Collection<Number160> contentKeys = message.getKeys();
		
		final boolean digest = message.getType() == Type.REQUEST_2;
		if(digest)
		{
			final Number320 key = new Number320(locationKey, domainKey);
			final DigestInfo digestInfo;
			if (contentKeys != null)
			{
				digestInfo = peerBean.getStorage().digest(key, contentKeys);	
			}
			else	
			{ 
				digestInfo = peerBean.getStorage().digest(key);
			}
			responseMessage.setKeys(digestInfo.getKeyDigests());
			return responseMessage;
		}
		else
		{
			final Map<Number480, Data> result;
			if (contentKeys != null)
			{
				result = new HashMap<Number480, Data>();
				for (Number160 contentKey : contentKeys)
				{
					Number480 key = new Number480(locationKey, domainKey, contentKey);
					Data data = peerBean.getStorage().get(key);
					if (data != null)
					{
						result.put(key, data);
					}
				}
			}
			else	
			{
				result = peerBean.getStorage().get(new Number320(locationKey, domainKey));
			}
			responseMessage.setDataMapConvert(result);
			return responseMessage;
		}
	}

	private Message handleRemove(final Message message, final Message responseMessage,
			final boolean sendBackResults)
	{
		final Number160 locationKey = message.getKeyKey1();
		final Number160 domainKey = message.getKeyKey2();
		final Collection<Number160> contentKeys = message.getKeys();
		final PublicKey publicKey = message.getPublicKey();
		final Map<Number480, Data> result;
		if (contentKeys != null)
		{
			result = new HashMap<Number480, Data>();
			for (Number160 contentKey : contentKeys)
			{
				Number480 key = new Number480(locationKey, domainKey, contentKey);
				Data data = peerBean.getStorage().remove(key, publicKey);
				if (data != null)
					result.put(key, data);
			}
		}
		else
		{
			result = peerBean.getStorage().remove(new Number320(locationKey, domainKey), publicKey);
		}
		if (!sendBackResults)
		{
			// make a copy, so the iterator in the codec wont conflict with
			// concurrent calls
			responseMessage.setKeysConvert(result.keySet());
		}
		else
		{
			// make a copy, so the iterator in the codec wont conflict with
			// concurrent calls
			responseMessage.setDataMapConvert(result);
		}
		return responseMessage;
	}
	
	private Message handleCompareAndPut(Message message, Message responseMessage, boolean partial,
			boolean protectDomain)
	{
		final Number160 locationKey = message.getKeyKey1();
		final Number160 domainKey = message.getKeyKey2();
		final Map<Number160, HashData> hashDataMap = message.getHashDataMap();
		final PublicKey publicKey = message.getPublicKey();
		
		final Collection<Number160> result = peerBean.getStorage().compareAndPut(
				locationKey, domainKey, hashDataMap, publicKey, partial, protectDomain);
		
		if (result.size() > 0 && peerBean.getReplicationStorage() != null)
			peerBean.getReplicationStorage().checkResponsibility(locationKey);
		responseMessage.setKeys(result);
		if(result.size()==0)
		{
			responseMessage.setType(Type.NOT_FOUND);
		}
		else if(result.size() != hashDataMap.size())
		{
			responseMessage.setType(Type.PARTIALLY_OK);
		}
		return responseMessage;
	}
}
