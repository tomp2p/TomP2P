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
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(StorageRPC.class);

	public StorageRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		super(peerBean, connectionBean);
		registerIoHandler(Command.COMPARE_PUT, Command.PUT, Command.GET, Command.ADD, Command.REMOVE);
	}

	public PeerAddress getPeerAddress()
	{
		return getPeerBean().getServerPeerAddress();
	}

	/**
	 * Stores data on a remote peer. Overwrites data if the data already exists. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to store the data
	 * @param locationKey The location of the data
	 * @param domainKey The domain of the data
	 * @param dataMap The map with the content key and data
	 * @param protectDomain Set to true if the domain should be set to
	 *        protected. This means that this domain is flagged an a public key
	 *        is stored for this entry. An update or removal can only be made
	 *        with the matching private key.
	 * @param protectEntry Set to true if the entry should be set to protected.
	 *        This means that this domain is flagged an a public key is stored
	 *        for this entry. An update or removal can only be made with the
	 *        matching private key.
	 * @param signMessage Set to true if the message should be signed. For
	 *        protecting an entry, this needs to be set to true.
	 * @param channelCreator The channel creator
	 * @param forceUDP Set to true if the communication should be UDP, default
	 *        is TCP
	 * @return FutureResponse that stores which content keys have been stored.
	 */
	public FutureResponse put(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final Map<Number160, Data> dataMap, boolean protectDomain, boolean protectEntry,
			boolean signMessage, ChannelCreator channelCreator, boolean forceUDP)
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
		return put(remotePeer, locationKey, domainKey, dataMap, request, protectDomain
				|| signMessage || protectEntry, channelCreator, forceUDP);
	}

	/**
	 * Stores data on a remote peer. Only stores data if the data does not already exist. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to store the data
	 * @param locationKey The location of the data
	 * @param domainKey The domain of the data
	 * @param dataMap The map with the content key and data
	 * @param protectDomain Set to true if the domain should be set to
	 *        protected. This means that this domain is flagged an a public key
	 *        is stored for this entry. An update or removal can only be made
	 *        with the matching private key.
	 * @param protectEntry Set to true if the entry should be set to protected.
	 *        This means that this domain is flagged an a public key is stored
	 *        for this entry. An update or removal can only be made with the
	 *        matching private key.
	 * @param signMessage Set to true if the message should be signed. For
	 *        protecting an entry, this needs to be set to true.
	 * @param channelCreator The channel creator
	 * @param forceUDP Set to true if the communication should be UDP, default
	 *        is TCP
	 * @return FutureResponse that stores which content keys have been stored.
	 */
	public FutureResponse putIfAbsent(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final Map<Number160, Data> dataMap, boolean protectDomain, boolean protectEntry,
			boolean signMessage, ChannelCreator channelCreator, boolean forceUDP)
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
		return put(remotePeer, locationKey, domainKey, dataMap, request, protectDomain
				|| signMessage || protectEntry, channelCreator, forceUDP);
	}
	
	/**
	 * Compares and puts data on a peer. It first compares the hashes that the
	 * user provided on the remote peer, and if the hashes match, the data is
	 * stored. If the flag partial put has been set, then it will store those
	 * data where the hashes match and ignore the others. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to store the data
	 * @param locationKey The location key
	 * @param domainKey The domain key
	 * @param hashDataMap The map with the data and the hashes to compare to
	 * @param protectDomain Protect the domain
	 * @param protectEntry Protect the entry
	 * @param signMessage Set to true if the message should be signed. For
	 *        protecting an entry, this needs to be set to true.
	 * @param partialPut Set to true if partial puts should be allowed. If set
	 *        to false, then the complete map must match the hash, otherwise it
	 *        wont be stored.
	 * @param channelCreator The channel creator
	 * @param forceUDP Set to true if the communication should be UDP, default is TCP
	 * @return FutureResponse that stores which content keys have been stored.
	 */
	public FutureResponse compareAndPut(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final Map<Number160, HashData> hashDataMap,
			boolean protectDomain, boolean protectEntry, boolean signMessage, boolean partialPut,
			ChannelCreator channelCreator, boolean forceUDP)
	{
		nullCheck(remotePeer, locationKey, domainKey, hashDataMap);
		final Message message;
		if (protectDomain)
		{
			message = createMessage(remotePeer, Command.COMPARE_PUT, partialPut ? Type.REQUEST_4
					: Type.REQUEST_2);
		}
		else
		{
			message = createMessage(remotePeer, Command.COMPARE_PUT, partialPut ? Type.REQUEST_3
					: Type.REQUEST_1);
		}
		if (signMessage)
		{
			message.setPublicKeyAndSign(getPeerBean().getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		message.setHashDataMap(hashDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		if(!forceUDP)
		{
			final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse, getPeerBean(),
					getConnectionBean(), message);
			return request.sendTCP(channelCreator);
		}
		else
		{
			final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse, getPeerBean(),
					getConnectionBean(), message);
			return request.sendUDP(channelCreator);
		}
	}

	/**
	 * Stores the data either via put or putIfAbsent. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to store the data
	 * @param locationKey The location key
	 * @param domainKey The domain key
	 * @param dataMap The map with the content key and data
	 * @param type The type of put request, this depends on
	 *        put/putIfAbsent/protected/not-protected
	 * @param signMessage Set to true to sign message
	 * @param channelCreator The channel creator
	 * @param forceUDP Set to true if the communication should be UDP, default
	 *        is TCP
	 * @return FutureResponse that stores which content keys have been stored.
	 */
	private FutureResponse put(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final Map<Number160, Data> dataMap, final Type type,
			boolean signMessage, ChannelCreator channelCreator, boolean forceUDP)
	{
		nullCheck(remotePeer, locationKey, domainKey, dataMap);
		final Message message = createMessage(remotePeer, Command.PUT, type);
		if (signMessage) {
			message.setPublicKeyAndSign(getPeerBean().getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		message.setDataMap(dataMap);
				
		FutureResponse futureResponse = new FutureResponse(message);
		if(!forceUDP)
		{
			final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message);
			return request.sendTCP(channelCreator);
		}
		else
		{
			final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message);
			return request.sendUDP(channelCreator);
		}
	}

	/**
	 * Adds data on a remote peer. The main difference to
	 * {@link #put(PeerAddress, Number160, Number160, Map, Type, boolean, ChannelCreator, boolean)}
	 * and
	 * {@link #putIfAbsent(PeerAddress, Number160, Number160, Map, boolean, boolean, boolean, ChannelCreator, boolean)}
	 * is that it will convert the data collection to map. The key for the map
	 * will be the SHA-1 hash of the data. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to store the data
	 * @param locationKey The location key
	 * @param domainKey The domain key
	 * @param dataSet The set with data. This will be converted to a map. The
	 *        key for the map is the SHA-1 of the data.
	 * @param protectDomain Set to true if the domain should be set to
	 *        protected. This means that this domain is flagged an a public key
	 *        is stored for this entry. An update or removal can only be made
	 *        with the matching private key.
	 * @param signMessage Set to true if the message should be signed. For
	 *        protecting an entry, this needs to be set to true.
	 * @param channelCreator The channel creator
	 * @param forceUDP Set to true if the communication should be UDP, default
	 *        is TCP
	 * @return FutureResponse that stores which content keys have been stored.
	 */
	public FutureResponse add(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final Collection<Data> dataSet, boolean protectDomain,
			boolean signMessage, ChannelCreator channelCreator, boolean forceUDP)
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
		nullCheck(remotePeer, locationKey, domainKey, dataSet);
		Map<Number160, Data> dataMap = new HashMap<Number160, Data>(dataSet.size());
		for (Data data : dataSet)
			dataMap.put(data.getHash(), data);
		final Message message = createMessage(remotePeer, Command.ADD, type);
		if (protectDomain || signMessage) {
			message.setPublicKeyAndSign(getPeerBean().getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		message.setDataMap(dataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		if(!forceUDP)
		{
			final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message);
			return request.sendTCP(channelCreator);
		}
		else
		{
			final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message);
			return request.sendUDP(channelCreator);
		}
	}
	
	/**
	 * Get the data from a remote peer. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to send this request
	 * @param locationKey The location key
	 * @param domainKey The domain key
	 * @param contentKeys The content keys or null if requested all
	 * @param protectedDomains Add the public key to protect the domain. In
	 *        order to make this work, the message needs to be signed
	 * @param signMessage Adds a public key and signs the message
	 * @param digest Returns a list of hashes of the data stored on this peer
	 * @param channelCreator The channel creator that creates connections.
	 *        Typically we need one connection here.
	 * @param forceUDP Set to true if the communication should be UDP, default
	 *        is TCP
	 * @return The future response to keep track of future events
	 */
	public FutureResponse get(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final Collection<Number160> contentKeys,
			PublicKey protectedDomains, boolean signMessage, boolean digest, 
			ChannelCreator channelCreator, boolean forceUDP)
	{
		nullCheck(remotePeer, locationKey, domainKey);
		final Message message = createMessage(remotePeer, Command.GET, digest ? Type.REQUEST_2 : Type.REQUEST_1);
		if (signMessage) {
			message.setPublicKeyAndSign(getPeerBean().getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		if (contentKeys != null)
			message.setKeys(contentKeys);
		if (protectedDomains != null)
			message.setPublicKey(protectedDomains);
		FutureResponse futureResponse = new FutureResponse(message);
		if(!forceUDP)
		{
			final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message);
			return request.sendTCP(channelCreator);
		}
		else
		{
			final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message);
			return request.sendUDP(channelCreator);
		}
	}

	/**
	 * Removes data from a peer. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to send this request
	 * @param locationKey The location key
	 * @param domainKey The domain key
	 * @param contentKeys The content keys or null if requested all
	 * @param sendBackResults Set to true if the removed data should be sent
	 *        back
	 * @param signMessage Adds a public key and signs the message. For protected
	 *        entry and domains, this needs to be provided.
	 * @param channelCreator The channel creator that creates connections
	 * @param forceUDP Set to true if the communication should be UDP, default
	 *        is TCP
	 * @return The future response to keep track of future events
	 */
	public FutureResponse remove(final PeerAddress remotePeer, final Number160 locationKey,
			final Number160 domainKey, final Collection<Number160> contentKeys,
			final boolean sendBackResults, final boolean signMessage, 
			ChannelCreator channelCreator, boolean forceUDP)
	{
		nullCheck(remotePeer, locationKey, domainKey);
		final Message message = createMessage(remotePeer, Command.REMOVE, sendBackResults
				? Type.REQUEST_2 : Type.REQUEST_1);
		if (signMessage) {
			message.setPublicKeyAndSign(getPeerBean().getKeyPair());
		}
		message.setKeyKey(locationKey, domainKey);
		if (contentKeys != null)
			message.setKeys(contentKeys);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse, getPeerBean(), getConnectionBean(), message);
		return request.sendTCP(channelCreator);
	}

	@Override
	public Message handleResponse(final Message message, boolean sign) throws IOException
	{
		if(!(message.getCommand() == Command.ADD || message.getCommand() == Command.PUT 
				|| message.getCommand() == Command.GET || message.getCommand() == Command.REMOVE 
				|| message.getCommand() == Command.COMPARE_PUT))
		{
			throw new IllegalArgumentException("Message content is wrong");
		}
		final Message responseMessage = createResponseMessage(message, Type.OK);
		if(sign) 
		{
    		responseMessage.setPublicKeyAndSign(getPeerBean().getKeyPair());
    	}
		switch (message.getCommand())
		{
			case ADD:
			{
				nullCheck(message.getKeyKey1(), message.getKeyKey2(), message.getDataMap());
				return handleAdd(message, responseMessage, isDomainProtected(message));
			}
			case PUT:
			{
				nullCheck(message.getKeyKey1(), message.getKeyKey2(), message.getDataMap());
				return handlePut(message, responseMessage, isStoreIfAbsent(message),
						isDomainProtected(message));
			}
			case GET:
			{
				nullCheck(message.getKeyKey1(), message.getKeyKey2());
				return handleGet(message, responseMessage);
			}
			
			case REMOVE:
			{
				nullCheck(message.getKeyKey1(), message.getKeyKey2());
				return handleRemove(message, responseMessage, message.getType() == Type.REQUEST_2);
			}
			case COMPARE_PUT:
			{
				nullCheck(message.getKeyKey1(), message.getKeyKey2(), message.getHashDataMap());
				return handleCompareAndPut(message, responseMessage, isPartial(message),
						isDomainProtected(message));
			}
			default:
			{
				throw new IllegalArgumentException("Message content is wrong");
			}
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
		if (getPeerBean().getReplicationStorage() != null)
			getPeerBean().getReplicationStorage().updatePeerMapIfCloser(locationKey,
					message.getSender().getID());
		Collection<Number160> result = new HashSet<Number160>();
		for (Map.Entry<Number160, Data> entry : toStore.entrySet())
		{
			if (getPeerBean().getStorage().put(locationKey, domainKey, entry.getKey(),
					entry.getValue(), publicKey, putIfAbsent, protectDomain)) {
				if(logger.isDebugEnabled()) {
					logger.debug("put data with key "+locationKey+" on "+getPeerBean().getServerPeerAddress());
				}
				result.add(entry.getKey());
			}
			else {
				if(logger.isDebugEnabled()) {
					logger.debug("could not add "+locationKey+" on "+getPeerBean().getServerPeerAddress());
				}
				getPeerBean().getStorage().put(locationKey, domainKey, entry.getKey(),
						entry.getValue(), publicKey, putIfAbsent, protectDomain);
			}
		}
		// check the responsibility of the newly added data, do something
		// (notify) if we are responsible
		if (result.size() > 0 && getPeerBean().getReplicationStorage() != null)
			getPeerBean().getReplicationStorage().checkResponsibility(locationKey);
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
		if (getPeerBean().getReplicationStorage() != null)
			getPeerBean().getReplicationStorage().updatePeerMapIfCloser(locationKey,
					message.getSender().getID());
		Collection<Number160> result = new HashSet<Number160>();
		for (Map.Entry<Number160, Data> entry : data.entrySet())
		{
			if (getPeerBean().getStorage().put(locationKey, domainKey, entry.getKey(),
					entry.getValue(), publicKey, false, protectDomain)) {
				if(logger.isDebugEnabled()) {
					logger.debug("add data with key "+locationKey+" on "+getPeerBean().getServerPeerAddress());
				}
				result.add(entry.getKey());
			}
		}
		// check the responsibility of the newly added data, do something
		// (notify) if we are responsible
		if (result.size() > 0 && getPeerBean().getReplicationStorage() != null)
			getPeerBean().getReplicationStorage().checkResponsibility(locationKey);
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
			final DigestInfo digestInfo = getPeerBean().getStorage().digest(locationKey, domainKey, contentKeys);	
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
					Data data = getPeerBean().getStorage().get(locationKey, domainKey, contentKey);
					if (data != null)
					{
						result.put(key, data);
					}
				}
			}
			else	
			{
				result = getPeerBean().getStorage().get(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE);
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
				Data data = getPeerBean().getStorage().remove(locationKey, domainKey, contentKey, publicKey);
				if (data != null)
					result.put(key, data);
			}
		}
		else
		{
			result = getPeerBean().getStorage().remove(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE, publicKey);
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
		
		final Collection<Number160> result = getPeerBean().getStorage().compareAndPut(
				locationKey, domainKey, hashDataMap, publicKey, partial, protectDomain);
		
		if (logger.isDebugEnabled())
		{
			logger.debug("stored " + result.size());
		}
		if (result.size() > 0 && getPeerBean().getReplicationStorage() != null)
			getPeerBean().getReplicationStorage().checkResponsibility(locationKey);
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
