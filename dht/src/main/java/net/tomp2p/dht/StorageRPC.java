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

package net.tomp2p.dht;

import java.io.IOException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.KeyMap640Keys;
import net.tomp2p.message.KeyMapByte;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.BloomfilterFactory;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RPC that deals with storage.
 * 
 * @author Thomas Bocek
 * 
 */
public class StorageRPC extends DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(StorageRPC.class);
    private static final Random RND = new Random();

    private final BloomfilterFactory factory;
    private final StorageLayer storageLayer;
    private ReplicationListener replicationListener = null;

    /**
     * Register the store rpc for put, compare put, get, add, and remove.
     * 
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
     */
    public StorageRPC(final PeerBean peerBean, final ConnectionBean connectionBean, final StorageLayer storageLayer) {
        super(peerBean, connectionBean);
        register(RPC.Commands.PUT.getNr(), 
        		RPC.Commands.GET.getNr(), RPC.Commands.ADD.getNr(), 
        		RPC.Commands.REMOVE.getNr(), RPC.Commands.DIGEST.getNr(), 
        		RPC.Commands.DIGEST_BLOOMFILTER.getNr(), RPC.Commands.PUT_META.getNr(), 
				RPC.Commands.DIGEST_META_VALUES.getNr(), RPC.Commands.PUT_CONFIRM.getNr(),
				RPC.Commands.GET_LATEST.getNr(), RPC.Commands.GET_LATEST_WITH_DIGEST.getNr());
        this.factory = peerBean.bloomfilterFactory();
        this.storageLayer = storageLayer;
    }
    
    public StorageRPC replicationListener(ReplicationListener replicationListener) {
    	this.replicationListener = replicationListener;
    	return this;
    }
    
    public ReplicationListener replicationListener() {
    	return replicationListener;
    }

    /**
     * Stores data on a remote peer. Overwrites data if the data already exists. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to store the data
     * @param locationKey
     *            The location of the data
     * @param domainKey
     *            The domain of the data
     * @param dataMap
     *            The map with the content key and data
     * @param protectDomain
     *            Set to true if the domain should be set to protected. This means that this domain is flagged an a
     *            public key is stored for this entry. An update or removal can only be made with the matching private
     *            key.
     * @param protectEntry
     *            Set to true if the entry should be set to protected. This means that this domain is flagged an a
     *            public key is stored for this entry. An update or removal can only be made with the matching private
     *            key.
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse put(final PeerAddress remotePeer, final PutBuilder putBuilder,
            final ChannelCreator channelCreator) {
        final Type request = putBuilder.isProtectDomain() ? Type.REQUEST_2 : Type.REQUEST_1;
        return put(remotePeer, putBuilder, request, channelCreator);
    }

    /**
     * Stores data on a remote peer. Only stores data if the data does not already exist. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to store the data
     * @param locationKey
     *            The location of the data
     * @param domainKey
     *            The domain of the data
     * @param dataMap
     *            The map with the content key and data
     * @param protectDomain
     *            Set to true if the domain should be set to protected. This means that this domain is flagged an a
     *            public key is stored for this entry. An update or removal can only be made with the matching private
     *            key.
     * @param protectEntry
     *            Set to true if the entry should be set to protected. This means that this domain is flagged an a
     *            public key is stored for this entry. An update or removal can only be made with the matching private
     *            key.
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse putIfAbsent(final PeerAddress remotePeer, final PutBuilder putBuilder,
            final ChannelCreator channelCreator) {
        final Type request;
        if (putBuilder.isProtectDomain()) {
            request = Type.REQUEST_4;
        } else {
            request = Type.REQUEST_3;
        }
        return put(remotePeer, putBuilder, request, channelCreator);
    }

    /**
     * Stores the data either via put or putIfAbsent. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to store the data
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataMap
     *            The map with the content key and data
     * @param type
     *            The type of put request, this depends on put/putIfAbsent/protected/not-protected
     * @param signMessage
     *            Set to true to sign message
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    /*
     * private FutureResponse put(final PeerAddress remotePeer, final Number160 locationKey, final Number160 domainKey,
     * final Map<Number160, Data> dataMap, final Type type, boolean signMessage, ChannelCreator channelCreator, boolean
     * forceUDP, SenderCacheStrategy senderCacheStrategy) {
     */
    private FutureResponse put(final PeerAddress remotePeer, final PutBuilder putBuilder, final Type type,
            final ChannelCreator channelCreator) {

        Utils.nullCheck(remotePeer);

        final DataMap dataMap;
        if (putBuilder.dataMap() != null) {
            dataMap = new DataMap(putBuilder.dataMap());
        } else {
            dataMap = new DataMap(putBuilder.locationKey(), putBuilder.domainKey(),
                    putBuilder.versionKey(), putBuilder.dataMapContent());
        }

        final Message message = createMessage(remotePeer, RPC.Commands.PUT.getNr(), type);

        if (putBuilder.isSign()) {
            message.publicKeyAndSign(putBuilder.keyPair());
        }

        message.setDataMap(dataMap);

        final FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
                peerBean(), connectionBean(), putBuilder);

        if (!putBuilder.isForceUDP()) {
            return request.sendTCP(channelCreator);
        } else {
            return request.sendUDP(channelCreator);
        }

    }
    
    public FutureResponse putMeta(final PeerAddress remotePeer, final PutBuilder putBuilder, 
            final ChannelCreator channelCreator) {

        Utils.nullCheck(remotePeer);

        final DataMap dataMap;
        if (putBuilder.dataMap() != null) {
            dataMap = new DataMap(putBuilder.dataMap());
        } else {
            dataMap = new DataMap(putBuilder.locationKey(), putBuilder.domainKey(),
                    putBuilder.versionKey(), putBuilder.dataMapContent());
        }
        
        final Type type;
        if (putBuilder.changePublicKey()!=null) {
        	//change domain protection key
        	type = Type.REQUEST_2;
        } else {
        	//change entry protection key, or set timestamp
        	type = Type.REQUEST_1;
        }

        final Message message = createMessage(remotePeer, RPC.Commands.PUT_META.getNr(), type);

        if (putBuilder.isSign()) {
            message.publicKeyAndSign(putBuilder.keyPair());
        } else if (type == Type.REQUEST_2) {
        	throw new IllegalAccessError("can only change public key if message is signed");
        }
        
        if (putBuilder.changePublicKey()!=null) {
        	message.key(putBuilder.locationKey());
        	message.key(putBuilder.domainKey());
        	message.publicKey(putBuilder.changePublicKey());
        } else {
        	message.setDataMap(dataMap);
        }

        final FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
                peerBean(), connectionBean(), putBuilder);

        if (!putBuilder.isForceUDP()) {
            return request.sendTCP(channelCreator);
        } else {
            return request.sendUDP(channelCreator);
        }

    }

	public FutureResponse putConfirm(final PeerAddress remotePeer, final PutBuilder putBuilder,
			final ChannelCreator channelCreator) {

		Utils.nullCheck(remotePeer);

		final DataMap dataMap;
		if (putBuilder.dataMap() != null) {
			dataMap = new DataMap(putBuilder.dataMap());
		} else {
			dataMap = new DataMap(putBuilder.locationKey(), putBuilder.domainKey(),
					putBuilder.versionKey(), putBuilder.dataMapContent());
		}

		final Message message = createMessage(remotePeer, RPC.Commands.PUT_CONFIRM.getNr(), Type.REQUEST_1);

		if (putBuilder.isSign()) {
			message.publicKeyAndSign(putBuilder.keyPair());
		}

		message.setDataMap(dataMap);

		final FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
				peerBean(), connectionBean(), putBuilder);

		if (!putBuilder.isForceUDP()) {
			return request.sendTCP(channelCreator);
		} else {
			return request.sendUDP(channelCreator);
		}
	}

    /**
     * Adds data on a remote peer. The main difference to
     * {@link #put(PeerAddress, Number160, Number160, Map, Type, boolean, ChannelCreator, boolean)} and
     * {@link #putIfAbsent(PeerAddress, Number160, Number160, Map, boolean, boolean, boolean, ChannelCreator, boolean)}
     * is that it will convert the data collection to map. The key for the map will be the SHA-1 hash of the data. This
     * is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to store the data
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataSet
     *            The set with data. This will be converted to a map. The key for the map is the SHA-1 of the data.
     * @param protectDomain
     *            Set to true if the domain should be set to protected. This means that this domain is flagged an a
     *            public key is stored for this entry. An update or removal can only be made with the matching private
     *            key.
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse add(final PeerAddress remotePeer, final AddBuilder addBuilder,
            ChannelCreator channelCreator) {
        Utils.nullCheck(remotePeer, addBuilder.locationKey(), addBuilder.domainKey());
        final Type type;
        if (addBuilder.isProtectDomain()) {
            if (addBuilder.isList()) {
                type = Type.REQUEST_4;
            } else {
                type = Type.REQUEST_2;
            }
        } else {
            if (addBuilder.isList()) {
                type = Type.REQUEST_3;
            } else {
                type = Type.REQUEST_1;
            }
        }

        // convert the data
        Map<Number160, Data> dataMap = new HashMap<Number160, Data>(addBuilder.dataSet().size());
        if (addBuilder.dataSet() != null) {
            for (Data data : addBuilder.dataSet()) {
                if (addBuilder.isList()) {
                    Number160 hash = new Number160(addBuilder.random());
                    while (dataMap.containsKey(hash)) {
                        hash = new Number160(addBuilder.random());
                    }
                    dataMap.put(hash, data);
                } else {
                    dataMap.put(data.hash(), data);
                }
            }
        }

        final Message message = createMessage(remotePeer, RPC.Commands.ADD.getNr(), type);

        if (addBuilder.isSign()) {
            message.publicKeyAndSign(addBuilder.keyPair());
        }

        message.setDataMap(new DataMap(addBuilder.locationKey(), addBuilder.domainKey(), addBuilder
                .versionKey(), dataMap));

        final FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
                peerBean(), connectionBean(), addBuilder);
        if (!addBuilder.isForceUDP()) {
            return request.sendTCP(channelCreator);
        } else {
            return request.sendUDP(channelCreator);
        }

    }

    public FutureResponse digest(final PeerAddress remotePeer, final DigestBuilder getBuilder,
            final ChannelCreator channelCreator) {
    	
    	final Byte command;
        if(getBuilder.isReturnBloomFilter()) {
        	command = RPC.Commands.DIGEST_BLOOMFILTER.getNr();
        } else if(getBuilder.isReturnMetaValues()) {
        	command = RPC.Commands.DIGEST_META_VALUES.getNr();
        } else {
        	command = RPC.Commands.DIGEST.getNr();
        }
        
        final Type type;
        if (getBuilder.isAscending() && getBuilder.isBloomFilterAnd()) {
            type = Type.REQUEST_1;
        } else if(!getBuilder.isAscending() && getBuilder.isBloomFilterAnd()){
            type = Type.REQUEST_2;
        } else if(getBuilder.isAscending() && !getBuilder.isBloomFilterAnd()){
        	type = Type.REQUEST_3;
        } else {
        	type = Type.REQUEST_4;
        }
        
        final Message message = createMessage(remotePeer, command, type);

        if (getBuilder.isSign()) {
            message.publicKeyAndSign(getBuilder.keyPair());
        }

        if (getBuilder.to() != null && getBuilder.from() != null) {
            final Collection<Number640> keys = new ArrayList<Number640>(2);
            keys.add(getBuilder.from());
            keys.add(getBuilder.to());
            message.intValue(getBuilder.returnNr());
            message.keyCollection(new KeyCollection(keys));
        } else if (getBuilder.keys() == null) {

            if (getBuilder.locationKey() == null || getBuilder.domainKey() == null) {
                throw new IllegalArgumentException("Null not allowed in location or domain");
            }
            message.key(getBuilder.locationKey());
            message.key(getBuilder.domainKey());

            if (getBuilder.contentKeys() != null) {
                message.keyCollection(new KeyCollection(getBuilder.locationKey(), getBuilder
                        .domainKey(), getBuilder.versionKey(), getBuilder.contentKeys()));
            } else {
                message.intValue(getBuilder.returnNr());
                if (getBuilder.keyBloomFilter() != null || getBuilder.contentBloomFilter() != null) {
                    if (getBuilder.keyBloomFilter() != null) {
                        message.bloomFilter(getBuilder.keyBloomFilter());
                    }
                    if (getBuilder.contentBloomFilter() != null) {
                        message.bloomFilter(getBuilder.contentBloomFilter());
                    }
                }
            }
        } else {
            message.keyCollection(new KeyCollection(getBuilder.keys()));
        }

        final FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
                peerBean(), connectionBean(), getBuilder);
        if (!getBuilder.isForceUDP()) {
            return request.sendTCP(channelCreator);
        } else {
            return request.sendUDP(channelCreator);
        }
    }

    public FutureResponse get(final PeerAddress remotePeer, final GetBuilder getBuilder,
            final ChannelCreator channelCreator) {
    	final Type type;
        if (getBuilder.isAscending() && getBuilder.isBloomFilterAnd()) {
            type = Type.REQUEST_1;
        } else if(!getBuilder.isAscending() && getBuilder.isBloomFilterAnd()){
            type = Type.REQUEST_2;
        } else if(getBuilder.isAscending() && !getBuilder.isBloomFilterAnd()){
        	type = Type.REQUEST_3;
        } else {
        	type = Type.REQUEST_4;
        }
        final Message message = createMessage(remotePeer, RPC.Commands.GET.getNr(), type);

        if (getBuilder.isSign()) {
            message.publicKeyAndSign(getBuilder.keyPair());
        }

        if (getBuilder.to() != null && getBuilder.from() != null) {
            final Collection<Number640> keys = new ArrayList<Number640>(2);
            keys.add(getBuilder.from());
            keys.add(getBuilder.to());
            message.intValue(getBuilder.returnNr());
            message.keyCollection(new KeyCollection(keys));
        } else if (getBuilder.keys() == null) {

            if (getBuilder.locationKey() == null || getBuilder.domainKey() == null) {
                throw new IllegalArgumentException("Null not allowed in location or domain");
            }
            message.key(getBuilder.locationKey());
            message.key(getBuilder.domainKey());

            if (getBuilder.contentKeys() != null) {
                message.keyCollection(new KeyCollection(getBuilder.locationKey(), getBuilder
                        .domainKey(), getBuilder.versionKey(), getBuilder.contentKeys()));
            } else {
                message.intValue(getBuilder.returnNr());
                if (getBuilder.keyBloomFilter() != null || getBuilder.contentBloomFilter() != null) {
                    if (getBuilder.keyBloomFilter() != null) {
                        message.bloomFilter(getBuilder.keyBloomFilter());
                    }
                    if (getBuilder.contentBloomFilter() != null) {
                        message.bloomFilter(getBuilder.contentBloomFilter());
                    }
                }
            }
        } else {
            message.keyCollection(new KeyCollection(getBuilder.keys()));
        }

        final FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
                peerBean(), connectionBean(), getBuilder);
        if (!getBuilder.isForceUDP()) {
            return request.sendTCP(channelCreator);
        } else {
            return request.sendUDP(channelCreator);
        }
    }

	public FutureResponse getLatest(final PeerAddress remotePeer, final GetBuilder getBuilder,
			final ChannelCreator channelCreator, final RPC.Commands command) {
		final Type type = Type.REQUEST_1;
		final Message message = createMessage(remotePeer, command.getNr(), type);

		if (getBuilder.isSign()) {
			message.publicKeyAndSign(getBuilder.keyPair());
		}

		message.key(getBuilder.locationKey());
		message.key(getBuilder.domainKey());
		message.key(getBuilder.contentKey());

		final FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
				peerBean(), connectionBean(), getBuilder);
		if (!getBuilder.isForceUDP()) {
			return request.sendTCP(channelCreator);
		} else {
			return request.sendUDP(channelCreator);
		}
	}

    /**
     * Removes data from a peer. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param contentKeys
     *            The content keys or null if requested all
     * @param sendBackResults
     *            Set to true if the removed data should be sent back
     * @param signMessage
     *            Adds a public key and signs the message. For protected entry and domains, this needs to be provided.
     * @param channelCreator
     *            The channel creator that creates connections
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return The future response to keep track of future events
     */
    public FutureResponse remove(final PeerAddress remotePeer, final RemoveBuilder removeBuilder,
            final ChannelCreator channelCreator) {
        final Message message = createMessage(remotePeer, RPC.Commands.REMOVE.getNr(),
                removeBuilder.isReturnResults() ? Type.REQUEST_2 : Type.REQUEST_1);

        if (removeBuilder.isSign()) {
            message.publicKeyAndSign(removeBuilder.keyPair());
        }
        
        if (removeBuilder.to() != null && removeBuilder.from() != null) {
            final Collection<Number640> keys = new ArrayList<Number640>(2);
            keys.add(removeBuilder.from());
            keys.add(removeBuilder.to());
            //marker
            message.intValue(0);
            message.keyCollection(new KeyCollection(keys));
        } else if (removeBuilder.keys() == null) {

            if (removeBuilder.locationKey() == null || removeBuilder.domainKey() == null) {
                throw new IllegalArgumentException("Null not allowed in location or domain");
            }
            message.key(removeBuilder.locationKey());
            message.key(removeBuilder.domainKey());

            if (removeBuilder.contentKeys() != null) {
                message.keyCollection(new KeyCollection(removeBuilder.locationKey(), removeBuilder
                        .domainKey(), removeBuilder.versionKey(), removeBuilder.contentKeys()));
            }
        } else {
            message.keyCollection(new KeyCollection(removeBuilder.keys()));
        }

        final FutureResponse futureResponse = new FutureResponse(message);

        final RequestHandler<FutureResponse> request = new RequestHandler<FutureResponse>(futureResponse,
                peerBean(), connectionBean(), removeBuilder);
        if (!removeBuilder.isForceUDP()) {
            return request.sendTCP(channelCreator);
        } else {
            return request.sendUDP(channelCreator);
        }
    }

    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign,
            Responder responder) throws Exception {

    	if (!(message.command() == RPC.Commands.ADD.getNr() || message.command() == RPC.Commands.PUT.getNr()
				|| message.command() == RPC.Commands.GET.getNr()
				|| message.command() == RPC.Commands.REMOVE.getNr()
				|| message.command() == RPC.Commands.DIGEST.getNr()
				|| message.command() == RPC.Commands.DIGEST_BLOOMFILTER.getNr()
				|| message.command() == RPC.Commands.DIGEST_META_VALUES.getNr()
				|| message.command() == RPC.Commands.PUT_META.getNr()
				|| message.command() == RPC.Commands.PUT_CONFIRM.getNr()
				|| message.command() == RPC.Commands.GET_LATEST.getNr()
				|| message.command() == RPC.Commands.GET_LATEST_WITH_DIGEST.getNr())) {
			throw new IllegalArgumentException("Message content is wrong " + message.command());
		}
        final Message responseMessage = createResponseMessage(message, Type.OK);

        //switch/case does not work here out of the box, need to convert byte back to enum, not sure if thats worth it.
        if (message.command() == RPC.Commands.ADD.getNr()) {
        	handleAdd(message, responseMessage, isDomainProtected(message));
        } else if(message.command() == RPC.Commands.PUT.getNr()) {
            handlePut(message, responseMessage, isStoreIfAbsent(message), isDomainProtected(message));
        } else if (message.command() == RPC.Commands.PUT_CONFIRM.getNr()) {
        	handlePutConfirm(message, responseMessage);
        } else if (message.command() == RPC.Commands.GET.getNr()) {
            handleGet(message, responseMessage);
		} else if (message.command() == RPC.Commands.GET_LATEST.getNr()) {
			handleGetLatest(message, responseMessage, false);
		} else if (message.command() == RPC.Commands.GET_LATEST_WITH_DIGEST.getNr()) {
			handleGetLatest(message, responseMessage, true);
        } else if (message.command() == RPC.Commands.DIGEST.getNr() 
        		|| message.command() == RPC.Commands.DIGEST_BLOOMFILTER.getNr()
        		|| message.command() == RPC.Commands.DIGEST_META_VALUES.getNr()) {
            handleDigest(message, responseMessage);
        } else if (message.command() == RPC.Commands.REMOVE.getNr()) {
            handleRemove(message, responseMessage, message.type() == Type.REQUEST_2);
        } else if (message.command() == RPC.Commands.PUT_META.getNr()) {
            handlePutMeta(message, responseMessage, message.type() == Type.REQUEST_2);
        }else {
            throw new IllegalArgumentException("Message content is wrong");
        }
        if (sign) {
            responseMessage.publicKeyAndSign(peerBean().getKeyPair());
        }
        LOG.debug("response for storage request: {}", responseMessage);
        responder.response(responseMessage);
    }

    

	private boolean isDomainProtected(final Message message) {
        boolean protectDomain = message.publicKey(0) != null
                && (message.type() == Type.REQUEST_2 || message.type() == Type.REQUEST_4);
        return protectDomain;
    }

    private boolean isStoreIfAbsent(final Message message) {
        boolean absent = message.type() == Type.REQUEST_3 || message.type() == Type.REQUEST_4;
        return absent;
    }

    private boolean isList(final Message message) {
        boolean partial = message.type() == Type.REQUEST_3 || message.type() == Type.REQUEST_4;
        return partial;
    }

    private boolean isAscending(final Message message) {
        boolean partial = message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_3;
        return partial;
    }

    private boolean isBloomFilterAnd(final Message message) {
        boolean partial = message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2;
        return partial;
    }
    
    private void handlePutMeta(Message message, Message responseMessage, boolean isDomain) {
    	LOG.debug("handlePutMeta {}", message);
    	final PublicKey publicKey = message.publicKey(0);
        final DataMap toStore = message.dataMap(0);
        final Map<Number640, Byte> result;
        final int dataSize;
        if(isDomain) {
        	dataSize = 1;
        	result = new HashMap<Number640, Byte>(1);
        	LOG.debug("received meta request to change domain");
        	Number160 locationKey = message.key(0);
        	Number160 domainKey = message.key(1);
        	final Number640 key = new Number640(locationKey, domainKey, Number160.ZERO, Number160.ZERO);
        	PublicKey publicKeyChange = message.publicKey(1);
        	Enum<?> status = storageLayer.updateMeta(key.locationAndDomainKey(), publicKey, publicKeyChange);
        	result.put(key, (byte) status.ordinal());
        } else {
        	dataSize = toStore.size();
            result = new HashMap<Number640, Byte>(dataSize);
        	LOG.debug("received meta request to change entry");
        	for (Map.Entry<Number640, Data> entry : toStore.dataMap().entrySet()) {
        		entry.getValue().meta();
        		Enum<?> status = storageLayer.updateMeta(publicKey, entry.getKey(), entry.getValue());
        		result.put(entry.getKey(), (byte) status.ordinal());
        	}
        }
        responseMessage.type(result.size() == dataSize ? Type.OK : Type.PARTIALLY_OK);
        responseMessage.keyMapByte(new KeyMapByte(result));
    }

    private Message handlePut(final Message message, final Message responseMessage,
            final boolean putIfAbsent, final boolean protectDomain) throws IOException {
    	LOG.debug("handlePut {}", message);
        final PublicKey publicKey = message.publicKey(0);
        final DataMap toStore = message.dataMap(0);
        final int dataSize = toStore.size();
        final Map<Number640, Byte> result = new HashMap<Number640, Byte>(dataSize);
        for (Map.Entry<Number640, Data> entry : toStore.dataMap().entrySet()) {
            Enum<?> putStatus = doPut(putIfAbsent, protectDomain, publicKey, entry.getKey(), entry.getValue());
            result.put(entry.getKey(), (byte) putStatus.ordinal());
            // check the responsibility of the newly added data, do something
            // (notify) if we are responsible
            if (!entry.getValue().hasPrepareFlag()) {
            	if ((putStatus == PutStatus.OK || putStatus == PutStatus.VERSION_FORK || putStatus == PutStatus.DELETED)
            			&& replicationListener != null) {
            		replicationListener.dataInserted(
            				entry.getKey().locationKey());
            	}
            }
           
        }

        responseMessage.type(result.size() == dataSize ? Type.OK : Type.PARTIALLY_OK);
        responseMessage.keyMapByte(new KeyMapByte(result));
        return responseMessage;
    }

	private void handlePutConfirm(final Message message, final Message responseMessage) throws IOException {
		LOG.debug("handlePutConfirm {}", message);
		final PublicKey publicKey = message.publicKey(0);
		final DataMap toStore = message.dataMap(0);
		final int dataSize = toStore.size();
		final Map<Number640, Byte> result = new HashMap<Number640, Byte>(dataSize);
		LOG.debug("Received put confirmation.");
		for (Map.Entry<Number640, Data> entry : toStore.dataMap().entrySet()) {
			Enum<?> status = storageLayer.putConfirm(publicKey, entry.getKey(), entry.getValue());
			result.put(entry.getKey(), (byte) status.ordinal());
			
			if ((status == PutStatus.OK || status == PutStatus.VERSION_FORK)
        			&& replicationListener != null) {
        		replicationListener.dataInserted(
        				entry.getKey().locationKey());
        	}
		}
		
		responseMessage.type(result.size() == dataSize ? Type.OK : Type.PARTIALLY_OK);
		responseMessage.keyMapByte(new KeyMapByte(result));
	}

    private Message handleAdd(final Message message, final Message responseMessage,
            final boolean protectDomain) {
    	LOG.debug("handleAdd {}", message);
        Utils.nullCheck(message.dataMap(0));
        final Map<Number640, Byte> result = new HashMap<Number640, Byte>();
        final DataMap dataMap = message.dataMap(0);
        final PublicKey publicKey = message.publicKey(0);
        final boolean list = isList(message);
        // here we set the map with the close peers. If we get data by a
        // sender and the sender is closer than us, we assume that the sender has
        // the data and we don't need to transfer data to the closest (sender)
        // peer.

        for (Map.Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {
            Enum<?> status = doAdd(protectDomain, entry, publicKey, list, storageLayer, peerBean().serverPeerAddress());
            result.put(entry.getKey(), (byte) status.ordinal());

            // check the responsibility of the newly added data, do something
            // (notify) if we are responsible
            if (!entry.getValue().hasPrepareFlag()) {
            	if (status == PutStatus.OK && replicationListener!=null) {
            		replicationListener.dataInserted(
            				entry.getKey().locationKey());
            	}
            }

        }
        responseMessage.keyMapByte(new KeyMapByte(result));
        return responseMessage;
    }

    private Enum<?> doPut(final boolean putIfAbsent, final boolean protectDomain, final PublicKey publicKey,
            final Number640 key, final Data value) {
        LOG.debug("put data with key {} on {} with data {}", key, peerBean().serverPeerAddress(), value);
        return storageLayer.put(key, value, publicKey, putIfAbsent, protectDomain);
    }

    private static Enum<?> doAdd(final boolean protectDomain, final Map.Entry<Number640, Data> entry,
            final PublicKey publicKey, final boolean list, final StorageLayer storageLayer, final PeerAddress serverPeerAddress) {

        LOG.debug("add list data with key {} on {}", entry.getKey(), serverPeerAddress);
        if (list) {
            Number160 contentKey2 = new Number160(RND);
            Enum<?> status;
            Number640 key = new Number640(entry.getKey().locationKey(), entry.getKey().domainKey(),
                    contentKey2, entry.getKey().versionKey());
            while ((status = storageLayer.put(key, entry.getValue(), publicKey, true, protectDomain)) == PutStatus.FAILED_NOT_ABSENT) {
                contentKey2 = new Number160(RND);
            }
            return status;
        } else {
            return storageLayer.put(entry.getKey(), entry.getValue(), publicKey, false, protectDomain);
        }
    }

    private Message handleGet(final Message message, final Message responseMessage) {
    	LOG.debug("handleGet {}", message);
        final Number160 locationKey = message.key(0);
        final Number160 domainKey = message.key(1);
        final KeyCollection contentKeys = message.keyCollection(0);
        final SimpleBloomFilter<Number160> contentBloomFilter = message.bloomFilter(0);
        final SimpleBloomFilter<Number160> versionBloomFilter = message.bloomFilter(1);
        final Integer returnNr = message.intAt(0);
        final int limit = returnNr == null ? -1 : returnNr;
        final boolean ascending = isAscending(message);
        final boolean isRange = contentKeys != null && returnNr != null;
        final boolean isCollection = contentKeys != null && returnNr == null;
        final boolean isBloomFilterAnd = isBloomFilterAnd(message);
        final Map<Number640, Data> result = doGet(locationKey, domainKey, contentKeys, contentBloomFilter,
                versionBloomFilter, limit, ascending, isRange, isCollection, isBloomFilterAnd);
        responseMessage.setDataMap(new DataMap(result));
        return responseMessage;
    }

	private Map<Number640, Data> doGet(final Number160 locationKey, final Number160 domainKey,
            final KeyCollection contentKeys, final SimpleBloomFilter<Number160> contentBloomFilter,
            final SimpleBloomFilter<Number160> versionBloomFilter, final int limit, final boolean ascending,
            final boolean isRange, final boolean isCollection, final boolean isBloomFilterAnd) {
	    final Map<Number640, Data> result;
        if (isCollection) {
            result = new HashMap<Number640, Data>();
            for (Number640 key : contentKeys.keys()) {
                Data data = storageLayer.get(key);
                if (data != null) {
                    result.put(key, data);
                }
            }
        } else if (isRange) {
            // get min/max
            Iterator<Number640> iterator = contentKeys.keys().iterator();
            Number640 min = iterator.next();
            Number640 max = iterator.next();
            result = storageLayer.get(min, max, limit, ascending);

        } else if (contentBloomFilter != null || versionBloomFilter != null) {
            Number640 min = new Number640(locationKey, domainKey, Number160.ZERO, Number160.ZERO);
            Number640 max = new Number640(locationKey, domainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
            result = storageLayer.get(min, max, contentBloomFilter, versionBloomFilter, limit, ascending, isBloomFilterAnd);
        } else {
            // get all
            Number640 min = new Number640(locationKey, domainKey, Number160.ZERO, Number160.ZERO);
            Number640 max = new Number640(locationKey, domainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
            result = storageLayer.get(min, max, limit, ascending);
        }
	    return result;
    }

	private Message handleGetLatest(final Message message, final Message responseMessage,
			final boolean withDigest) {
		LOG.debug("handleGetLatest {}", message);
		final Number160 locationKey = message.key(0);
		final Number160 domainKey = message.key(1);
		final Number160 contentKey = message.key(2);

		Number640 key = new Number640(locationKey, domainKey, contentKey, Number160.ZERO);
		final Map<Number640, Data> result = storageLayer.getLatestVersion(key);
		responseMessage.setDataMap(new DataMap(result));

		if (withDigest) {
			final DigestInfo digestInfo = storageLayer.digest(key.minVersionKey(), key.maxVersionKey(), -1, true);
			responseMessage.keyMap640Keys(new KeyMap640Keys(digestInfo.digests()));
		}

		return responseMessage;
	}

    private Message handleDigest(final Message message, final Message responseMessage) {
    	LOG.debug("handleDigest {}", message);
        final Number160 locationKey = message.key(0);
        final Number160 domainKey = message.key(1);
        final KeyCollection contentKeys = message.keyCollection(0);
        final SimpleBloomFilter<Number160> contentBloomFilter = message.bloomFilter(0);
        final SimpleBloomFilter<Number160> versionBloomFilter = message.bloomFilter(1);
        final Integer returnNr = message.intAt(0);
        final int limit = returnNr == null ? -1 : returnNr;
        final boolean ascending = isAscending(message);
        final boolean isRange = contentKeys != null && returnNr != null;
        final boolean isCollection = contentKeys != null && returnNr == null;
        final boolean isBloomFilterAnd = isBloomFilterAnd(message);
        final boolean isReturnBloomfilter = message.command() == RPC.Commands.DIGEST_BLOOMFILTER.getNr();
        final boolean isReturnMetaValues = message.command() == RPC.Commands.DIGEST_META_VALUES.getNr();
        if(isReturnMetaValues) {
        	final Map<Number640, Data> result = doGet(locationKey, domainKey, contentKeys, contentBloomFilter,
                    versionBloomFilter, limit, ascending, isRange, isCollection, isBloomFilterAnd);
        	DataMap dataMap = new DataMap(result, true);
        	responseMessage.setDataMap(dataMap);
        } else {
        	final DigestInfo digestInfo = doDigest(locationKey, domainKey, contentKeys, contentBloomFilter,
        			versionBloomFilter, limit, ascending, isRange, isCollection, isBloomFilterAnd);
        	if (isReturnBloomfilter) {
                responseMessage.bloomFilter(digestInfo.contentKeyBloomFilter(factory));
                responseMessage.bloomFilter(digestInfo.versionKeyBloomFilter(factory));
            } else {
                responseMessage.keyMap640Keys(new KeyMap640Keys(digestInfo.digests()));
            }
        }
        return responseMessage;

    }

	private DigestInfo doDigest(
            final Number160 locationKey, final Number160 domainKey, final KeyCollection contentKeys,
            final SimpleBloomFilter<Number160> contentBloomFilter,
            final SimpleBloomFilter<Number160> versionBloomFilter, final int limit, final boolean ascending,
            final boolean isRange, final boolean isCollection, final boolean isBloomFilterAnd) {
	    final DigestInfo digestInfo;
        if (isCollection) {
            digestInfo = storageLayer.digest(contentKeys.keys());
        } else if (isRange) {
            // get min/max
            Iterator<Number640> iterator = contentKeys.keys().iterator();
            Number640 min = iterator.next();
            Number640 max = iterator.next();
            digestInfo = storageLayer.digest(min, max, limit, ascending);
        } else if (contentBloomFilter != null || versionBloomFilter != null) {
            final Number320 locationAndDomainKey = new Number320(locationKey, domainKey);
            digestInfo = storageLayer.digest(locationAndDomainKey, contentBloomFilter,
            		versionBloomFilter, limit, ascending, isBloomFilterAnd);
        } else {
            // get all
            Number640 min = new Number640(locationKey, domainKey, Number160.ZERO, Number160.ZERO);
            Number640 max = new Number640(locationKey, domainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
            digestInfo = storageLayer.digest(min, max, limit, ascending);
        }
        return digestInfo;

        
    }

    private Message handleRemove(final Message message, final Message responseMessage,
            final boolean sendBackResults) {
    	LOG.debug("handleRemove {}", message);
        final Number160 locationKey = message.key(0);
        final Number160 domainKey = message.key(1);
        final KeyCollection keys = message.keyCollection(0);
        final PublicKey publicKey = message.publicKey(0);
        Map<Number640, Data> result1 = null;
        Map<Number640, Byte> result2 = null;
        final Integer returnNr = message.intAt(0);
        //used as a marker for the moment
        //final int limit = returnNr == null ? -1 : returnNr;
        final boolean isRange = keys != null && returnNr != null;
        final boolean isCollection = keys != null && returnNr == null;
               
        if (isCollection) {
        	if(sendBackResults) {
        		result1 = new HashMap<Number640, Data>(keys.size());
        		for (Number640 key : keys.keys()) {
                    Pair<Data,Enum<?>> data = storageLayer.remove(key, publicKey, sendBackResults);
                    notifyRemoveResponsibility(key.locationKey(), data.element1());
                    if(data.element0() != null) {
                    	result1.put(key, data.element0());
                    }
                }
        	} else {
        		result2 = new HashMap<Number640, Byte>(keys.size());
        		for (Number640 key : keys.keys()) {
                    Pair<Data,Enum<?>> data = storageLayer.remove(key, publicKey, sendBackResults);
                    notifyRemoveResponsibility(key.locationKey(), data.element1());
                    result2.put(key, (byte) data.element1().ordinal());
                }
        	}
            
        } else if(isRange) {
            Iterator<Number640> iterator = keys.keys().iterator();
            Number640 from = iterator.next();
            Number640 to = iterator.next();
            if(sendBackResults) {
            	result1 = storageLayer.removeReturnData(from, to, publicKey);
            } else {
            	result2 = storageLayer.removeReturnStatus(from, to, publicKey);
            }
        }
        else if (locationKey != null && domainKey != null) {
            Number640 from = new Number640(locationKey, domainKey, Number160.ZERO, Number160.ZERO);
            Number640 to = new Number640(locationKey, domainKey, Number160.MAX_VALUE, Number160.MAX_VALUE);
            if(sendBackResults) {
            	result1 = storageLayer.removeReturnData(from, to, publicKey);
            } else {
            	result2 = storageLayer.removeReturnStatus(from, to, publicKey);
            }
        } else {
            throw new IllegalArgumentException("Either two keys or a key set are necessary");
        }
        if (!sendBackResults) {
        	for(Map.Entry<Number640, Byte> entry:result2.entrySet()) {
        		notifyRemoveResponsibility(entry.getKey().locationKey(), PutStatus.values()[entry.getValue()]);
        	}
        	// make a copy, so the iterator in the codec wont conflict with
            // concurrent calls
            responseMessage.keyMapByte(new KeyMapByte(result2));
        } else {
        	for(Map.Entry<Number640, Data> entry:result1.entrySet()) {
        		notifyRemoveResponsibility(entry.getKey().locationKey(), PutStatus.OK);
        	}
        	// make a copy, so the iterator in the codec wont conflict with
            // concurrent calls
            responseMessage.setDataMap(new DataMap(result1));
        }
        return responseMessage;
    }

	private void notifyRemoveResponsibility(Number160 locationKey, Enum<?> status) {
		if (status == PutStatus.OK && replicationListener!=null) {
			replicationListener.dataRemoved(locationKey);
		}
    }
}
