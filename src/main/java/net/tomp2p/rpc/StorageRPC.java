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
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureSuccessEvaluator;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric.PutStatus;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageRPC extends ReplyHandler {
    final private static Logger logger = LoggerFactory.getLogger(StorageRPC.class);

    // TODO: find good values
    final static int bitArraySize = 9000;

    final static int expectedElements = 1000;

    final static private Random rnd = new Random();

    public StorageRPC(PeerBean peerBean, ConnectionBean connectionBean) {
        super(peerBean, connectionBean);
        registerIoHandler(Command.COMPARE_PUT, Command.PUT, Command.GET, Command.ADD, Command.REMOVE);
    }

    public PeerAddress getPeerAddress() {
        return getPeerBean().getServerPeerAddress();
    }

    /**
     * Stores data on a remote peer. Overwrites data if the data already exists.
     * This is an RPC.
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
     *            Set to true if the domain should be set to protected. This
     *            means that this domain is flagged an a public key is stored
     *            for this entry. An update or removal can only be made with the
     *            matching private key.
     * @param protectEntry
     *            Set to true if the entry should be set to protected. This
     *            means that this domain is flagged an a public key is stored
     *            for this entry. An update or removal can only be made with the
     *            matching private key.
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an
     *            entry, this needs to be set to true.
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse put(final PeerAddress remotePeer, final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMap, boolean protectDomain, boolean protectEntry, boolean signMessage,
            ChannelCreator channelCreator, boolean forceUDP, SenderCacheStrategy senderCacheStrategy) {
        final Type request;
        if (protectDomain) {
            request = Type.REQUEST_2;
        } else {
            request = Type.REQUEST_1;
        }
        return put(remotePeer, locationKey, domainKey, dataMap, request, protectDomain || signMessage || protectEntry,
                channelCreator, forceUDP, senderCacheStrategy);
    }

    /**
     * Stores data on a remote peer. Only stores data if the data does not
     * already exist. This is an RPC.
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
     *            Set to true if the domain should be set to protected. This
     *            means that this domain is flagged an a public key is stored
     *            for this entry. An update or removal can only be made with the
     *            matching private key.
     * @param protectEntry
     *            Set to true if the entry should be set to protected. This
     *            means that this domain is flagged an a public key is stored
     *            for this entry. An update or removal can only be made with the
     *            matching private key.
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an
     *            entry, this needs to be set to true.
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse putIfAbsent(final PeerAddress remotePeer, final Number160 locationKey,
            final Number160 domainKey, final Map<Number160, Data> dataMap, boolean protectDomain, boolean protectEntry,
            boolean signMessage, ChannelCreator channelCreator, boolean forceUDP,
            SenderCacheStrategy senderCacheStrategy) {
        final Type request;
        if (protectDomain) {
            request = Type.REQUEST_4;
        } else {
            request = Type.REQUEST_3;
        }
        return put(remotePeer, locationKey, domainKey, dataMap, request, protectDomain || signMessage || protectEntry,
                channelCreator, forceUDP, senderCacheStrategy);
    }

    /**
     * Compares and puts data on a peer. It first compares the hashes that the
     * user provided on the remote peer, and if the hashes match, the data is
     * stored. If the flag partial put has been set, then it will store those
     * data where the hashes match and ignore the others. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to store the data
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param hashDataMap
     *            The map with the data and the hashes to compare to
     * @param futureSuccessEvaluator
     *            The evaluator that determines if a future was a success.
     * @param protectDomain
     *            Protect the domain
     * @param protectEntry
     *            Protect the entry
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an
     *            entry, this needs to be set to true.
     * @param partialPut
     *            Set to true if partial puts should be allowed. If set to
     *            false, then the complete map must match the hash, otherwise it
     *            wont be stored.
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse compareAndPut(final PeerAddress remotePeer, final Number160 locationKey,
            final Number160 domainKey, final Map<Number160, HashData> hashDataMap,
            final FutureSuccessEvaluator futureSuccessEvaluator, boolean protectDomain, boolean protectEntry,
            boolean signMessage, boolean partialPut, ChannelCreator channelCreator, boolean forceUDP) {
        Utils.nullCheck(remotePeer, locationKey, domainKey, hashDataMap);
        final Message message;
        if (protectDomain) {
            message = createMessage(remotePeer, Command.COMPARE_PUT, partialPut ? Type.REQUEST_4 : Type.REQUEST_2);
        } else {
            message = createMessage(remotePeer, Command.COMPARE_PUT, partialPut ? Type.REQUEST_3 : Type.REQUEST_1);
        }
        if (signMessage) {
            message.setPublicKeyAndSign(getPeerBean().getKeyPair());
        }
        message.setKeyKey(locationKey, domainKey);
        message.setHashDataMap(hashDataMap);
        FutureResponse futureResponse = new FutureResponse(message, futureSuccessEvaluator);
        if (!forceUDP) {
            final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
            return request.sendTCP(channelCreator);
        } else {
            final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
            return request.sendUDP(channelCreator);
        }
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
     *            The type of put request, this depends on
     *            put/putIfAbsent/protected/not-protected
     * @param signMessage
     *            Set to true to sign message
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    private FutureResponse put(final PeerAddress remotePeer, final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMap, final Type type, boolean signMessage, ChannelCreator channelCreator,
            boolean forceUDP, SenderCacheStrategy senderCacheStrategy) {
        Utils.nullCheck(remotePeer, locationKey, domainKey, dataMap);
        final Message message = createMessage(remotePeer, Command.PUT, type);
        if (signMessage) {
            message.setPublicKeyAndSign(getPeerBean().getKeyPair());
        }
        message.setKeyKey(locationKey, domainKey);
        message.setDataMap(dataMap);

        FutureResponse futureResponse = new FutureResponse(message);
        if (senderCacheStrategy == null) {
            if (!forceUDP) {
                final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse,
                        getPeerBean(), getConnectionBean(), message);
                return request.sendTCP(channelCreator);
            } else {
                final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse,
                        getPeerBean(), getConnectionBean(), message);
                return request.sendUDP(channelCreator);
            }
        } else {
            if (forceUDP) {
                throw new IllegalArgumentException("cannot bulk transfer with UDP");
            }

            final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
            CacheKey cacheKey = new CacheKey(remotePeer, type, signMessage);
            return senderCacheStrategy.putIfAbsent(cacheKey, request, channelCreator);
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
     * @param remotePeer
     *            The remote peer to store the data
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataSet
     *            The set with data. This will be converted to a map. The key
     *            for the map is the SHA-1 of the data.
     * @param protectDomain
     *            Set to true if the domain should be set to protected. This
     *            means that this domain is flagged an a public key is stored
     *            for this entry. An update or removal can only be made with the
     *            matching private key.
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an
     *            entry, this needs to be set to true.
     * @param channelCreator
     *            The channel creator
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse add(final PeerAddress remotePeer, final Number160 locationKey, final Number160 domainKey,
            final Collection<Data> dataSet, boolean protectDomain, boolean signMessage, boolean list,
            ChannelCreator channelCreator, boolean forceUDP, SenderCacheStrategy senderCacheStrategy) {
        Utils.nullCheck(remotePeer, locationKey, domainKey, dataSet);
        final Type type;
        if (protectDomain) {
            if (list) {
                type = Type.REQUEST_4;
            } else {
                type = Type.REQUEST_2;
            }
        } else {
            if (list) {
                type = Type.REQUEST_3;
            } else {
                type = Type.REQUEST_1;
            }
        }

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
        if (senderCacheStrategy == null) {
            if (!forceUDP) {
                final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse,
                        getPeerBean(), getConnectionBean(), message);
                return request.sendTCP(channelCreator);
            } else {
                final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse,
                        getPeerBean(), getConnectionBean(), message);
                return request.sendUDP(channelCreator);
            }
        } else {
            if (forceUDP) {
                throw new IllegalArgumentException("cannot bulk transfer with UDP");
            }
            final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
            CacheKey cacheKey = new CacheKey(remotePeer, type, signMessage);
            return senderCacheStrategy.putIfAbsent(cacheKey, request, channelCreator);
        }
    }

    /**
     * Get the data from a remote peer. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param contentKeys
     *            The content keys or null if requested all
     * @param signMessage
     *            Adds a public key and signs the message
     * @param digest
     *            Returns a list of hashes of the data stored on this peer
     * @param channelCreator
     *            The channel creator that creates connections. Typically we
     *            need one connection here.
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return The future response to keep track of future events
     */
    public FutureResponse get(final PeerAddress remotePeer, final Number160 locationKey, final Number160 domainKey,
            final Collection<Number160> contentKeys, final SimpleBloomFilter<Number160> keyBloomFilter,
            final SimpleBloomFilter<Number160> contentBloomFilter, final boolean signMessage, final boolean digest,
            final boolean returnBloomFilter, final boolean range, final ChannelCreator channelCreator,
            final boolean forceUDP) {
        Utils.nullCheck(remotePeer, locationKey, domainKey);
        Type type;
        if (range && !digest) {
            type = Type.REQUEST_4;
        } else if (!range && !digest) {
            type = Type.REQUEST_1;
        } else if (digest && !returnBloomFilter) {
            type = Type.REQUEST_2;
        } else
        // if(digest && returnBloomFilter)
        {
            // TODO: sent arguments bitArraySize and expectedElement
            type = Type.REQUEST_3;
        }
        final Message message = createMessage(remotePeer, Command.GET, type);
        if (signMessage) {
            message.setPublicKeyAndSign(getPeerBean().getKeyPair());
        }
        message.setKeyKey(locationKey, domainKey);
        if (contentKeys != null) {
            message.setKeys(contentKeys);
        } else if (keyBloomFilter != null || contentBloomFilter != null) {
            message.setTwoBloomFilter(keyBloomFilter, contentBloomFilter);
        }
        FutureResponse futureResponse = new FutureResponse(message);
        if (!forceUDP) {
            final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
            return request.sendTCP(channelCreator);
        } else {
            final RequestHandlerUDP<FutureResponse> request = new RequestHandlerUDP<FutureResponse>(futureResponse,
                    getPeerBean(), getConnectionBean(), message);
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
     *            Adds a public key and signs the message. For protected entry
     *            and domains, this needs to be provided.
     * @param channelCreator
     *            The channel creator that creates connections
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP
     * @return The future response to keep track of future events
     */
    public FutureResponse remove(final PeerAddress remotePeer, final Number160 locationKey, final Number160 domainKey,
            final Collection<Number160> contentKeys, final boolean sendBackResults, final boolean signMessage,
            ChannelCreator channelCreator, boolean forceUDP) {
        Utils.nullCheck(remotePeer, locationKey, domainKey);
        final Message message = createMessage(remotePeer, Command.REMOVE, sendBackResults ? Type.REQUEST_2
                : Type.REQUEST_1);
        if (signMessage) {
            message.setPublicKeyAndSign(getPeerBean().getKeyPair());
        }
        message.setKeyKey(locationKey, domainKey);
        if (contentKeys != null)
            message.setKeys(contentKeys);
        FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandlerTCP<FutureResponse> request = new RequestHandlerTCP<FutureResponse>(futureResponse,
                getPeerBean(), getConnectionBean(), message);
        return request.sendTCP(channelCreator);
    }

    @Override
    public Message handleResponse(final Message message, boolean sign) throws IOException {
        if (!(message.getCommand() == Command.ADD || message.getCommand() == Command.PUT
                || message.getCommand() == Command.GET || message.getCommand() == Command.REMOVE || message
                    .getCommand() == Command.COMPARE_PUT)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message responseMessage = createResponseMessage(message, Type.OK);
        if (sign) {
            responseMessage.setPublicKeyAndSign(getPeerBean().getKeyPair());
        }
        switch (message.getCommand()) {
        case ADD: {
            boolean isNull = Utils.nullCheckRetVal(message.getKeyKey1(), message.getKeyKey2(), message.getDataMap());
            if (isNull) {
                Utils.nullCheck(message.getDataMap480());
            }
            return handleAdd(message, responseMessage, isDomainProtected(message), isNull);
        }
        case PUT: {
            boolean isNull = Utils.nullCheckRetVal(message.getKeyKey1(), message.getKeyKey2(), message.getDataMap());
            if (isNull) {
                Utils.nullCheck(message.getDataMap480());
            }
            return handlePut(message, responseMessage, isStoreIfAbsent(message), isDomainProtected(message), isNull);
        }
        case GET: {
            Utils.nullCheck(message.getKeyKey1(), message.getKeyKey2());
            return handleGet(message, responseMessage);
        }

        case REMOVE: {
            Utils.nullCheck(message.getKeyKey1(), message.getKeyKey2());
            return handleRemove(message, responseMessage, message.getType() == Type.REQUEST_2);
        }
        case COMPARE_PUT: {
            Utils.nullCheck(message.getKeyKey1(), message.getKeyKey2(), message.getHashDataMap());
            return handleCompareAndPut(message, responseMessage, isPartial(message), isDomainProtected(message));
        }
        default: {
            throw new IllegalArgumentException("Message content is wrong");
        }
        }
    }

    private boolean isDomainProtected(final Message message) {
        boolean protectDomain = message.getPublicKey() != null
                && (message.getType() == Type.REQUEST_2 || message.getType() == Type.REQUEST_4);
        return protectDomain;
    }

    private boolean isStoreIfAbsent(final Message message) {
        boolean absent = message.getType() == Type.REQUEST_3 || message.getType() == Type.REQUEST_4;
        return absent;
    }

    private boolean isPartial(final Message message) {
        boolean partial = message.getType() == Type.REQUEST_3 || message.getType() == Type.REQUEST_4;
        return partial;
    }

    private boolean isList(final Message message) {
        boolean partial = message.getType() == Type.REQUEST_3 || message.getType() == Type.REQUEST_4;
        return partial;
    }

    private Message handlePut(final Message message, final Message responseMessage, final boolean putIfAbsent,
            final boolean protectDomain, final boolean isDataMap480) throws IOException {
        final PublicKey publicKey = message.getPublicKey();
        final int dataSize;
        if (isDataMap480) {
            final Collection<Number480> result = new HashSet<Number480>();
            final Map<Number480, Data> toStore = message.getDataMap480();
            dataSize = toStore.size();
            for (Map.Entry<Number480, Data> entry : toStore.entrySet()) {
                // here we set the map with the close peers. If we get data by a
                // sender
                // and the sender is closer than us, we assume that the sender
                // has the
                // data and we don't need to transfer data to the closest
                // (sender) peer.
                if (getPeerBean().getReplicationStorage() != null) {
                    getPeerBean().getReplicationStorage().updatePeerMapIfCloser(entry.getKey().getLocationKey(),
                            message.getSender().getID());
                }

                if (doPut(putIfAbsent, protectDomain, publicKey, entry.getKey().getLocationKey(), entry.getKey()
                        .getDomainKey(), entry.getKey().getContentKey(), entry.getValue())) {
                    result.add(entry.getKey());
                    // check the responsibility of the newly added data, do
                    // something
                    // (notify) if we are responsible
                    if (getPeerBean().getReplicationStorage() != null) {
                        getPeerBean().getReplicationStorage().checkResponsibility(entry.getKey().getLocationKey());
                    }
                }
            }

            if (result.size() == 0 && !putIfAbsent) {
                responseMessage.setType(Type.DENIED);
            } else if (result.size() == 0 && putIfAbsent) {
                // put if absent does not return an error if it did not work!
                responseMessage.setType(Type.OK);
                responseMessage.setKeys480(result);
            } else {
                responseMessage.setKeys480(result);
                if (result.size() != dataSize) {
                    responseMessage.setType(Type.PARTIALLY_OK);
                }
            }
        } else {
            final Collection<Number160> result = new HashSet<Number160>();
            final Number160 locationKey = message.getKeyKey1();
            final Number160 domainKey = message.getKeyKey2();

            final Map<Number160, Data> toStore = message.getDataMap();
            dataSize = toStore.size();

            // here we set the map with the close peers. If we get data by a
            // sender
            // and the sender is closer than us, we assume that the sender has
            // the
            // data and we don't need to transfer data to the closest (sender)
            // peer.
            if (getPeerBean().getReplicationStorage() != null)
                getPeerBean().getReplicationStorage().updatePeerMapIfCloser(locationKey, message.getSender().getID());

            for (Map.Entry<Number160, Data> entry : toStore.entrySet()) {
                if (doPut(putIfAbsent, protectDomain, publicKey, locationKey, domainKey, entry.getKey(),
                        entry.getValue())) {
                    result.add(entry.getKey());
                }
            }
            // check the responsibility of the newly added data, do something
            // (notify) if we are responsible
            if (result.size() > 0 && getPeerBean().getReplicationStorage() != null) {
                getPeerBean().getReplicationStorage().checkResponsibility(locationKey);
            }

            if (result.size() == 0 && !putIfAbsent) {
                responseMessage.setType(Type.DENIED);
            } else if (result.size() == 0 && putIfAbsent) {
                // put if absent does not return an error if it did not work!
                responseMessage.setType(Type.OK);
                responseMessage.setKeys(result);
            } else {
                responseMessage.setKeys(result);
                if (result.size() != dataSize) {
                    responseMessage.setType(Type.PARTIALLY_OK);
                }
            }
        }

        return responseMessage;
    }

    private boolean doPut(final boolean putIfAbsent, final boolean protectDomain, final PublicKey publicKey,
            final Number160 locationKey, final Number160 domainKey, final Number160 contentKey, final Data value) {
        if (getPeerBean().getStorage().put(locationKey, domainKey, contentKey, value, publicKey, putIfAbsent,
                protectDomain) == PutStatus.OK) {
            if (logger.isDebugEnabled()) {
                logger.debug("put data with key " + locationKey + " on " + getPeerBean().getServerPeerAddress());
            }
            return true;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("could not add " + locationKey + " on " + getPeerBean().getServerPeerAddress());
            }
        }
        return false;
    }

    private Message handleAdd(final Message message, final Message responseMessage, final boolean protectDomain,
            final boolean isDataMap480) {
        if (isDataMap480) {
            final Collection<Number480> result = new HashSet<Number480>();
            final Map<Number480, Data> data480 = message.getDataMap480();
            final PublicKey publicKey = message.getPublicKey();
            final boolean list = isList(message);
            // here we set the map with the close peers. If we get data by a
            // sender
            // and the sender is closer than us, we assume that the sender has
            // the
            // data and we don't need to transfer data to the closest (sender)
            // peer.

            for (Map.Entry<Number480, Data> entry : data480.entrySet()) {
                if (getPeerBean().getReplicationStorage() != null) {
                    getPeerBean().getReplicationStorage().updatePeerMapIfCloser(entry.getKey().getLocationKey(),
                            message.getSender().getID());
                }
                Number160 contentKey = doAdd(protectDomain, entry.getKey().getLocationKey(), entry.getKey()
                        .getDomainKey(), entry.getKey().getContentKey(), entry.getValue(), publicKey, list,
                        getPeerBean());
                if (contentKey != null) {
                    result.add(new Number480(entry.getKey().getLocationKey(), entry.getKey().getDomainKey(), contentKey));
                }
                // check the responsibility of the newly added data, do
                // something
                // (notify) if we are responsible
                if (result.size() > 0 && getPeerBean().getReplicationStorage() != null) {
                    getPeerBean().getReplicationStorage().checkResponsibility(entry.getKey().getLocationKey());
                }

            }
            responseMessage.setKeys480(result);
        } else {
            final Number160 locationKey = message.getKeyKey1();
            final Number160 domainKey = message.getKeyKey2();
            final Map<Number160, Data> data = message.getDataMap();
            final PublicKey publicKey = message.getPublicKey();
            final boolean list = isList(message);
            // here we set the map with the close peers. If we get data by a
            // sender
            // and the sender is closer than us, we assume that the sender has
            // the
            // data and we don't need to transfer data to the closest (sender)
            // peer.
            if (getPeerBean().getReplicationStorage() != null) {
                getPeerBean().getReplicationStorage().updatePeerMapIfCloser(locationKey, message.getSender().getID());
            }
            Collection<Number160> result = new HashSet<Number160>();
            for (Map.Entry<Number160, Data> entry : data.entrySet()) {
                Number160 contentKey = doAdd(protectDomain, locationKey, domainKey, entry.getKey(), entry.getValue(),
                        publicKey, list, getPeerBean());
                if (contentKey != null) {
                    result.add(contentKey);
                }
            }
            // check the responsibility of the newly added data, do something
            // (notify) if we are responsible
            if (result.size() > 0 && getPeerBean().getReplicationStorage() != null) {
                getPeerBean().getReplicationStorage().checkResponsibility(locationKey);
            }
            responseMessage.setKeys(result);
        }
        return responseMessage;
    }

    private static Number160 doAdd(final boolean protectDomain, final Number160 locationKey, final Number160 domainKey,
            final Number160 contentKey, final Data value, final PublicKey publicKey, final boolean list,
            PeerBean peerBean) {
        if (list) {
            Number160 contentKey2 = new Number160(rnd);
            PutStatus status;
            while ((status = peerBean.getStorage().put(locationKey, domainKey, contentKey2, value, publicKey, true,
                    protectDomain)) == PutStatus.FAILED_NOT_ABSENT) {
                contentKey2 = new Number160(rnd);
            }
            if (status == PutStatus.OK) {
                if (logger.isDebugEnabled()) {
                    logger.debug("add list data with key " + locationKey + " on " + peerBean.getServerPeerAddress());
                }
                return contentKey2;
            }
        } else {
            if (peerBean.getStorage().put(locationKey, domainKey, contentKey, value, publicKey, false, protectDomain) == PutStatus.OK) {
                if (logger.isDebugEnabled()) {
                    logger.debug("add data with key " + locationKey + " on " + peerBean.getServerPeerAddress());
                }
                return contentKey;
            }
        }
        return null;
    }

    private Message handleGet(final Message message, final Message responseMessage) {
        final Number160 locationKey = message.getKeyKey1();
        final Number160 domainKey = message.getKeyKey2();
        final Collection<Number160> contentKeys = message.getKeys();
        final SimpleBloomFilter<Number160> keyBloomFilter = message.getBloomFilter1();
        final SimpleBloomFilter<Number160> contentBloomFilter = message.getBloomFilter2();
        final boolean range = message.getType() == Type.REQUEST_4;
        final boolean digest = message.getType() == Type.REQUEST_2 || message.getType() == Type.REQUEST_3;
        if (digest) {
            final DigestInfo digestInfo;
            if (keyBloomFilter != null || contentBloomFilter != null) {
                digestInfo = getPeerBean().getStorage().digest(locationKey, domainKey, keyBloomFilter,
                        contentBloomFilter);
            } else {
                digestInfo = getPeerBean().getStorage().digest(locationKey, domainKey, contentKeys);
            }
            if (message.getType() == Type.REQUEST_2) {
                responseMessage.setKeyMap(digestInfo.getDigests());
            } else if (message.getType() == Type.REQUEST_3) {
                // TODO: make size good enough
                responseMessage.setTwoBloomFilter(digestInfo.getKeyBloomFilter(bitArraySize, expectedElements),
                        digestInfo.getContentBloomFilter(bitArraySize, expectedElements));
            }
            return responseMessage;
        } else {
            final Map<Number480, Data> result;
            if (contentKeys != null) {
                result = new HashMap<Number480, Data>();
                if (!range || contentKeys.size() != 2) {
                    for (Number160 contentKey : contentKeys) {
                        Data data = getPeerBean().getStorage().get(locationKey, domainKey, contentKey);
                        if (data != null) {
                            Number480 key = new Number480(locationKey, domainKey, contentKey);
                            result.put(key, data);
                        }
                    }
                } else {
                    // get min/max
                    Iterator<Number160> iterator = contentKeys.iterator();
                    Number160 min = iterator.next();
                    Number160 max = iterator.next();
                    SortedMap<Number480, Data> map = getPeerBean().getStorage().get(locationKey, domainKey, min, max);
                    Number320 lockKey = new Number320(locationKey, domainKey);
                    Lock lock = getPeerBean().getStorage().getLockNumber320().lock(lockKey);
                    try {
                        result.putAll(map);
                    } finally {
                        getPeerBean().getStorage().getLockNumber320().unlock(lockKey, lock);
                    }
                }
            } else if (keyBloomFilter != null || contentBloomFilter != null) {
                result = new HashMap<Number480, Data>();
                // TODO: idea, make the get with filters and push this to the
                // storage
                SortedMap<Number480, Data> tmp = getPeerBean().getStorage().get(locationKey, domainKey, Number160.ZERO,
                        Number160.MAX_VALUE);
                Number320 lockKey = new Number320(locationKey, domainKey);
                Lock lock = getPeerBean().getStorage().getLockNumber320().lock(lockKey);
                try {
                    for (Map.Entry<Number480, Data> entry : tmp.entrySet()) {
                        if (keyBloomFilter == null || keyBloomFilter.contains(entry.getKey().getContentKey())) {
                            if (contentBloomFilter == null || contentBloomFilter.contains(entry.getValue().getHash())) {
                                result.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                } finally {
                    getPeerBean().getStorage().getLockNumber320().unlock(lockKey, lock);
                }
            } else {
                result = getPeerBean().getStorage().get(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE);
            }
            responseMessage.setDataMapConvert(result);
            return responseMessage;
        }
    }

    private Message handleRemove(final Message message, final Message responseMessage, final boolean sendBackResults) {
        final Number160 locationKey = message.getKeyKey1();
        final Number160 domainKey = message.getKeyKey2();
        final Collection<Number160> contentKeys = message.getKeys();
        final PublicKey publicKey = message.getPublicKey();
        final Map<Number480, Data> result;
        if (contentKeys != null) {
            result = new HashMap<Number480, Data>();
            for (Number160 contentKey : contentKeys) {
                Number480 key = new Number480(locationKey, domainKey, contentKey);
                Data data = getPeerBean().getStorage().remove(locationKey, domainKey, contentKey, publicKey);
                if (data != null)
                    result.put(key, data);
            }
        } else {
            result = getPeerBean().getStorage().remove(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE,
                    publicKey);
        }
        if (!sendBackResults) {
            // make a copy, so the iterator in the codec wont conflict with
            // concurrent calls
            responseMessage.setKeysConvert(result.keySet());
        } else {
            // make a copy, so the iterator in the codec wont conflict with
            // concurrent calls
            responseMessage.setDataMapConvert(result);
        }
        return responseMessage;
    }

    private Message handleCompareAndPut(Message message, Message responseMessage, boolean partial, boolean protectDomain) {
        final Number160 locationKey = message.getKeyKey1();
        final Number160 domainKey = message.getKeyKey2();
        final Map<Number160, HashData> hashDataMap = message.getHashDataMap();
        final PublicKey publicKey = message.getPublicKey();

        final Collection<Number160> result = getPeerBean().getStorage().compareAndPut(locationKey, domainKey,
                hashDataMap, publicKey, partial, protectDomain);

        if (logger.isDebugEnabled()) {
            logger.debug("stored " + result.size());
        }
        if (result.size() > 0 && getPeerBean().getReplicationStorage() != null)
            getPeerBean().getReplicationStorage().checkResponsibility(locationKey);
        responseMessage.setKeys(result);
        if (result.size() == 0) {
            responseMessage.setType(Type.NOT_FOUND);
        } else if (result.size() != hashDataMap.size()) {
            responseMessage.setType(Type.PARTIALLY_OK);
        }
        return responseMessage;
    }
}
