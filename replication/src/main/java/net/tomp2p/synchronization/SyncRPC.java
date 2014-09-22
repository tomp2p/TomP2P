/*
 * Copyright 2013 Thomas Bocek, Maxat Pernebayev
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

package net.tomp2p.synchronization;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.dht.ReplicationListener;
import net.tomp2p.dht.StorageLayer;
import net.tomp2p.dht.StorageLayer.PutStatus;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.KeyMap640Keys;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DataBuffer;
import net.tomp2p.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Synchronization RPC is used to synchronize data between peers by transferring only changes.
 * 
 * @author Thomas Bocek
 * @author Maxat Pernebayev
 * 
 */
public class SyncRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRPC.class);

    public static final byte INFO_COMMAND = RPC.Commands.SYNC_INFO.getNr();
    public static final byte SYNC_COMMAND = RPC.Commands.SYNC.getNr();
    
    private final int blockSize;
    private final StorageLayer storageLayer;
    private final ReplicationListener replicationListener;

    /**
     * Constructor that registers this RPC with the message handler.
     * 
     * @param peerBean
     *            The peer bean that contains data that is unique for each peer
     * @param connectionBean
     *            The connection bean that is unique per connection (multiple peers can share a single connection)
     * @param storageLayer 
     */
    public SyncRPC(final PeerBean peerBean, final ConnectionBean connectionBean, final int blockSize, StorageLayer storageLayer, ReplicationListener replicationListener) {
        super(peerBean, connectionBean);
        register(INFO_COMMAND, SYNC_COMMAND);
        this.blockSize = blockSize;
        this.storageLayer = storageLayer;
        this.replicationListener = replicationListener;
    }

    /**
     * Sends info message that asks whether the data is present at replica peer or not. If it is present whether it has
     * changed. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param synchronizationBuilder
     *            Used for keeping parameters that are sent
     * @param channelCreator
     *            The channel creator that creates connections
     * @return The future response to keep track of future events
     */
	public FutureResponse infoMessage(final PeerAddress remotePeer,
	        final SyncBuilder synchronizationBuilder, final ChannelCreator channelCreator) {
		final Message message = createMessage(remotePeer, INFO_COMMAND,
		        synchronizationBuilder.isSyncFromOldVersion() ? Type.REQUEST_2 : Type.REQUEST_1);

		if (synchronizationBuilder.isSign()) {
			message.publicKeyAndSign(synchronizationBuilder.keyPair());
		}

		KeyMap640Keys keyMap = new KeyMap640Keys(synchronizationBuilder.dataMapHash());
		message.keyMap640Keys(keyMap);

		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
		        peerBean(), connectionBean(), synchronizationBuilder);
		LOG.debug("Info sent {}", message);
		return requestHandler.sendTCP(channelCreator);
	}

    /**
     * Sends sync message that transfers the changed parts of data to a replica peer. This is an RPC
     * 
     * @param remotePeer
     *            The remote peer to send this message
     * @param synchronizationBuilder
     *            Used for keeping parameters that are sent
     * @param channelCreator
     *            The channel creator that creates connections
     * @return The future response to keep track of future events
     * @throws IOException
     */
    public FutureResponse syncMessage(final PeerAddress remotePeer,
            final SyncBuilder synchronizationBuilder, final ChannelCreator channelCreator)
            throws IOException {
        final Message message = createMessage(remotePeer, SYNC_COMMAND, Type.REQUEST_1);

        if (synchronizationBuilder.isSign()) {
            message.publicKeyAndSign(synchronizationBuilder.keyPair());
        }

        DataMap dataMap = synchronizationBuilder.dataMap();
        message.setDataMap(dataMap);

        FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), synchronizationBuilder);
        LOG.debug("Sync sent {}", message);
        return requestHandler.sendTCP(channelCreator);
    }

    @Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {
        if (!(message.command() == INFO_COMMAND || message.command() == SYNC_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message responseMessage = createResponseMessage(message, Type.OK);
        if(message.command() == INFO_COMMAND) {
            handleInfo(message, responseMessage, responder);
        } else if (message.command() == SYNC_COMMAND) {
            handleSync(message, responseMessage, responder);
        } else {
            throw new IllegalArgumentException("Message content is wrong");
        }
    }

    /**
     * Handles the info message and returns a reply. This is an RPC.
     * 
     * @param message
     *            The message from a responsible peer
     * @param responseMessage
     *            The response message to a responsible peer
     * @return The response message
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchAlgorithmException
     */
    private void handleInfo(final Message message, final Message responseMessage, Responder responder) {
        LOG.debug("Info received from {} -> I'm {}", message.sender().peerId(), message.recipient()
                .peerId());
        
        final boolean isSyncFromOldVersion = message.type() == Type.REQUEST_2;
        final KeyMap640Keys keysMap = message.keyMap640Keys(0);
        final Map<Number640, Data> retVal = new HashMap<Number640, Data>();
        
        for (Map.Entry<Number640, Collection<Number160>> entry : keysMap.keysMap().entrySet()) {
            Data data = storageLayer.get(entry.getKey());
            if(entry.getValue().size() != 1) {
            	continue;
            }
            if (data != null) {
                // found, check if same
                if (entry.getValue().iterator().next().equals(data.hash())) {
                    retVal.put(entry.getKey(), new Data().flag1());
                    LOG.debug("no sync required");
                } else {
                    // get the checksums
                	// TODO: don't copy data, toBytes does a copy!
                    List<Checksum> checksums = RSync.checksums(data.toBytes(), blockSize);
                    AlternativeCompositeByteBuf abuf = AlternativeCompositeByteBuf.compBuffer();
                    DataBuffer dataBuffer = SyncUtils.encodeChecksum(checksums, entry.getKey().versionKey(), data.hash(), abuf);
                    retVal.put(entry.getKey(), new Data(dataBuffer));
                    LOG.debug("sync required hash = {}", data.hash());
                }
            } else {
            	if(isSyncFromOldVersion) {
            		//TODO: the client could send us his history to figure out what the latest version in this history is
            		Entry<Number640, Data> latest = storageLayer.
            				get(entry.getKey().minVersionKey(), entry.getKey().maxVersionKey(), 1, false).lastEntry();
            		// TODO: don't copy data, toBytes does a copy!
            		List<Checksum> checksums = RSync.checksums(latest.getValue().toBytes(), blockSize);
            		AlternativeCompositeByteBuf abuf = AlternativeCompositeByteBuf.compBuffer();
                    DataBuffer dataBuffer = SyncUtils.encodeChecksum(checksums, latest.getKey().versionKey(), 
                    		latest.getValue().hash(), abuf);
                    retVal.put(entry.getKey(), new Data(dataBuffer));
                    LOG.debug("sync required for version");
            	} else {
            		// not found
            		retVal.put(entry.getKey(), new Data().flag2());
            		LOG.debug("copy required, not found on this peer {}", entry.getKey());
            	}
            }
        }
        responseMessage.setDataMap(new DataMap(retVal));
        responder.response(responseMessage);
    }

    /**
     * Handles the sync message by putting the changed part of data into a hash table. This is an RPC.
     * 
     * @param message
     *            The message from a responsible peer
     * @param responseMessage
     *            The response message to a responsible peer
     * @return The response message
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void handleSync(final Message message, final Message responseMessage, Responder responder) {
        LOG.debug("Sync received: got from {} -> I'm {}", message.sender().peerId(), message.recipient()
                .peerId());

        final DataMap dataMap = message.dataMap(0);
        final PublicKey publicKey = message.publicKey(0);
        final List<Number640> retVal = new ArrayList<Number640>(dataMap.size());

        for (Map.Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {
            if (entry.getValue().isFlag2()) {
            	LOG.debug("remove entry {}", entry.getKey());
            	Pair<Data, Enum<?>> result = storageLayer.remove(entry.getKey(), publicKey, false);
                if (replicationListener != null && result.element1() == PutStatus.OK) {
                	replicationListener.dataRemoved(entry.getKey().locationKey());
                }
            } else if (entry.getValue().length() > 0) {
                if (entry.getValue().isFlag1()) {
                    // diff
                	LOG.debug("handle diff {}", entry.getKey());
                	final ByteBuf buf = entry.getValue().buffer();
                	Number160 versionKey = SyncUtils.decodeHeader(buf);
                	Number160 hash = SyncUtils.decodeHeader(buf);
                    List<Instruction> instructions = SyncUtils.decodeInstructions(buf);

                    Data dataOld = storageLayer.get(new Number640(entry.getKey().locationDomainAndContentKey(), versionKey));

                    if (dataOld == null || !dataOld.hash().equals(hash)) {
                        continue;
                    }
                    // TODO: don't copy data, toBytes does a copy!
                    DataBuffer reconstructedValue = RSync.reconstruct(dataOld.toBytes(), instructions, blockSize);
                    //TODO: domain protection?, make the flags configurable
                    Enum<?> status = storageLayer.put(entry.getKey(), new Data(reconstructedValue), publicKey, false, false);
                    if (status == PutStatus.OK) {
                        retVal.add(entry.getKey());
                        if (replicationListener != null) {
                        	replicationListener.dataInserted(
                                    entry.getKey().locationKey());
                        }
                    }

                } else {
                    // copy
                	LOG.debug("handle copy {}", entry.getKey());
                    //TODO: domain protection?, make the flags configurable
                    Enum<?> status = storageLayer.put(entry.getKey(), entry.getValue(),
                            message.publicKey(0), false, false);
                    if (status == PutStatus.OK) {
                        retVal.add(entry.getKey());
                        if (replicationListener != null) {
                        	replicationListener.dataInserted(
                                    entry.getKey().locationKey());
                        }
                    }
                }
            }
        }
        responseMessage.keyCollection(new KeyCollection(retVal));
        responder.response(responseMessage);
    }
}
