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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.KeyMap640;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageLayer.PutStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Synchronization RPC is used to synchronize data between peers by transferring only changes.
 * 
 * @author Thomas Bocek
 * @author Maxat Pernebayev
 * 
 */
public class SynchronizationRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SynchronizationRPC.class);

    public static final byte INFO_COMMAND = RPC.Commands.SYNC_INFO.getNr();
    public static final byte SYNC_COMMAND = RPC.Commands.SYNC.getNr();

    /**
     * Constructor that registers this RPC with the message handler.
     * 
     * @param peerBean
     *            The peer bean that contains data that is unique for each peer
     * @param connectionBean
     *            The connection bean that is unique per connection (multiple peers can share a single connection)
     */
    public SynchronizationRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        super(peerBean, connectionBean);
        register(INFO_COMMAND, SYNC_COMMAND);
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
            final SynchronizationDirectBuilder synchronizationBuilder, final ChannelCreator channelCreator) {
        final Message message = createMessage(remotePeer, INFO_COMMAND, Type.REQUEST_1);

        if (synchronizationBuilder.isSign()) {
            message.setPublicKeyAndSign(synchronizationBuilder.keyPair());
        }

        KeyMap640 keyMap = new KeyMap640(synchronizationBuilder.dataMapHash());
        message.setKeyMap640(keyMap);

        FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), synchronizationBuilder);
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
            final SynchronizationDirectBuilder synchronizationBuilder, final ChannelCreator channelCreator)
            throws IOException {
        final Message message = createMessage(remotePeer, SYNC_COMMAND, Type.REQUEST_1);

        if (synchronizationBuilder.isSign()) {
            message.setPublicKeyAndSign(synchronizationBuilder.keyPair());
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
        if (!(message.getCommand() == INFO_COMMAND || message.getCommand() == SYNC_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message responseMessage = createResponseMessage(message, Type.OK);
        if(message.getCommand() == INFO_COMMAND) {
            handleInfo(message, responseMessage, responder);
        } else if (message.getCommand() == SYNC_COMMAND) {
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
        LOG.debug("Info received: {} -> {}", message.getSender().getPeerId(), message.getRecipient()
                .getPeerId());

        KeyMap640 keysMap = message.getKeyMap640(0);

        Map<Number640, Data> retVal = new HashMap<Number640, Data>();
        

        for (Map.Entry<Number640, Number160> entry : keysMap.keysMap().entrySet()) {
            Data data = peerBean().storage().get(entry.getKey());
            if (data != null) {
                // found, check if same
                if (entry.getValue().equals(data.hash())) {
                    retVal.put(entry.getKey(), new Data(new byte[] { 0 }));
                    LOG.debug("no sync required");
                } else {
                    // get the checksums
                    ArrayList<Checksum> checksums = Synchronization.getChecksums(data.toBytes(),
                            Synchronization.SIZE);
                    
                    byte[] encoded = Synchronization.encodeChecksumList(checksums);
                    retVal.put(entry.getKey(), new Data(encoded));
                    LOG.debug("sync required");
                }
            } else {
                // not found
                retVal.put(entry.getKey(), new Data(new byte[] { 1 }));
                LOG.debug("copy required");
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
        LOG.debug("Sync received: {} -> {}", message.getSender().getPeerId(), message.getRecipient()
                .getPeerId());

        final DataMap dataMap = message.getDataMap(0);
        final PublicKey publicKey = message.getPublicKey(0);

        List<Number640> retVal = new ArrayList<Number640>(dataMap.size());

        for (Map.Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {

            if (entry.getValue().isFlag2()) {
                peerBean().storage().remove(entry.getKey(), publicKey, false);
            } else if (entry.getValue().length() > 0) {
                if (entry.getValue().isFlag1()) {
                    // diff
                    ArrayList<Instruction> instructions = Synchronization.decodeInstructionList(entry
                            .getValue().toBytes());
                    Number160 hash = Synchronization.decodeHash(entry.getValue().toBytes());

                    Data data = peerBean().storage().get(entry.getKey());

                    if (hash.equals(data.hash())) {
                        continue;
                    }
                    byte[] reconstructedValue = Synchronization.getReconstructedValue(data.toBytes(),
                            instructions, Synchronization.SIZE);
                    //TODO: domain protection?, make the flags configurable
                    Enum<?> status = peerBean().storage().put(entry.getKey(), new Data(reconstructedValue), publicKey, false, false);
                    if (status == PutStatus.OK) {
                        retVal.add(entry.getKey());
                    }

                } else {
                    // copy
                    //TODO: domain protection?, make the flags configurable
                    Enum<?> status = peerBean().storage().put(entry.getKey(), entry.getValue(),
                            message.getPublicKey(0), false, false);
                    if (status == PutStatus.OK) {
                        retVal.add(entry.getKey());
                    }

                }
                if (peerBean().replicationStorage() != null) {
                    peerBean().replicationStorage().updateAndNotifyResponsibilities(
                            entry.getKey().getLocationKey());
                }
            }
        }
        responseMessage.setKeyCollection(new KeyCollection(retVal));
        responder.response(responseMessage);
    }
}
