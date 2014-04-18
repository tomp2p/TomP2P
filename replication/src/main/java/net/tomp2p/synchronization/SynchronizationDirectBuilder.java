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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.builder.DHTBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The builder for the synchronization. This class first sends an info message to get the checksums, then it checks what
 * needs to be done, (nothing, full copy, diff).
 * 
 * @author Thomas Bocek
 * @author Maxat Pernebayev
 * 
 */
public class SynchronizationDirectBuilder extends DHTBuilder<SynchronizationDirectBuilder> {

    private static final Logger LOG = LoggerFactory.getLogger(SynchronizationDirectBuilder.class);

    private static final FutureDone<SynchronizationStatistics> FUTURE_SHUTDOWN = new FutureDone<SynchronizationStatistics>()
            .setFailed("sync builder - peer is shutting down");

    private DataMap dataMap;
    private Number640 key;
    private Set<Number640> keys;

    private NavigableMap<Number640, Number160> dataMapHash;
    private ArrayList<Instruction> instructions;

    private final PeerAddress other;
    
    private final PeerSync peerSync;

    /**
     * Constructor.
     * 
     * @param peer
     *            The responsible peer that performs synchronization
     */
    public SynchronizationDirectBuilder(final PeerSync peerSync, final PeerAddress other) {
        super(peerSync.peer(), Number160.ZERO);
        self(this);
        this.other = other;
        this.peerSync = peerSync;
    }

    public SynchronizationDirectBuilder dataMap(DataMap dataMap) {
        this.dataMap = dataMap;
        return this;
    }

    public Number640 key() {
        return key;
    }

    public SynchronizationDirectBuilder key(Number640 key) {
        this.key = key;
        return this;
    }

    public Set<Number640> keys() {
        return keys;
    }

    public SynchronizationDirectBuilder keys(Set<Number640> keys) {
        this.keys = keys;
        return this;
    }

    public DataMap dataMap() {
        if (dataMap != null) {
            return dataMap;
        } else {
            Map<Number640, Data> newDataMap = new HashMap<Number640, Data>();
            if (key != null) {
                Data data = peer.getPeerBean().storage().get(key);
                if (data == null) {
                    data = new Data().setFlag2();
                }
                newDataMap.put(key, data);
            }
            if (keys != null) {
                for (Number640 key : keys) {
                    Data data = peer.getPeerBean().storage().get(key);
                    if (data == null) {
                        data = new Data().setFlag2();
                    }
                    newDataMap.put(key, data);
                }
            }
            if (newDataMap.size() > 0) {
                return new DataMap(newDataMap);
            } else {
                throw new IllegalArgumentException("Need either dataMap, key, or keys!");
            }
        }
    }

    public NavigableMap<Number640, Number160> dataMapHash() {
        if (dataMapHash == null) {
            dataMapHash = new TreeMap<Number640, Number160>();
        }
        if (dataMap != null) {
            dataMapHash.putAll(dataMap.convertToHash());
        }
        if (key != null) {
            dataMapHash.put(key, peer.getPeerBean().storage().get(key).hash());
        }
        if (keys != null) {
            for (Number640 key : keys) {
                dataMapHash.put(key, peer.getPeerBean().storage().get(key).hash());
            }
        }
        return dataMapHash;
    }

    public ArrayList<Instruction> instructions() {
        return instructions;
    }

    @Override
    public SynchronizationDirectBuilder setDomainKey(final Number160 domainKey) {
        throw new IllegalArgumentException("Cannot be set here");
    }

    public FutureDone<SynchronizationStatistics> start() {
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        final FutureDone<SynchronizationStatistics> futureSync = new FutureDone<SynchronizationStatistics>();
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(0, 2);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (!future2.isSuccess()) {
                    futureSync.setFailed(future2);
                    LOG.error("checkDirect failed {}", future2.getFailedReason());
                    return;
                }
                final FutureResponse futureResponse = peerSync.synchronizationRPC().infoMessage(other,
                        SynchronizationDirectBuilder.this, future2.getChannelCreator());
                futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                    @Override
                    public void operationComplete(FutureResponse future) throws Exception {
                        if (future.isFailed()) {
                            Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
                            futureSync.setFailed(future);
                            LOG.error("checkDirect failed {}", future.getFailedReason());
                            return;
                        }

                        Message responseMessage = future.getResponse();
                        DataMap dataMap = responseMessage.getDataMap(0);

                        if (dataMap == null) {
                            futureSync.setFailed("nothing received, something is wrong");
                            return;
                        }

                        Map<Number640, Data> retVal = new HashMap<Number640, Data>();
                        boolean secondMessageRequired = false;
                        int dataCopy = 0;
                        int dataCopyCount = 0;
                        int diffCount = 0;
                        int dataNotCopied = 0;
                        for (Map.Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {
                            byte[] data = entry.getValue().toBytes();
                            if (entry.getValue().length() == 1) {
                                // do nothing or copy everyting
                                if (data[0] == 0) {
                                    // do nothing
                                    retVal.put(entry.getKey(), new Data(new byte[] {}));
                                } else {
                                    // put everything
                                    secondMessageRequired = true;
                                    Data data2 = peer.getPeerBean().storage().get(entry.getKey());
                                    retVal.put(entry.getKey(), data2);
                                    dataCopy += data2.length();
                                    dataCopyCount++;
                                }
                            } else {
                                // put diff
                                secondMessageRequired = true;
                                ArrayList<Checksum> checksums = Synchronization.decodeChecksumList(data);
                                Data data2 = peer.getPeerBean().storage().get(entry.getKey());
                                ArrayList<Instruction> instructions = Synchronization.getInstructions(
                                        data2.toBytes(), checksums, Synchronization.SIZE);
                                for (Instruction instruction : instructions) {
                                    if (instruction.getReference() != -1) {
                                        dataNotCopied += Synchronization.SIZE;
                                    } else {
                                        dataCopy += instruction.literalSize();
                                    }
                                }
                                byte[] endoced = Synchronization.encodeInstructionList(instructions,
                                        dataMapHash.get(entry.getKey()));
                                Data data1 = new Data(endoced).setFlag1();
                                retVal.put(entry.getKey(), data1);
                                diffCount++;
                            }
                        }
                        final SynchronizationStatistics syncStat = new SynchronizationStatistics();
                        syncStat.dataCopy(dataCopy);
                        syncStat.dataCopyCount(dataCopyCount);
                        syncStat.diffCount(diffCount);
                        syncStat.dataNotCopied(dataNotCopied);
                        SynchronizationDirectBuilder.this.dataMap = new DataMap(retVal);

                        if (secondMessageRequired) {
                            FutureResponse fr = peerSync.synchronizationRPC().syncMessage(other,
                                    SynchronizationDirectBuilder.this, future2.getChannelCreator());
                            fr.addListener(new BaseFutureAdapter<FutureResponse>() {

                                @Override
                                public void operationComplete(FutureResponse future) throws Exception {
                                    if (future.isFailed()) {
                                        futureSync.setFailed(future);
                                    } else {
                                        futureSync.setDone(syncStat);
                                    }
                                }
                            });
                            Utils.addReleaseListener(future2.getChannelCreator(), fr, futureResponse);
                        } else {
                            futureSync.setDone(syncStat);
                            Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
                        }
                    }
                });
            }
        });
        return futureSync;
    }
}
