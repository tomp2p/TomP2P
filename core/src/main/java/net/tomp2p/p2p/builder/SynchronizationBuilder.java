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

package net.tomp2p.p2p.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message2;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.replication.Checksum;
import net.tomp2p.replication.Instruction;
import net.tomp2p.replication.Synchronization;
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
public class SynchronizationBuilder extends DHTBuilder<SynchronizationBuilder> {

    private static final Logger LOG = LoggerFactory.getLogger(SynchronizationBuilder.class);

    private DataMap dataMap;
    private Map<Number480, Number160> dataMapHash;
    private ArrayList<Instruction> instructions;

    private final PeerAddress other;

    /**
     * Constructor.
     * 
     * @param peer
     *            The responsible peer that performs synchronization
     */
    public SynchronizationBuilder(final Peer peer, final PeerAddress other) {
        super(peer, Number160.ZERO);
        self(this);
        this.other = other;
    }

    public SynchronizationBuilder dataMap(DataMap dataMap) {
        this.dataMap = dataMap;
        return this;
    }

    public DataMap dataMap() {
        return dataMap;
    }

    public Map<Number480, Number160> dataMapHash() {
        return dataMapHash;
    }

    public ArrayList<Instruction> instructions() {
        return instructions;
    }

    @Override
    public SynchronizationBuilder setDomainKey(final Number160 domainKey) {
        throw new IllegalArgumentException("Cannot be set here");
    }

    public FutureDone<Void> start() {
        final FutureDone<Void> futureSync = new FutureDone<>();
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(0, 2);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (!future2.isSuccess()) {
                    futureSync.setFailed(future2);
                    LOG.error("checkDirect failed {}", future2.getFailedReason());
                    return;
                }
                dataMapHash = dataMap.convertToHash();
                final FutureResponse futureResponse = peer.getSynchronizationRPC().infoMessage(other,
                        SynchronizationBuilder.this, future2.getChannelCreator());
                futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                    @Override
                    public void operationComplete(FutureResponse future) throws Exception {
                        if (future.isFailed()) {
                            Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
                            futureSync.setFailed(future);
                            LOG.error("checkDirect failed {}", future.getFailedReason());
                            return;
                        }

                        Message2 responseMessage = future.getResponse();
                        DataMap dataMap = responseMessage.getDataMap(0);

                        if (dataMap == null) {
                            futureSync.setFailed("nothing received, something is wrong");
                            return;
                        }

                        Map<Number480, Data> retVal = new HashMap<Number480, Data>();
                        boolean secondMessageRequired = false;
                        for (Map.Entry<Number480, Data> entry : dataMap.dataMap().entrySet()) {
                            byte[] data = entry.getValue().toBytes();
                            if (entry.getValue().length() == 1) {
                                // do nothing or copy everyting
                                if (data[0] == 0) {
                                    // do nothing
                                    retVal.put(entry.getKey(), new Data(new byte[] {}));
                                } else {
                                    // put everything
                                    secondMessageRequired = true;
                                    retVal.put(entry.getKey(), dataMap.dataMap().get(entry.getKey()));
                                }
                            } else {
                                // put diff
                                secondMessageRequired = true;
                                ArrayList<Checksum> checksums = Synchronization.decodeChecksumList(entry
                                        .getValue().toBytes());
                                ArrayList<Instruction> instructions = Synchronization.getInstructions(entry
                                        .getValue().toBytes(), checksums, Synchronization.SIZE);
                                byte[] endoced = Synchronization.encodeInstructionList(instructions,
                                        dataMapHash.get(entry.getKey()));
                                Data data1 = new Data(endoced, true);
                                retVal.put(entry.getKey(), data1);
                            }
                        }
                        dataMap = new DataMap(retVal);

                        if (secondMessageRequired) {
                            FutureResponse fr = peer.getSynchronizationRPC().syncMessage(other,
                                    SynchronizationBuilder.this, future2.getChannelCreator());
                            fr.addListener(new BaseFutureAdapter<FutureResponse>() {

                                @Override
                                public void operationComplete(FutureResponse future) throws Exception {
                                    if (future.isFailed()) {
                                        futureSync.setFailed(future);
                                    } else {
                                        futureSync.setDone();
                                    }
                                }
                            });
                            Utils.addReleaseListener(future2.getChannelCreator(), fr, futureResponse);
                        } else {
                            Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
                        }
                    }
                });
            }
        });
        return futureSync;
    }
}
