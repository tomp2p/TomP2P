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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DataBuffer;
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
public class SyncBuilder extends DHTBuilder<SyncBuilder> {

    private static final Logger LOG = LoggerFactory.getLogger(SyncBuilder.class);
    private static final FutureDone<SyncStat> FUTURE_SHUTDOWN = new FutureDone<SyncStat>()
            .setFailed("sync builder - peer is shutting down");
    static final int DEFAULT_BLOCK_SIZE = 700;
    
    private final PeerAddress other;
    private final PeerSync peerSync;
    private final int blockSize;

    private DataMap dataMap;
    private Number640 key;
    private Set<Number640> keys;
    private NavigableMap<Number640, Set<Number160>> dataMapHash;
    private ArrayList<Instruction> instructions;
    private boolean syncFromOldVersion = false;
    
    public SyncBuilder(final PeerSync peerSync, final PeerAddress other) {
    	this(peerSync, other, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Constructor.
     * 
     * @param peer
     *            The responsible peer that performs synchronization
     */
    public SyncBuilder(final PeerSync peerSync, final PeerAddress other, final int blockSize) {
        super(peerSync.peer(), Number160.ZERO);
        self(this);
        this.other = other;
        this.peerSync = peerSync;
        this.blockSize = blockSize;
    }

    public SyncBuilder dataMap(DataMap dataMap) {
        this.dataMap = dataMap;
        return this;
    }

    public Number640 key() {
        return key;
    }

    public SyncBuilder key(Number640 key) {
        this.key = key;
        return this;
    }

    public Set<Number640> keys() {
        return keys;
    }

    public SyncBuilder keys(Set<Number640> keys) {
        this.keys = keys;
        return this;
    }
    
    public SyncBuilder syncFromOldVersion() {
    	syncFromOldVersion = true;
        return this;
    }
    
    public boolean isSyncFromOldVersion() {
        return syncFromOldVersion;
    }

    public SyncBuilder syncFromOldVersion(boolean syncFromOldVersion) {
        this.syncFromOldVersion = syncFromOldVersion;
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

    public NavigableMap<Number640, Set<Number160>> dataMapHash() {
        if (dataMapHash == null) {
            dataMapHash = new TreeMap<Number640, Set<Number160>>();
        }
        if (dataMap != null) {
        	for (Map.Entry<Number640, Number160> entry : dataMap.convertToHash().entrySet()) {
        		Set<Number160> hashSet = new HashSet<Number160>(1);
        		hashSet.add(entry.getValue());
        		dataMapHash.put(key, hashSet);
        	}
        }
        if (key != null) {
        	Set<Number160> hashSet = new HashSet<Number160>(1);
        	hashSet.add(peer.getPeerBean().storage().get(key).hash());
            dataMapHash.put(key, hashSet);
        }
        if (keys != null) {
            for (Number640 key : keys) {
            	Set<Number160> hashSet = new HashSet<Number160>(1);
            	hashSet.add(peer.getPeerBean().storage().get(key).hash());
                dataMapHash.put(key, hashSet);
            }
        }
        return dataMapHash;
    }

    public ArrayList<Instruction> instructions() {
        return instructions;
    }

    @Override
    public SyncBuilder setDomainKey(final Number160 domainKey) {
        throw new IllegalArgumentException("Cannot be set here");
    }

    public FutureDone<SyncStat> start() {
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }
        final FutureDone<SyncStat> futureSync = new FutureDone<SyncStat>();
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(0, 2);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (!future2.isSuccess()) {
                    futureSync.setFailed(future2);
                    LOG.error("checkDirect failed {}", future2.getFailedReason());
                    return;
                }
                final FutureResponse futureResponse = peerSync.syncRPC().infoMessage(other,
                        SyncBuilder.this, future2.getChannelCreator());
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
                        	LOG.error("nothing received, something is wrong");
                            futureSync.setFailed("nothing received, something is wrong");
                            return;
                        }

                        Map<Number640, Data> retVal = new HashMap<Number640, Data>();
                        boolean syncMessageRequired = false;
                        int dataCopy = 0;
                        int dataOrig = 0;
                        //int dataCopyCount = 0;
                        //int diffCount = 0;
                        //int dataNotCopied = 0;
                        for (Map.Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {
                        	
                        	Data data = entry.getValue();
                        	if(data.length() == 0) {
                        		if(data.isFlag1()) {
                        			LOG.debug("no sync required");
                        			syncMessageRequired = false;
                        		} else if(data.isFlag2()) {
                        			LOG.debug("copy required for key {}",entry.getKey());
                        			syncMessageRequired = true;
                        			Data data2 = peer.getPeerBean().storage().get(entry.getKey());
                        			dataOrig += data2.length();
                        			//copy
                                    retVal.put(entry.getKey(), data2);
                                    dataCopy += data2.length();
                                    
                        		}
                        	} else {
                        		LOG.debug("sync required");
                        		syncMessageRequired = true;
                        		Data data2 = peer.getPeerBean().storage().get(entry.getKey());
                        		dataOrig += data2.length();
                        		final ByteBuf buffer = data.buffer();
                        		Number160 versionKey = SyncUtils.decodeHeader(buffer);
                        		Number160 hash = SyncUtils.decodeHeader(buffer);
             
                        		List<Checksum> checksums = SyncUtils.decodeChecksums(buffer);
                        		// TODO: don't copy data, toBytes does a copy!
                        		List<Instruction> instructions = RSync.instructions(
                                         data2.toBytes(), checksums, blockSize);
                        		
                        		AlternativeCompositeByteBuf abuf = AlternativeCompositeByteBuf.compBuffer();
                        		
                        		dataCopy += SyncUtils.encodeInstructions(instructions, versionKey, hash, abuf);
                        		DataBuffer dataBuffer = new DataBuffer(abuf);
                        		//diff
                        		Data data1 = new Data(dataBuffer).setFlag1();
                                retVal.put(entry.getKey(), data1);                    		
                        	}
                        }
                        final SyncStat syncStat = new SyncStat(peer.getPeerAddress().getPeerId(), other.getPeerId(), dataCopy, dataOrig);
                        if (syncMessageRequired) {
                        	SyncBuilder.this.dataMap(new DataMap(retVal));
                        	FutureResponse fr = peerSync.syncRPC().syncMessage(other,
                                    SyncBuilder.this, future2.getChannelCreator());
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
