/*
 * Copyright 2011 Thomas Bocek
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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.message.KeyMap640Keys;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.DistributedRouting;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.p2p.builder.BasicBuilder;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.p2p.builder.SearchableBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DefaultBloomfilterFactory;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedHashTable {
    private static final Logger logger = LoggerFactory.getLogger(DistributedHashTable.class);
    
    private static final NavigableSet<PeerAddress> EMPTY_NAVIGABLE_SET = new TreeSet<PeerAddress>();
    
    public static final int REASON_CANCEL = 254;
    public static final int REASON_UNKOWN = 255;

    private final DistributedRouting routing;

    private final StorageRPC storeRCP;

    private final DirectDataRPC directDataRPC;

    public DistributedHashTable(DistributedRouting routing, StorageRPC storeRCP, DirectDataRPC directDataRPC) {
        this.routing = routing;
        this.storeRCP = storeRCP;
        this.directDataRPC = directDataRPC;
    }

    public FuturePut add(final AddBuilder builder) {
        final FuturePut futureDHT = new FuturePut(builder, builder.requestP2PConfiguration()
                .minimumResults(), builder.dataSet().size());
        builder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                	final RoutingBuilder routingBuilder = createBuilder(builder);
                	final FutureRouting futureRouting = routing.route(routingBuilder, Type.REQUEST_1, future.channelCreator());
                	
                    futureDHT.futureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(final FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("adding lkey={} on {}", builder.locationKey(),
                                        futureRouting.potentialHits());
                                parallelRequests(builder.requestP2PConfiguration(),
                                		EMPTY_NAVIGABLE_SET , futureRouting.potentialHits(), futureDHT, false,
                                        future.channelCreator(), new OperationMapper<FuturePut>() {
                                            Map<PeerAddress, Map<Number640, Byte>> rawData = new HashMap<PeerAddress, Map<Number640, Byte>>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
                                                return storeRCP.add(address, builder, channelCreator);
                                            }

                                            @Override
                                            public void response(FuturePut futureDHT) {
                                                futureDHT.storedKeys(rawData);
                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, but we
                                                // need to check the result if we could store it.
                                                if (future.isSuccess() && future.responseMessage().isOk()) {
                                                    rawData.put(future.request().recipient(), future
                                                            .responseMessage().keyMapByte(0).keysMap());
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.failed(futureRouting);
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.channelCreator());
                } else {
                    futureDHT.failed(future);
                }
            }
        });
        return futureDHT;
    }

    /*
     * public FutureDHT direct(final Number160 locationKey, final ByteBuf buffer, final boolean raw, final
     * RoutingConfiguration routingConfiguration, final RequestP2PConfiguration p2pConfiguration, final
     * FutureCreate<FutureDHT> futureCreate, final boolean cancelOnFinish, final boolean manualCleanup, final
     * FutureChannelCreator futureChannelCreator, final ConnectionReservation connectionReservation) {
     */

    public FutureSend direct(final SendBuilder builder) {

        final FutureSend futureDHT = new FutureSend(builder, builder.requestP2PConfiguration()
                .minimumResults(), new VotingSchemeDHT());

        builder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                	
                	final RoutingBuilder routingBuilder = createBuilder(builder);
                	final FutureRouting futureRouting = routing.route(routingBuilder, Type.REQUEST_1, future.channelCreator());

                    futureDHT.futureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("storing lkey={} on {}", builder.locationKey(),
                                        futureRouting.potentialHits());
                                parallelRequests(builder.requestP2PConfiguration(),
                                		EMPTY_NAVIGABLE_SET, futureRouting.potentialHits(), futureDHT,
                                        builder.isCancelOnFinish(), future.channelCreator(),
                                        new OperationMapper<FutureSend>() {
                                            Map<PeerAddress, ByteBuf> rawChannels = new HashMap<PeerAddress, ByteBuf>();

                                            Map<PeerAddress, Object> rawObjects = new HashMap<PeerAddress, Object>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
                                                return directDataRPC.send(address, builder, channelCreator);
                                            }

                                            @Override
                                            public void response(FutureSend futureDHT) {
                                                if (builder.isRaw())
                                                    futureDHT.directData1(rawChannels);
                                                else
                                                    futureDHT.directData2(rawObjects);
                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, but we
                                                // need to check the result if we could store it.
                                                if (future.isSuccess() && future.responseMessage().isOk()) {
                                                    if (builder.isRaw()) {
                                                        rawChannels.put(future.request().recipient(),
                                                                future.responseMessage().buffer(0).buffer());
                                                    } else {
                                                        try {
                                                            rawObjects.put(
                                                                    future.request().recipient(),
                                                                    future.responseMessage().buffer(0)
                                                                            .object());
                                                        } catch (ClassNotFoundException e) {
                                                            rawObjects.put(
                                                                    future.request().recipient(), e);
                                                        } catch (IOException e) {
                                                            rawObjects.put(
                                                                    future.request().recipient(), e);
                                                        }
                                                    }
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.failed(futureRouting);
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.channelCreator());

                } else {
                    futureDHT.failed(future);
                }
            }
        });

        return futureDHT;
    }

    public FuturePut put(final PutBuilder putBuilder) {
        final int dataSize = UtilsDHT.dataSize(putBuilder);
        final FuturePut futureDHT = new FuturePut(putBuilder, putBuilder.requestP2PConfiguration()
                .minimumResults(), dataSize);
        putBuilder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                	
                	final RoutingBuilder routingBuilder = createBuilder(putBuilder);
                	final FutureRouting futureRouting = routing.route(routingBuilder, Type.REQUEST_1, future.channelCreator());
                	
                    futureDHT.futureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(final FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("storing lkey={} on {}", putBuilder.locationKey(),
                                        futureRouting.potentialHits());

                                parallelRequests(putBuilder.requestP2PConfiguration(),
                                		EMPTY_NAVIGABLE_SET, futureRouting.potentialHits(), futureDHT, false,
                                        future.channelCreator(), new OperationMapper<FuturePut>() {

                                            Map<PeerAddress, Map<Number640, Byte>> rawData = new HashMap<PeerAddress, Map<Number640, Byte>>();

                                            @Override
                                            public FutureResponse create(final ChannelCreator channelCreator,
                                                    final PeerAddress address) {
                                                if (putBuilder.isPutIfAbsent()) {
                                                    return storeRCP.putIfAbsent(address, putBuilder,
                                                            channelCreator);
                                                } else if (putBuilder.isPutMeta()) {
                                                	return storeRCP.putMeta(address, putBuilder,
                                                            channelCreator);
                                                } else if (putBuilder.isPutConfirm()) {
                                                	return storeRCP.putConfirm(address, putBuilder, channelCreator);
                                                } else {
                                                    return storeRCP.put(address, putBuilder, channelCreator);
                                                }
                                            }

                                            @Override
                                            public void response(final FuturePut futureDHT) {
                                                futureDHT.storedKeys(rawData);
                                            }

                                            @Override
                                            public void interMediateResponse(final FutureResponse future) {
                                                // the future tells us that the communication was successful, to check
                                                // the result if we could store it.
                                                if (future.isSuccess() && future.responseMessage().isOk()) {
                                                    rawData.put(future.request().recipient(), future
                                                            .responseMessage().keyMapByte(0).keysMap());
                                                } else {
                                                	if(future.emptyResponse() == null) {
                                                		Map<Number640, Byte> error = Utils.setMapError(future.request().dataMap(0).dataMap(), (byte) REASON_CANCEL);
                                                		rawData.put(future.request().recipient(), error);
                                                	} else {
                                                		logger.debug("future failed: "+future.failedReason());
                                                		Map<Number640, Byte> error = Utils.setMapError(future.request().dataMap(0).dataMap(), (byte) REASON_UNKOWN);
                                                        rawData.put(future.request().recipient(), error);
                                                	}
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.failed(futureRouting);
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.channelCreator());
                } else {
                    futureDHT.failed(future);
                }
            }
        });
        return futureDHT;
    }

    public FutureGet get(final GetBuilder builder) {

        final FutureGet futureDHT = new FutureGet(builder, builder.requestP2PConfiguration()
                .minimumResults(), new VotingSchemeDHT());

        builder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                	
                	final RoutingBuilder routingBuilder = createBuilder(builder);
                	fillRoutingBuilder(builder, routingBuilder);
                	final FutureRouting futureRouting = routing.route(routingBuilder, builder.isFastGet()? Type.REQUEST_2 : Type.REQUEST_1, future.channelCreator());

                    futureDHT.futureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("found direct hits for get: {}", futureRouting.directHits());
                                      
                                RequestP2PConfiguration p2pConfiguration2 = adjustConfiguration(builder.requestP2PConfiguration, 
                                		futureRouting.potentialHits().size());
                                        
                                // store in direct hits
                                parallelRequests(
                                        p2pConfiguration2,
                                        builder.isFastGet() ? futureRouting.directHits(): EMPTY_NAVIGABLE_SET,
                                        futureRouting.potentialHits(),
                                        futureDHT, true,
                                        future.channelCreator(), new OperationMapper<FutureGet>() {
                                            Map<PeerAddress, Map<Number640, Data>> rawData = new HashMap<PeerAddress, Map<Number640, Data>>();
                                            Map<PeerAddress, DigestResult> rawDigest = new HashMap<PeerAddress, DigestResult>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
												if (builder.isGetLatest()) {
													if (builder.isWithDigest()) {
														return storeRCP.getLatest(address, builder,
																channelCreator, RPC.Commands.GET_LATEST_WITH_DIGEST);
													} else {
														return storeRCP.getLatest(address, builder,
																channelCreator,
																RPC.Commands.GET_LATEST);
													}
												} else {
													return storeRCP.get(address, builder, channelCreator);
												}
                                            }

                                            @Override
                                            public void response(FutureGet futureDHT) {

                                                futureDHT.receivedData(rawData, rawDigest);

                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, which is
                                                // ok for digest
                                                if (future.isSuccess()) {
                                                    
                                                    Map<Number640, Data> data = future.responseMessage().dataMap(0).dataMap();
                                                    if(data !=null && !data.isEmpty()) {
                                                        rawData.put(future.request().recipient(), data);
                                                    }
													
													KeyMap640Keys keyMaps = future.responseMessage()
															.keyMap640Keys(0);
													if (keyMaps != null && keyMaps.keysMap() != null) {
														rawDigest.put(future.request().recipient(),
																new DigestResult(keyMaps.keysMap()));
													}

                                                    logger.debug("set data from {}", future.request()
                                                            .recipient());
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.failed(futureRouting);
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.channelCreator());
                } else {
                    futureDHT.failed(future);
                }
            }
        });
        return futureDHT;
    }

    public FutureDigest digest(final DigestBuilder builder) {
        final FutureDigest futureDHT = new FutureDigest(builder, builder.requestP2PConfiguration()
                .minimumResults(), new VotingSchemeDHT());

        builder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                	
                	final RoutingBuilder routingBuilder = createBuilder(builder);
                	fillRoutingBuilder(builder, routingBuilder);
                	final FutureRouting futureRouting = routing.route(routingBuilder, builder.isFastGet()? Type.REQUEST_2 : Type.REQUEST_1, future.channelCreator());
                    
                    futureDHT.futureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("found direct hits for digest: {}",
                                        futureRouting.directHits());
                                // store in direct hits
                                parallelRequests(
                                        builder.requestP2PConfiguration(),
                                        builder.isFastGet() ? futureRouting.directHits(): EMPTY_NAVIGABLE_SET,
                                        futureRouting.potentialHits(), 
                                        futureDHT, true,
                                        future.channelCreator(), new OperationMapper<FutureDigest>() {
                                            Map<PeerAddress, DigestResult> rawDigest = new HashMap<PeerAddress, DigestResult>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
                                                return storeRCP.digest(address, builder, channelCreator);
                                            }

                                            @Override
                                            public void response(FutureDigest futureDHT) {
                                                futureDHT.receivedDigest(rawDigest);
                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, which is
                                                // ok for digest
                                                if (future.isSuccess()) {
											        final DigestResult digest;
											        if (builder.isReturnMetaValues()) {
												        Map<Number640, Data> dataMap = future.responseMessage()
												                .dataMap(0).dataMap();
												        digest = new DigestResult(dataMap);
											        } else if (builder.isReturnBloomFilter()) {
												        SimpleBloomFilter<Number160> sbf1 = future.responseMessage()
												                .bloomFilter(0);
												        SimpleBloomFilter<Number160> sbf2 = future.responseMessage()
												                .bloomFilter(1);
												        digest = new DigestResult(sbf1, sbf2);
											        } else {
												        NavigableMap<Number640, Collection<Number160>> keyDigest = future
												                .responseMessage().keyMap640Keys(0).keysMap();
												        digest = new DigestResult(keyDigest);
											        }
											        rawDigest.put(future.request().recipient(), digest);
											        logger.debug("set data from {}", future.request().recipient());
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.failed(futureRouting);
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.channelCreator());
                } else {
                    futureDHT.failed(future);
                }
            }
        });
        return futureDHT;
    }

    /*
     * public FutureDHT remove(final Number160 locationKey, final Number160 domainKey, final Collection<Number160>
     * contentKeys, final RoutingConfiguration routingConfiguration, final RequestP2PConfiguration p2pConfiguration,
     * final boolean returnResults, final boolean signMessage, final boolean isManualCleanup, FutureCreate<FutureDHT>
     * futureCreate, final FutureChannelCreator futureChannelCreator, final ConnectionReservation connectionReservation)
     * {
     */
    public FutureRemove remove(final RemoveBuilder builder) {
        final FutureRemove futureDHT = new FutureRemove(builder, new VotingSchemeDHT());

        builder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                	
                	final RoutingBuilder routingBuilder = createBuilder(builder);
                    fillRoutingBuilder(builder, routingBuilder);
                	final FutureRouting futureRouting = routing.route(routingBuilder, builder.isFastGet() ? Type.REQUEST_2 : Type.REQUEST_1, future.channelCreator());

                    futureDHT.futureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {

                                logger.debug("found direct hits for remove: {}",
                                        futureRouting.directHits());

                                RequestP2PConfiguration p2pConfiguration2 = adjustConfiguration(builder.requestP2PConfiguration, 
                                		futureRouting.potentialHits().size());

                                parallelRequests(p2pConfiguration2, 
                                		builder.isFastGet() ? futureRouting.directHits(): EMPTY_NAVIGABLE_SET,
                                        futureRouting.potentialHits(),
                                		futureDHT, false, future.channelCreator(),
                                        new OperationMapper<FutureRemove>() {
                                            Map<PeerAddress, Map<Number640, Data>> rawDataResult = new HashMap<PeerAddress, Map<Number640, Data>>();

                                            Map<PeerAddress, Map<Number640, Byte>> rawDataNoResult = new HashMap<PeerAddress, Map<Number640, Byte>>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
                                                return storeRCP.remove(address, builder, channelCreator);
                                            }

                                            @Override
                                            public void response(FutureRemove futureDHT) {
                                                if (builder.isReturnResults()) {
                                                    futureDHT.receivedData(rawDataResult);
                                                } else {
                                                    futureDHT.storedKeys(rawDataNoResult);
                                                }
                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, but we
                                                // need to check the result if we could store it.
                                                if (future.isSuccess() && future.responseMessage().isOk()) {
                                                    if (builder.isReturnResults()) {
                                                        rawDataResult.put(future.request().recipient(),
                                                                future.responseMessage().dataMap(0).dataMap());
                                                    } else {
                                                        rawDataNoResult.put(future.request()
                                                                .recipient(), future.responseMessage()
                                                                .keyMapByte(0).keysMap());
                                                    }
                                                }
                                            }
                                        });
                            } else
                                futureDHT.failed(futureRouting);
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.channelCreator());
                } else {
                    futureDHT.failed(future);
                }

            }

			
        });
        return futureDHT;
    }

    /**
     * Creates RPCs and executes them parallel.
     * 
     * @param p2pConfiguration
     *            The configuration that specifies e.g. how many parallel requests there are.
     * @param queue
     *            The sorted set that will be queries. The first RPC takes the first in the queue.
     * @param futureDHT
     *            The future object that tracks the progress
     * @param cancleOnFinish
     *            Set to true if the operation should be canceled (e.g. file transfer) if the future has finished.
     * @param operation
     *            The operation that creates the request
     */
    public static <K extends FutureDHT<?>> K parallelRequests(final RequestP2PConfiguration p2pConfiguration,
            final NavigableSet<PeerAddress> directHit, final NavigableSet<PeerAddress> potentialHit, final boolean cancleOnFinish,
            final FutureChannelCreator futureChannelCreator, final OperationMapper<K> operation,
            final K futureDHT) {

        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    parallelRequests(p2pConfiguration, directHit, potentialHit, futureDHT, cancleOnFinish,
                            future.channelCreator(), operation);
                    Utils.addReleaseListener(future.channelCreator(), futureDHT);
                } else {
                    futureDHT.failed(future);
                }
            }
        });
        return futureDHT;
    }

    //TODO: have two queues, direct queue + potential queue.
    private static <K extends FutureDHT<?>> void parallelRequests(RequestP2PConfiguration p2pConfiguration,
    		NavigableSet<PeerAddress> directHit, NavigableSet<PeerAddress> potentialHit, K future, boolean cancleOnFinish, ChannelCreator channelCreator,
            OperationMapper<K> operation) {
    	//the potential hits may contain same values as in directHit, so remove it from potentialHit
    	for(PeerAddress peerAddress:directHit) {
    		potentialHit.remove(peerAddress);
    	}
    	
        if (p2pConfiguration.minimumResults() == 0) {
            operation.response(future);
            return;
        }
        FutureResponse[] futures = new FutureResponse[p2pConfiguration.parallel()];
        // here we split min and pardiff, par=min+pardiff
        loopRec(directHit, potentialHit, p2pConfiguration.minimumResults(), new AtomicInteger(0),
                p2pConfiguration.maxFailure(), p2pConfiguration.parallelDiff(),
                new AtomicReferenceArray<FutureResponse>(futures), future, cancleOnFinish, channelCreator,
                operation);
    }

    private static <K extends FutureDHT<?>> void loopRec(final NavigableSet<PeerAddress> directHit, final NavigableSet<PeerAddress> potentialHit,
            final int min, final AtomicInteger nrFailure, final int maxFailure, final int parallelDiff,
            final AtomicReferenceArray<FutureResponse> futures, final K futureDHT,
            final boolean cancelOnFinish, final ChannelCreator channelCreator,
            final OperationMapper<K> operation) {
        // final int parallel=min+parallelDiff;
        int active = 0;
        for (int i = 0; i < min + parallelDiff; i++) {
            if (futures.get(i) == null) {
                PeerAddress next = directHit.pollFirst();
                if(next == null) {
                	next = potentialHit.pollFirst();
                }
                if (next != null) {
                    active++;
                    FutureResponse futureResponse = operation.create(channelCreator, next);
                    futures.set(i, futureResponse);
                    futureDHT.addRequests(futureResponse);
                }
            } else {
                active++;
            }
        }
        if (active == 0) {
            operation.response(futureDHT);
            if (cancelOnFinish) {
                cancel(futures);
            }
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("fork/join status: " + min + "/" + active + " (" + parallelDiff + ")");
        }
        FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(Math.min(min, active), false,
                futures);
        fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
            @Override
            public void operationComplete(final FutureForkJoin<FutureResponse> future) throws Exception {
                for (FutureResponse futureResponse : future.completed()) {
                    operation.interMediateResponse(futureResponse);
                }
                // we are finished if forkjoin says so or we got too many
                // failures
                if (future.isSuccess() || nrFailure.incrementAndGet() > maxFailure) {
                    if (cancelOnFinish) {
                        cancel(futures);
                    }
                    operation.response(futureDHT);
                } else {
                    loopRec(directHit, potentialHit, min - future.successCounter(), nrFailure, maxFailure, parallelDiff,
                            futures, futureDHT, cancelOnFinish, channelCreator, operation);
                }
            }
        });
    }
    
    private static RoutingBuilder createBuilder(BasicBuilder<?> builder) {
    	RoutingBuilder routingBuilder = builder.createBuilder(builder.requestP2PConfiguration(),
                builder.routingConfiguration());
        routingBuilder.locationKey(builder.locationKey());
        routingBuilder.domainKey(builder.domainKey());
        routingBuilder.peerFilters(builder.peerFilters());
        return routingBuilder;
    }
    
    private static void fillRoutingBuilder(final SearchableBuilder builder, final RoutingBuilder routingBuilder) {
        if (builder.from()!=null && builder.to() !=null) {
        	routingBuilder.range(builder.from(), builder.to());
        } else if (builder.contentKeys() != null && builder.contentKeys().size() == 1) {
        	//builder.contentKeys() can be null if we search for all
        	routingBuilder.contentKey(builder.contentKeys().iterator().next());
        }
        else if(builder.contentKeys() != null && builder.contentKeys().size() > 1) {
        	//builder.contentKeys() can be null if we search for all
        	DefaultBloomfilterFactory factory = new DefaultBloomfilterFactory();
        	SimpleBloomFilter<Number160> bf = factory.createContentKeyBloomFilter();
        	for (Number160 contentKey : builder.contentKeys()) {
        		bf.add(contentKey);
        	}
        	routingBuilder.keyBloomFilter(bf);
        }
    }

    /**
     * Cancel the future that causes the underlying futures to cancel as well.
     */
    private static void cancel(final AtomicReferenceArray<FutureResponse> futures) {
        int len = futures.length();
        for (int i = 0; i < len; i++) {
            BaseFuture baseFuture = futures.get(i);
            if (baseFuture != null) {
                baseFuture.cancel();
            }
        }
    }

    /**
     * Adjusts the number of minimum requests in the P2P configuration. When we query x peers for the get() operation
     * and they have y different data stored (y <= x), then set the minimum to y or to the value the user set if its
     * smaller. If no data is found, then return 0, so we don't start P2P RPCs.
     * 
     * @param p2pConfiguration
     *            The old P2P configuration with the user specified minimum result
     * @param directHitsDigest
     *            The digest information from the routing process
     * @return The new RequestP2PConfiguration with the new minimum result
     */
    public static RequestP2PConfiguration adjustConfiguration(final RequestP2PConfiguration p2pConfiguration,
            final int size) {

        int requested = p2pConfiguration.minimumResults();
        if (size >= requested) {
            return p2pConfiguration;
        } else {
            return new RequestP2PConfiguration(size, p2pConfiguration.maxFailure(),
                    p2pConfiguration.parallelDiff());
        }
    }
}
