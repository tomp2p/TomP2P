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
package net.tomp2p.p2p;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureGet;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.futures.FutureRemove;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureShutdown;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.p2p.builder.AddBuilder;
import net.tomp2p.p2p.builder.BasicBuilder;
import net.tomp2p.p2p.builder.GetBuilder;
import net.tomp2p.p2p.builder.PutBuilder;
import net.tomp2p.p2p.builder.RemoveBuilder;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.p2p.builder.SendBuilder;
import net.tomp2p.p2p.builder.ShutdownBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedHashTable {
    private static final Logger logger = LoggerFactory.getLogger(DistributedHashTable.class);

    private final DistributedRouting routing;

    private final StorageRPC storeRCP;

    private final DirectDataRPC directDataRPC;

    private final QuitRPC quitRPC;

    public DistributedHashTable(DistributedRouting routing, StorageRPC storeRCP, DirectDataRPC directDataRPC,
            QuitRPC quitRPC) {
        this.routing = routing;
        this.storeRCP = storeRCP;
        this.directDataRPC = directDataRPC;
        this.quitRPC = quitRPC;
    }
    
    public FuturePut add(final AddBuilder builder) {
        final FuturePut futureDHT = new FuturePut(builder.getRequestP2PConfiguration().getMinimumResults(),
                new VotingSchemeDHT());
        builder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    final FutureRouting futureRouting = createRouting(builder, Type.REQUEST_1,
                            future.getChannelCreator());
                    futureDHT.setFutureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(final FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("adding lkey={} on {}", builder.getLocationKey(),
                                        futureRouting.getPotentialHits());
                                parallelRequests(builder.getRequestP2PConfiguration(),
                                        futureRouting.getPotentialHits(), futureDHT, false,
                                        future.getChannelCreator(), new OperationMapper<FuturePut>() {
                                            Map<PeerAddress, Collection<Number480>> rawData480 = new HashMap<PeerAddress, Collection<Number480>>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
                                                return storeRCP.add(address, builder, channelCreator);
                                            }

                                            @Override
                                            public void response(FuturePut futureDHT) {
                                                futureDHT.setStoredKeys(builder.getLocationKey(),
                                                        builder.getDomainKey(), rawData480);
                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, but we
                                                // need to check the result if we could store it.
                                                if (future.isSuccess() && future.getResponse().isOk()) {
                                                    rawData480.put(future.getRequest().getRecipient(), future
                                                            .getResponse().getKeys(0).keys());
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.setFailed("routing failed");
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.getChannelCreator());
                } else {
                    futureDHT.setFailed(future);
                }
            }
        });
        return futureDHT;
    }

    /*public FutureDHT direct(final Number160 locationKey, final ByteBuf buffer, final boolean raw,
            final RoutingConfiguration routingConfiguration, final RequestP2PConfiguration p2pConfiguration,
            final FutureCreate<FutureDHT> futureCreate, final boolean cancelOnFinish, final boolean manualCleanup,
            final FutureChannelCreator futureChannelCreator, final ConnectionReservation connectionReservation) {*/
    
    public FutureDirect direct(final SendBuilder builder) {
    
        final FutureDirect futureDHT = 
                new FutureDirect(builder.getRequestP2PConfiguration().getMinimumResults(), new VotingSchemeDHT());
        
        builder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    
                    final FutureRouting futureRouting = createRouting(builder, Type.REQUEST_1,
                            future.getChannelCreator());
                    
                    futureDHT.setFutureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                    logger.debug("storing lkey={} on {}", builder.getLocationKey(), futureRouting.getPotentialHits());
                                parallelRequests(builder.getRequestP2PConfiguration(), futureRouting.getPotentialHits(), futureDHT,
                                        builder.isCancelOnFinish(), future.getChannelCreator(),
                                        new OperationMapper<FutureDirect>() {
                                            Map<PeerAddress, ByteBuf> rawChannels = new HashMap<PeerAddress, ByteBuf>();

                                            Map<PeerAddress, Object> rawObjects = new HashMap<PeerAddress, Object>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
                                                return directDataRPC.send(address, builder, channelCreator);
                                            }

                                            @Override
                                            public void response(FutureDirect futureDHT) {
                                                if (builder.isRaw())
                                                    futureDHT.setDirectData1(rawChannels);
                                                else
                                                    futureDHT.setDirectData2(rawObjects);
                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, but we need to check the result if we could store it.
                                                if (future.isSuccess() && future.getResponse().isOk()) {
                                                    if (builder.isRaw()) {
                                                        rawChannels.put(future.getRequest().getRecipient(),
                                                                future.getResponse().getBuffer(0).buffer());
                                                    } else {
                                                        try {
                                                            rawObjects.put(future.getRequest().getRecipient(),
                                                                    future.getResponse().getBuffer(0).object());
                                                        } catch (ClassNotFoundException e) {
                                                            rawObjects.put(future.getRequest().getRecipient(), e);
                                                        } catch (IOException e) {
                                                            rawObjects.put(future.getRequest().getRecipient(), e);
                                                        }
                                                    }
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.setFailed("routing failed");
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.getChannelCreator());
                    
                } else {
                    futureDHT.setFailed(future);
                }
            }
        });

        return futureDHT;
    }

    
    public FuturePut put(final PutBuilder putBuilder) {
        final FuturePut futureDHT = new FuturePut(
                putBuilder.getRequestP2PConfiguration().getMinimumResults(), new VotingSchemeDHT());
        putBuilder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    final FutureRouting futureRouting = createRouting(putBuilder, Type.REQUEST_1,
                            future.getChannelCreator());
                    futureDHT.setFutureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(final FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("storing lkey={} on {}", putBuilder.getLocationKey(),
                                        futureRouting.getPotentialHits());

                                parallelRequests(putBuilder.getRequestP2PConfiguration(),
                                        futureRouting.getPotentialHits(), futureDHT, false,
                                        future.getChannelCreator(), new OperationMapper<FuturePut>() {

                                            Map<PeerAddress, Collection<Number480>> rawData480 = new HashMap<PeerAddress, Collection<Number480>>();

                                            @Override
                                            public FutureResponse create(final ChannelCreator channelCreator,
                                                    final PeerAddress address) {
                                                //boolean protectEntry = Utils.checkEntryProtection(putBuilder
                                                //        .getDataMap());
                                                // TODO: content protection
                                                boolean putIfAbsent = putBuilder.isPutIfAbsent();
                                                if (putIfAbsent) {
                                                    return storeRCP.putIfAbsent(address, putBuilder,
                                                            channelCreator);
                                                } else {
                                                    return storeRCP.put(address, putBuilder, channelCreator);
                                                }
                                            }

                                            @Override
                                            public void response(final FuturePut futureDHT) {
                                                futureDHT.setStoredKeys(putBuilder.getLocationKey(),
                                                        putBuilder.getDomainKey(), rawData480);
                                            }

                                            @Override
                                            public void interMediateResponse(final FutureResponse future) {
                                                // the future tells us that the communication was successful, to check
                                                // the result if we could store it.
                                                if (future.isSuccess() && future.getResponse().isOk()) {
                                                    rawData480.put(future.getRequest().getRecipient(), future
                                                            .getResponse().getKeys(0).keys());
                                                }
                                            }
                                        });
                            } else {
                                futureDHT.setFailed("routing failed");
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.getChannelCreator());
                } else {
                    futureDHT.setFailed(future);
                }
            }
        });
        return futureDHT;
    }

    /*public FutureDHT get(final Number160 locationKey, final Number160 domainKey,
            final Collection<Number160> contentKeys, final SimpleBloomFilter<Number160> keyBloomFilter,
            final SimpleBloomFilter<Number160> contentBloomFilter, final RoutingConfiguration routingConfiguration,
            final RequestP2PConfiguration p2pConfiguration, final EvaluatingSchemeDHT evaluationScheme,
            final boolean signMessage, final boolean digest, final boolean returnBloomFilter, final boolean range,
            final boolean isManualCleanup, final FutureChannelCreator futureChannelCreator,
            final ConnectionReservation connectionReservation) {*/
    
    public FutureGet get(final GetBuilder builder) {
        
        final FutureGet futureDHT = new FutureGet(builder.getRequestP2PConfiguration().getMinimumResults(), new VotingSchemeDHT());
    
        builder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    
                    final FutureRouting futureRouting = createRouting(builder, Type.REQUEST_2,
                            future.getChannelCreator());
                    
                    futureDHT.setFutureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                logger.debug("found direct hits for get: {}", futureRouting.getDirectHits());
                                // this adjust is based on results from the routing process, if we find the same data on 2
                                // peers, we want to get it from one only. Unless its digest, then we want to know exactly what is going on
                                RequestP2PConfiguration p2pConfiguration2 = builder.isDigest() || builder.isRange() ? builder.getRequestP2PConfiguration()
                                        : adjustConfiguration(builder.getRequestP2PConfiguration(), futureRouting.getDirectHitsDigest());
                                // store in direct hits
                                parallelRequests(p2pConfiguration2, builder.isRange() ? futureRouting.getPotentialHits()
                                        : futureRouting.getDirectHits(), futureDHT, true, future
                                        .getChannelCreator(), new OperationMapper<FutureGet>() {
                                    Map<PeerAddress, Map<Number480, Data>> rawData = new HashMap<PeerAddress, Map<Number480, Data>>();

                                    Map<PeerAddress, DigestResult> rawDigest = new HashMap<PeerAddress, DigestResult>();

                                    @Override
                                    public FutureResponse create(ChannelCreator channelCreator, PeerAddress address) {
                                        return storeRCP.get(address, builder, channelCreator);
                                    }

                                    @Override
                                    public void response(FutureGet futureDHT) {
                                        if (builder.isDigest()) {
                                            futureDHT.setReceivedDigest(builder.getLocationKey(), builder.getDomainKey(), rawDigest);
                                        } else {
                                            futureDHT.setReceivedData(builder.getLocationKey(), builder.getDomainKey(), rawData);
                                        }
                                    }

                                    @Override
                                    public void interMediateResponse(FutureResponse future) {
                                        // the future tells us that the communication was successful, which is ok for digest
                                        if (future.isSuccess()) {
                                            if (builder.isDigest()) {
                                                SimpleBloomFilter<Number160> sbf1 = future.getResponse()
                                                        .getBloomFilter(0);
                                                SimpleBloomFilter<Number160> sbf2 = future.getResponse()
                                                        .getBloomFilter(1);
                                                final DigestResult digest;
                                                if (sbf1 == null && sbf2 == null) {
                                                    Map<Number480, Number160> keyDigest = future.getResponse()
                                                            .getKeysMap(0).keysMap();
                                                    digest = new DigestResult(keyDigest);
                                                } else {
                                                    digest = new DigestResult(sbf1, sbf2);
                                                }

                                                rawDigest.put(future.getRequest().getRecipient(), digest);
                                            } else {
                                                rawData.put(future.getRequest().getRecipient(), future
                                                        .getResponse().getDataMap(0).dataMap());
                                            }
                                            logger.debug("set data from {}", future.getRequest().getRecipient());
                                        }
                                    }
                                });
                            } else {
                                futureDHT.setFailed("routing failed");
                            }
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.getChannelCreator());
                } else {
                    futureDHT.setFailed(future);
                }
            }
        });
        return futureDHT;
    }

    /*public FutureDHT remove(final Number160 locationKey, final Number160 domainKey,
            final Collection<Number160> contentKeys, final RoutingConfiguration routingConfiguration,
            final RequestP2PConfiguration p2pConfiguration, final boolean returnResults, final boolean signMessage,
            final boolean isManualCleanup, FutureCreate<FutureDHT> futureCreate,
            final FutureChannelCreator futureChannelCreator, final ConnectionReservation connectionReservation) {*/
    public FutureRemove remove(final RemoveBuilder builder) {
        final FutureRemove futureDHT = new FutureRemove(builder.getRequestP2PConfiguration().getMinimumResults(), new VotingSchemeDHT());

        builder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    
                    final FutureRouting futureRouting = createRouting(builder, Type.REQUEST_2,
                            future.getChannelCreator());
                    //TODO: add content keys to the routing builder
                    
                    futureDHT.setFutureRouting(futureRouting);
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting futureRouting) throws Exception {
                            if (futureRouting.isSuccess()) {
                                
                                logger.debug("found direct hits for remove: {}", futureRouting.getDirectHits());
                                
                                RequestP2PConfiguration p2pConfiguration2 = adjustConfiguration(builder.getRequestP2PConfiguration(), futureRouting.getDirectHitsDigest());
                                
                                parallelRequests(p2pConfiguration2, futureRouting.getDirectHits(), futureDHT, false,
                                        future.getChannelCreator(), new OperationMapper<FutureRemove>() {
                                            Map<PeerAddress, Map<Number480, Data>> rawDataResult = new HashMap<PeerAddress, Map<Number480, Data>>();

                                            Map<PeerAddress, Collection<Number480>> rawDataNoResult = new HashMap<PeerAddress, Collection<Number480>>();

                                            @Override
                                            public FutureResponse create(ChannelCreator channelCreator,
                                                    PeerAddress address) {
                                                return storeRCP.remove(address, builder, channelCreator);
                                            }

                                            @Override
                                            public void response(FutureRemove futureDHT) {
                                                if (builder.isReturnResults())
                                                    futureDHT
                                                            .setReceivedData(builder.getLocationKey(), builder.getDomainKey(), rawDataResult);
                                                else
                                                    futureDHT.setStoredKeys(builder.getLocationKey(), builder.getDomainKey(),
                                                            rawDataNoResult);
                                            }

                                            @Override
                                            public void interMediateResponse(FutureResponse future) {
                                                // the future tells us that the communication was successful, but we need to check the result if we could store it.
                                                if (future.isSuccess() && future.getResponse().isOk()) {
                                                    if (builder.isReturnResults()) {
                                                        rawDataResult.put(future.getRequest().getRecipient(),
                                                                future.getResponse().getDataMap(0).dataMap());
                                                    } else {
                                                        rawDataNoResult.put(future.getRequest().getRecipient(),
                                                                future.getResponse().getKeys(0).keys());
                                                    }
                                                }
                                            }
                                        });
                            } else
                                futureDHT.setFailed("routing failed");
                        }
                    });
                    futureDHT.addFutureDHTReleaseListener(future.getChannelCreator());
                } else {
                    futureDHT.setFailed(future);
                }

            }
        });
        return futureDHT;
    }

    /**
     * Send a friendly shutdown message to your close neighbors.
     * 
     * @param connectionReservation
     *            The connection reservation The routing configuration
     * @param builder
     *            All other options
     * @return future shutdown
     */
    public FutureShutdown quit(final ShutdownBuilder builder) {
        final FutureShutdown futureShutdown = new FutureShutdown();

        builder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    // content key and domain key are not important when sending a friendly shutdown
                    
                    // no need for routing, we can take the close peers from our map, in addition offer a way to add peers to notify by the end user.
                    NavigableSet<PeerAddress> closePeers = routing.peerMap().closePeers(20);
                    closePeers = builder.filter(closePeers);
                    
                    parallelRequests(builder.getRequestP2PConfiguration(),
                            closePeers, futureShutdown, false,
                            future.getChannelCreator(), new OperationMapper<FutureShutdown>() {

                                @Override
                                public FutureResponse create(final ChannelCreator channelCreator,
                                        final PeerAddress address) {
                                    return quitRPC.quit(address, builder, channelCreator);
                                }

                                @Override
                                public void response(final FutureShutdown future) {
                                    future.setDone();
                                }

                                // its fire and forget, so don't bother checking the future success
                                @Override
                                public void interMediateResponse(final FutureResponse future) {
                                    // contactedPeers can be accessed by several threads
                                    futureShutdown.report(future.getRequest().getRecipient(),
                                            future.isSuccess());
                                }
                            });
                    futureShutdown.addFutureDHTReleaseListener(future.getChannelCreator());
                } else {
                    futureShutdown.setFailed(future);
                }
            }
        });
        return futureShutdown;
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
    public static <K extends FutureDHT> K parallelRequests(final RequestP2PConfiguration p2pConfiguration,
            final NavigableSet<PeerAddress> queue, final boolean cancleOnFinish,
            final FutureChannelCreator futureChannelCreator,
            final OperationMapper<K> operation, final K futureDHT) {

        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    parallelRequests(p2pConfiguration, queue, futureDHT, cancleOnFinish,
                            future.getChannelCreator(), operation); 
                    Utils.addReleaseListener(future.getChannelCreator(), futureDHT);
                } else {
                    futureDHT.setFailed(future);
                }
            }
        });
        return futureDHT;
    }

    private static <K extends FutureDHT> void parallelRequests(RequestP2PConfiguration p2pConfiguration,
            NavigableSet<PeerAddress> queue, K future, boolean cancleOnFinish, ChannelCreator channelCreator,
            OperationMapper<K> operation) {
        if (p2pConfiguration.getMinimumResults() == 0) {
            operation.response(future);
            return;
        }
        FutureResponse[] futures = new FutureResponse[p2pConfiguration.getParallel()];
        // here we split min and pardiff, par=min+pardiff
        loopRec(queue, p2pConfiguration.getMinimumResults(), new AtomicInteger(0),
                p2pConfiguration.getMaxFailure(), p2pConfiguration.getParallelDiff(),
                new AtomicReferenceArray<FutureResponse>(futures), future, cancleOnFinish, channelCreator,
                operation);
    }

    private static <K extends FutureDHT> void loopRec(final NavigableSet<PeerAddress> queue, final int min,
            final AtomicInteger nrFailure, final int maxFailure, final int parallelDiff,
            final AtomicReferenceArray<FutureResponse> futures, final K futureDHT, final boolean cancelOnFinish,
            final ChannelCreator channelCreator, final OperationMapper<K> operation) {
        // final int parallel=min+parallelDiff;
        int active = 0;
        for (int i = 0; i < min + parallelDiff; i++) {
            if (futures.get(i) == null) {
                PeerAddress next = queue.pollFirst();
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
                for (FutureResponse futureResponse : future.getCompleted()) {
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
                    loopRec(queue, min - future.getSuccessCounter(), nrFailure, maxFailure, parallelDiff, futures,
                            futureDHT, cancelOnFinish, channelCreator, operation);
                }
            }
        });
    }

    private FutureRouting createRouting(BasicBuilder<?> builder, Type type, ChannelCreator channelCreator) {
        
        //Number160 locationKey, Number160 domainKey,
        //Collection<Number160> contentKeys, RoutingConfiguration routingConfiguration,
        //RequestP2PConfiguration p2pConfiguration
        
        RoutingBuilder routingBuilder = builder.createBuilder(builder.getRequestP2PConfiguration(), builder.getRoutingConfiguration());
        routingBuilder.setLocationKey(builder.getLocationKey());
        routingBuilder.setDomainKey(builder.getDomainKey());
        /*if(contentKeys.size() == 1) {
            routingBuilder.setContentKey(contentKeys.iterator().next());
        } else if(contentKeys.size() > 1) {
            DefaultBloomfilterFactory factory = new DefaultBloomfilterFactory();
            SimpleBloomFilter<Number160> bf = factory.createContentKeyBloomFilter();
            for(Number160 contentKey: contentKeys) {
                bf.add(contentKey);
            }
            routingBuilder.setKeyBloomFilter(bf);
        }*/
        return routing.route(routingBuilder, type, channelCreator);
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
            final SortedMap<PeerAddress, DigestInfo> directHitsDigest) {

        int size = directHitsDigest.size();
        int requested = p2pConfiguration.getMinimumResults();
        if (size >= requested) {
            return p2pConfiguration;
        } else {
            return new RequestP2PConfiguration(size, p2pConfiguration.getMaxFailure(),
                    p2pConfiguration.getParallelDiff());
        }
    }
}
