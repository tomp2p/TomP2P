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
package net.tomp2p.tracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.DistributedRouting;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedTracker {
    final private static Logger logger = LoggerFactory.getLogger(DistributedTracker.class);

    // in theory if you have a perfect distribution then you can have up to
    // (direct_hits*max_trackers)+max_trackers^4 entries, which can be used. For
    // 30 as max_trackers and 5 direct hits, this results in (30*5)+30^4=810150.
    // 810150 is enough for a tracker I think.
    // final private static int MAX_FULL_TRACKERS = 4;

    final private DistributedRouting routing;

    final private PeerBean peerBean;

    final private TrackerRPC trackerRPC;

    final private PeerExchangeRPC peerExchangeRPC;

    final private Random rnd;

    final private Number160 stableRandom;

    public DistributedTracker(PeerBean peerBean, DistributedRouting routing, TrackerRPC trackerRPC,
            PeerExchangeRPC peerExchangeRPC) {
        this.routing = routing;
        this.trackerRPC = trackerRPC;
        this.peerExchangeRPC = peerExchangeRPC;
        this.peerBean = peerBean;
        this.rnd = new Random(peerBean.serverPeerAddress().peerId().hashCode());
        this.stableRandom = new Number160(rnd);

    }

   /* public FutureTracker getFromTracker(final Number160 locationKey, final Number160 domainKey,
            final RoutingConfiguration routingConfiguration, final TrackerConfiguration trackerConfiguration,
            final boolean expectAttachement, EvaluatingSchemeTracker evaluatingScheme, final boolean signMessage,
            final boolean useSecondaryTrackers, final Set<Number160> knownPeers,
            final FutureChannelCreator futureChannelCreator, final ConnectionReservation connectionReservation) {*/
    
    public FutureTracker get(final GetTrackerBuilder builder) {
    
        final FutureTracker futureTracker = new FutureTracker(builder.getEvaluatingScheme(), builder.getKnownPeers());
        builder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator futureChannelCreator2) throws Exception {
                if (futureChannelCreator2.isSuccess()) {
                    if (builder.isUseSecondaryTrackers()) {
                        TrackerData meshPeers = peerBean.trackerStorage().meshPeers(builder.getLocationKey(),
                                builder.getDomainKey());
                        TrackerData secondaryPeers = peerBean.trackerStorage().secondaryPeers(
                                builder.getLocationKey(), builder.getDomainKey());
                        NavigableSet<PeerAddress> secondaryQueue = new TreeSet<PeerAddress>(PeerMap
                                .createComparator(stableRandom));
                        for (PeerAddress peerAddress : meshPeers.peerAddresses().keySet()) {
                            secondaryQueue.add(peerAddress);
                        }
                        for (PeerAddress peerAddress : secondaryPeers.peerAddresses().keySet()) {
                            secondaryQueue.add(peerAddress);
                        }
                        startLoop(builder, futureTracker, secondaryQueue, futureChannelCreator2.channelCreator());
                    } else {
                        
                        final FutureRouting futureRouting = createRouting(builder, Type.REQUEST_3,
                                futureChannelCreator2.channelCreator());
                        
                        // final Number160 searchCloseTo=new
                        // Number160(rnd);
                        futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                            @Override
                            public void operationComplete(FutureRouting future) throws Exception {
                                if (futureRouting.isSuccess()) {
                                    if (logger.isDebugEnabled())
                                        logger.debug("found direct hits for tracker get: "
                                                + futureRouting.directHits());
                                    startLoop(builder, futureTracker, futureRouting.directHits(),
                                            futureChannelCreator2.channelCreator());
                                } else {
                                    futureTracker.failed("routing failed");
                                }
                            }

                        });
                    }
                    Utils.addReleaseListener(futureChannelCreator2.channelCreator(), futureTracker);
                } else {
                    futureTracker.failed(futureChannelCreator2);
                }
            }
        });
        return futureTracker;
    }

    private void startLoop(final GetTrackerBuilder builder, final FutureTracker futureTracker,
            final NavigableSet<PeerAddress> queueToAsk, final ChannelCreator cc) {
        loop(builder.getLocationKey(), builder.getDomainKey(), queueToAsk, builder.getTrackerConfiguration(), futureTracker, true, builder.getKnownPeers(),
                new Operation() {
                    @Override
                    public FutureResponse create(PeerAddress remotePeer, boolean primary) {
                        if (logger.isDebugEnabled())
                            logger.debug("tracker get: " + remotePeer + " location=" + builder.getLocationKey() + " ");
                        FutureResponse futureResponse = trackerRPC.getFromTracker(remotePeer, builder, cc);
                        if (logger.isDebugEnabled()) {
                            futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                                @Override
                                public void operationComplete(FutureResponse future) throws Exception {
                                    if (future.isSuccess()) {
                                        logger.debug("found the following peers: "
                                                + future.emptyResponse().trackerData(0));
                                    }
                                    else {
                                        logger.debug("failed to find peers: " + future.failedReason());
                                    }
                                }
                            });
                        }
                        return futureResponse;
                    }
                });
    }

    public FutureLateJoin<FutureResponse> startPeerExchange(final Number160 locationKey, final Number160 domainKey,
            final FutureChannelCreator futureChannelCreator, final ConnectionConfiguration configuration) {
        final TrackerData activePeers = peerBean.trackerStorage().activePeers(locationKey, domainKey);
        // TODO: make a limitrandom here in case we have many activepeers, below
        // is a sketch
        final TrackerData activePeers2 = Utils.limitRandom(activePeers, TrackerStorage.TRACKER_SIZE);
        final FutureLateJoin<FutureResponse> futureLateJoin = new FutureLateJoin<FutureResponse>(activePeers2.size());
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    for (PeerAddress peerAddress : activePeers2.peerAddresses().keySet()) {
                        FutureResponse futureResponses = peerExchangeRPC.peerExchange(peerAddress,
                                locationKey, domainKey, false, future.channelCreator(), configuration);
                        if (!futureLateJoin.add(futureResponses)) {
                            // the late join future is fininshed if the
                            // add returns false
                            break;
                        }
                    }
                    Utils.addReleaseListener(future.channelCreator(), futureLateJoin);
                } else {
                    futureLateJoin.failed(future);
                }
            }
        });
        return futureLateJoin;
    }

    /*public FutureTracker addToTracker(final Number160 locationKey, final Number160 domainKey, final byte[] attachment,
            final RoutingConfiguration routingConfiguration, final TrackerConfiguration trackerConfiguration,
            final boolean signMessage, final FutureCreate<BaseFuture> futureCreate, final Set<Number160> knownPeers,
            final FutureChannelCreator futureChannelCreator) {*/
    
    public FutureTracker add(final AddTrackerBuilder builder) {
    
        final FutureTracker futureTracker = new FutureTracker();
        builder.getFutureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator futureChannelCreator2) throws Exception {
                if (futureChannelCreator2.isSuccess()) {
                    
                    final FutureRouting futureRouting = createRouting(builder, Type.REQUEST_1,
                            futureChannelCreator2.channelCreator());
                    
                    futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
                        @Override
                        public void operationComplete(FutureRouting future) throws Exception {
                            if (futureRouting.isSuccess()) {
                                if (logger.isDebugEnabled())
                                    logger.debug("found potential hits for tracker add: "
                                            + futureRouting.potentialHits());
                                loop(builder.getLocationKey(), builder.getDomainKey(), futureRouting.potentialHits(), builder.getTrackerConfiguration(),
                                        futureTracker, false, builder.getKnownPeers(), new Operation() {
                                            @Override
                                            public FutureResponse create(PeerAddress remotePeer, boolean primary) {
                                                logger.debug("tracker add (me={}): {} location={}", peerBean.serverPeerAddress(), remotePeer, builder.getLocationKey());
                                                return trackerRPC.addToTracker(remotePeer, builder, futureChannelCreator2.channelCreator());
                                            }
                                        });
                            } else {
                                futureTracker.failed("routing failed");
                            }
                        }
                    });
                    Utils.addReleaseListener(futureChannelCreator2.channelCreator(), futureTracker);
                } else {
                    futureTracker.failed(futureChannelCreator2);
                }

            }
        });
        return futureTracker;
    }

    private void loop(Number160 locationKey, final Number160 domainKey, NavigableSet<PeerAddress> queueToAsk,
            TrackerConfiguration trackerConfiguration, FutureTracker futureTracker, boolean isGet,
            final Set<Number160> knownPeers, Operation operation) {
        FutureResponse[] futureResponses = new FutureResponse[trackerConfiguration.parallel()];
        NavigableSet<PeerAddress> secondaryQueue = new TreeSet<PeerAddress>(PeerMap.createComparator(
                stableRandom));
        loopRec(locationKey, domainKey, queueToAsk, secondaryQueue, new HashSet<PeerAddress>(),
                new HashSet<PeerAddress>(), new HashMap<PeerAddress,TrackerData>(), operation,
                trackerConfiguration.parallel(), new AtomicInteger(0), trackerConfiguration.maxFailure(),
                new AtomicInteger(0), trackerConfiguration.maxFullTrackers(), new AtomicInteger(0),
                trackerConfiguration.atLeastSucessfulRequestes(),
                trackerConfiguration.atLeastEntriesFromTrackers(), new AtomicInteger(0),
                trackerConfiguration.maxPrimaryTrackers(),
                new AtomicReferenceArray<FutureResponse>(futureResponses), futureTracker, knownPeers, isGet);
    }

    private void loopRec(final Number160 locationKey, final Number160 domainKey,
            final NavigableSet<PeerAddress> queueToAsk, final NavigableSet<PeerAddress> secondaryQueue,
            final Set<PeerAddress> alreadyAsked, final Set<PeerAddress> successAsked,
            final Map<PeerAddress, TrackerData> peerOnTracker, final Operation operation,
            final int parallel, final AtomicInteger nrFailures, final int maxFailures, final AtomicInteger trackerFull,
            final int maxTrackerFull, final AtomicInteger successfulRequests, final int atLeastSuccessfullRequests,
            final int atLeastEntriesFromTrackers, final AtomicInteger primaryTracker, final int maxPrimaryTracker,
            final AtomicReferenceArray<FutureResponse> futureResponses, final FutureTracker futureTracker,
            final Set<Number160> knownPeers, final boolean isGet) {
        // if its a get, we cancel connections, because we have what we want.
        // For a put, we want to store on many peers, thus there we don't
        // cancel.
        final boolean cancelOnFinish = isGet;
        logger.debug("we can ask {} primary, and {} secondary.", queueToAsk.size(), secondaryQueue.size());
        int active = 0;
        for (int i = 0; i < parallel; i++) {
            if (futureResponses.get(i) == null) {
                // TODO: make this more smart
                boolean primary = true;
                PeerAddress next = null;
                if (primaryTracker.incrementAndGet() <= maxPrimaryTracker) {
                    if (isGet) {
                        next = Utils.pollRandom(queueToAsk, rnd);
                    } else {
                        next = queueToAsk.pollFirst();
                    }
                }
                if (next == null) {
                    if (isGet) {
                        next = Utils.pollRandom(secondaryQueue, rnd);
                    } else {
                        next = secondaryQueue.pollFirst();
                    }
                    primary = false;
                }
                if (next != null) {
                    logger.debug("we are about to ask {}", next);
                    alreadyAsked.add(next);
                    active++;
                    futureResponses.set(i, operation.create(next, primary));
                }
            } else if (futureResponses.get(i) != null) {
                active++;
            }
        }
        if (active == 0) {
            logger.debug("we finished1, we asked {}, but we could ask {} more nodes {}", alreadyAsked.size(), queueToAsk.size(), alreadyAsked);
            queueToAsk.addAll(secondaryQueue);
            futureTracker.trackers(queueToAsk, successAsked, peerOnTracker);
            if(cancelOnFinish) {
                cancel(futureResponses);
            }
            return;
        }
        FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(1, false, futureResponses);
        fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
            @Override
            public void operationComplete(final FutureForkJoin<FutureResponse> future) throws Exception {
                boolean finished = false;
                FutureResponse futureResponse = future.last();
                // success if we could add the tracker, but also if the tracker
                // is full and sent a denied message
                boolean isFull = futureResponse != null && futureResponse.emptyResponse() != null
                        && futureResponse.emptyResponse().type() == Type.DENIED;
                boolean isPartial = futureResponse != null && futureResponse.emptyResponse() != null
                        && futureResponse.emptyResponse().type() == Type.PARTIALLY_OK;
                if (future.isSuccess() || isFull) {
                    if (!isFull) {
                        successAsked.add(futureResponse.request().recipient());
                    }
                    TrackerData newDataMap = futureResponse.emptyResponse().trackerData(0);
                    if(newDataMap !=null) {
                    	Collection<PeerAddress> newPeers = newDataMap.peerAddresses().keySet();
                    	mergeDiff(secondaryQueue, newPeers, alreadyAsked, queueToAsk);
                    	storeResult(peerOnTracker, newDataMap, futureResponse.request().recipient(), knownPeers);
                    }
                    int successRequests = isFull ? successfulRequests.get() : successfulRequests.incrementAndGet();
                    finished = evaluate(peerOnTracker, successRequests, atLeastSuccessfullRequests,
                            atLeastEntriesFromTrackers, isGet);
                    if (logger.isDebugEnabled())
                        logger.debug("evaluation result: finished=" + finished + ", " + peerOnTracker.size() + " / "
                                + atLeastEntriesFromTrackers);
                    // if peer reported that he can provide more data, we keep
                    // the peer in the list
                    if (!finished && isPartial && TrackerRPC.isPrimary(futureResponse)) {
                        if (logger.isDebugEnabled())
                            logger.debug("partial1: " + futureResponse.request().recipient());
                        queueToAsk.add(futureResponse.request().recipient());
                    }
                    if (!finished && isPartial && TrackerRPC.isSecondary(futureResponse)) {
                        if (logger.isDebugEnabled())
                            logger.debug("partial2: " + futureResponse.request().recipient());
                        secondaryQueue.add(futureResponse.request().recipient());
                    }
                    if (!finished && isFull) {
                        if (logger.isDebugEnabled())
                            logger.debug("tracker reported to be full. Check if finished due to full trackers.");
                        finished = trackerFull.incrementAndGet() >= maxTrackerFull;
                    }
                } else {
                    logger.debug("no success {}", future.failedReason());
                    finished = nrFailures.incrementAndGet() > maxFailures;
                }
                // check if done, or continune looping
                if (finished) {
                    Set<PeerAddress> potentialTrackers = new HashSet<PeerAddress>(queueToAsk);
                    potentialTrackers.addAll(secondaryQueue);
                    if (logger.isDebugEnabled()) {
                        logger.debug("we finished2, we asked " + alreadyAsked.size() + ", but we could ask "
                                + queueToAsk.size() + " more nodes (" + successfulRequests + "/"
                                + atLeastSuccessfullRequests + ")");
                    }
                    futureTracker.trackers(potentialTrackers, successAsked, peerOnTracker);
                    if(cancelOnFinish) {
                        cancel(futureResponses);
                    }
                } else {
                    loopRec(locationKey, domainKey, queueToAsk, secondaryQueue, alreadyAsked, successAsked,
                            peerOnTracker, operation, parallel, nrFailures, maxFailures, trackerFull, maxTrackerFull,
                            successfulRequests, atLeastSuccessfullRequests, atLeastEntriesFromTrackers, primaryTracker,
                            maxPrimaryTracker, futureResponses, futureTracker, knownPeers, isGet);
                }
            }
        });
    }

    private boolean evaluate(Map<?, ?> peerOnTracker, int successfulRequests, int atLeastSuccessfulRequests,
            int atLeastEntriesFromTrackers, boolean isGet) {
        if (isGet)
            return successfulRequests >= atLeastSuccessfulRequests
                    || peerOnTracker.size() >= atLeastEntriesFromTrackers;
        else
            return successfulRequests >= atLeastSuccessfulRequests;
    }

    /*private FutureRouting createRouting(Number160 locationKey, Number160 domainKey, Set<Number160> contentKeys,
            RoutingConfiguration routingConfiguration, Type type, final ChannelCreator channelCreator) {
        return routing.route(new RoutingBuilder(locationKey, domainKey, contentKeys, routingConfiguration.getDirectHits(), routingConfiguration.getMaxNoNewInfo(0), routingConfiguration.getMaxFailures(), routingConfiguration.getMaxSuccess(), routingConfiguration.getParallel(),
                routingConfiguration.isForceTCP()), type, channelCreator);
    }*/
    
    private FutureRouting createRouting(TrackerBuilder<?> builder, Type type, ChannelCreator channelCreator) {
        RoutingBuilder routingBuilder = builder.createBuilder(builder.getRoutingConfiguration());
        routingBuilder.locationKey(builder.getLocationKey());
        routingBuilder.domainKey(builder.getDomainKey());
        routingBuilder.peerFilters(builder.peerFilters());
        return routing.route(routingBuilder, type, channelCreator);
    }

    /**
     * Stores the data we found on the tracker. The future stores there raw data, and the user can evaluate it in the
     * future class.
     * 
     * @param peerOnTracker
     *            We store the results per peer. The result of each peer is stored in this map.
     * @param newDataMap
     *            The new data, we got from a peer
     * @param newDataProvider
     *            The peer we got the new data from
     * @param knownPeers
     *            The list of known peers. The list of know peers will be updated for every peer with get reports from
     *            and from its entries. This set is a bloomfilter, so there is no problem with growth, but it might
     *            result in false positives.
     */
    private static void storeResult(Map<PeerAddress, TrackerData> peerOnTracker,
            TrackerData newDataMap, PeerAddress newDataProvider, Set<Number160> knownPeers) {
    	if(knownPeers != null) {
    		knownPeers.add(newDataProvider.peerId());
    	}
        peerOnTracker.put(newDataProvider, newDataMap);
    }

    /**
     * Filters the collection of new peers from already known peers. The result of this operation is stored in
     * queueToAsk, which will be queried next.
     * 
     * @param queueToAsk
     *            The queue where we store new peers we found
     * @param newPeers
     *            New peers that were sent from other peers. Since the other peers don't know what peers we know, we
     *            must filter this collection.
     * @param knownPeers1
     *            Those peer we have already asked or are already in the queue
     * @param knownPeers2
     *            Those peer we have already asked or are already in the queue
     * @return True, if new information has been added to queueToAsk
     */
    private static boolean mergeDiff(Set<PeerAddress> queueToAsk, Collection<PeerAddress> newPeers,
            Collection<PeerAddress> knownPeers1, Collection<PeerAddress> knownPeers2) {
        // result will be small, so we chose an array list.
        final Collection<PeerAddress> result = Utils.difference(newPeers, new ArrayList<PeerAddress>(), knownPeers1,
                knownPeers2);
        // if result contains only elements that queueToAsk already has, false
        // will be returned.
        return queueToAsk.addAll(result);
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
     * This interface is used for the RPC operations. It creates the calls to other peers
     */
    public interface Operation {
        /**
         * Creates an RPC. For the distributed thracker, this is either addToTracker or getFromTracker
         * 
         * @param address
         *            The address of the remote peer
         * @param primary
         *            If the remote peer is a primary tracker, the flag is set to true
         * @return The future reponse of the RPC
         */
        FutureResponse create(PeerAddress address, boolean primary);
    }
}
