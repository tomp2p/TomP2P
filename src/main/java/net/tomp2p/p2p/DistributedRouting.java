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
package net.tomp2p.p2p;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureWrapper;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles routing of nodes to other nodes
 * 
 * @author Thomas Bocek
 */
public class DistributedRouting {
    final private static Logger logger = LoggerFactory.getLogger(DistributedRouting.class);

    final private NeighborRPC neighbors;

    final private PeerBean peerBean;

    final private Random rnd;

    public DistributedRouting(PeerBean peerBean, NeighborRPC neighbors) {
        this.neighbors = neighbors;
        this.peerBean = peerBean;
        rnd = new Random(peerBean.getServerPeerAddress().getPeerId().hashCode());
    }

    /**
     * Bootstraps to the given peerAddresses, i.e. looking for near nodes
     * 
     * @param peerAddresses
     *            the node to which bootstrap should be performed to
     * @param maxNoNewInfo
     *            number of nodes asked without new information to stop at
     * @param maxFailures
     *            number of failures to stop at
     * @param parallel
     *            number of routing requests performed concurrently
     * @return a FutureRouting object, is set to complete if the route has been found
     */
    public FutureWrapper<FutureRouting> bootstrap(final Collection<PeerAddress> peerAddresses, final int maxNoNewInfo,
            final int maxFailures, final int maxSuccess, final int parallel, final boolean forceTCP,
            final boolean isForceRoutingOnlyToSelf, final ChannelCreator cc) {
        // search close peers
        if (logger.isDebugEnabled()) {
            logger.debug("broadcast to " + peerAddresses);
        }
        final FutureWrapper<FutureRouting> futureWrapper = new FutureWrapper<FutureRouting>();
        // first we find close peers to us
        FutureRouting futureRouting = routing(peerAddresses, peerBean.getServerPeerAddress().getPeerId(), null, null,
                0, maxNoNewInfo, maxFailures, maxSuccess, parallel, Type.REQUEST_1, forceTCP, cc, true,
                isForceRoutingOnlyToSelf);
        // to not become a Fachidiot (expert idiot), we need to know other peers
        // as well
        futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
            @Override
            public void operationComplete(FutureRouting future) throws Exception {
                // don't care if suceeded or not, contact other peers
                FutureRouting futureRouting = routing(peerAddresses, null, null, null, 0, maxNoNewInfo, maxFailures,
                        maxSuccess, parallel, Type.REQUEST_1, forceTCP, cc, true, isForceRoutingOnlyToSelf);
                futureWrapper.waitFor(futureRouting);
            }
        });
        return futureWrapper;
    }

    /**
     * Looks for a route to the given locationKey
     * 
     * @param locationKey
     *            the node a route should be found to
     * @param domainKey
     *            the domain of the network the current node and locationKey is in
     * @param contentKeys
     *            keys of the content to search for. Only used if you perform a get
     * @param maxDirectHits
     *            number of direct hits to stop at
     * @param maxNoNewInfo
     *            number of nodes asked without new information to stop at
     * @param maxFailures
     *            number of failures to stop at
     * @param parallel
     *            number of routing requests performed concurrently
     * @param isDigest
     *            Set to true to return a digest of the remote content for close neighbors
     * @param forceTCP
     *            Set to true if routing should use TCP connections
     * @param cc
     *            the channel creator
     * @return a FutureRouting object, is set to complete if the route has been found
     */
    public FutureRouting route(final Number160 locationKey, final Number160 domainKey,
            final Collection<Number160> contentKeys, Type type, int maxDirectHits, int maxNoNewInfo, int maxFailures,
            int maxSuccess, int parallel, boolean forceTCP, final ChannelCreator cc) {
        // for bad distribution, use large NO_NEW_INFORMATION
        Collection<PeerAddress> startPeers = peerBean.getPeerMap().closePeers(locationKey, parallel * 2);
        return routing(startPeers, locationKey, domainKey, contentKeys, maxDirectHits, maxNoNewInfo, maxFailures,
                maxSuccess, parallel, type, forceTCP, cc, false, false);
    }

    /**
     * Looks for a route to the given locationKey
     * 
     * @param peerAddresses
     *            nodes which should be asked first for a route
     * @param locationKey
     *            the node a route should be found to
     * @param domainKey
     *            the domain of the network the current node and locationKey is in
     * @param contentKeys
     *            nodes which we got from another node
     * @param maxDirectHits
     *            number of direct hits to stop at
     * @param maxNoNewInfo
     *            number of nodes asked without new information to stop at
     * @param maxFailures
     *            number of failures to stop at
     * @param parallel
     *            number of routing requests performed concurrently
     * @return a FutureRouting object, is set to complete if the route has been found
     */
    private FutureRouting routing(Collection<PeerAddress> peerAddresses, Number160 locationKey,
            final Number160 domainKey, final Collection<Number160> contentKeys, int maxDirectHits, int maxNoNewInfo,
            int maxFailures, int maxSuccess, final int parallel, Type type, boolean forceTCP, final ChannelCreator cc,
            final boolean isBootstrap, final boolean isForceRoutingOnlyToSelf) {
        if (peerAddresses == null)
            throw new IllegalArgumentException("you need to specify some nodes");
        boolean randomSearch = locationKey == null;
        final FutureResponse[] futureResponses = new FutureResponse[parallel];
        final FutureRouting futureRouting = new FutureRouting();
        //
        final Comparator<PeerAddress> comparator;
        if (randomSearch) {
            comparator = peerBean.getPeerMap().createPeerComparator();
        } else {
            comparator = peerBean.getPeerMap().createPeerComparator(locationKey);
        }
        final NavigableSet<PeerAddress> queueToAsk = new TreeSet<PeerAddress>(comparator);
        // we can reuse the comparator
        final SortedSet<PeerAddress> alreadyAsked = new TreeSet<PeerAddress>(comparator);
        // as presented by Kazuyuki Shudo at AIMS 2009, its better to ask random
        // peers with the data than ask peers that ar ordered by distance ->
        // this balances load.
        final SortedMap<PeerAddress, DigestInfo> directHits = new TreeMap<PeerAddress, DigestInfo>(peerBean
                .getPeerMap().createPeerComparator());
        final NavigableSet<PeerAddress> potentialHits = new TreeSet<PeerAddress>(comparator);
        // fill initially
        queueToAsk.addAll(peerAddresses);
        alreadyAsked.add(peerBean.getServerPeerAddress());
        potentialHits.add(peerBean.getServerPeerAddress());
        // domainkey can be null if we bootstrap
        if (type == Type.REQUEST_2 && domainKey != null && !randomSearch) {
            DigestInfo digestBean = peerBean.getStorage().digest(locationKey, domainKey, contentKeys);
            if (digestBean.getSize() > 0) {
                directHits.put(peerBean.getServerPeerAddress(), digestBean);
            }
        } else if (type == Type.REQUEST_3 && !randomSearch) {
            DigestInfo digestInfo = peerBean.getTrackerStorage().digest(locationKey, domainKey, contentKeys);
            // we always put ourselfs to the tracker list, so we need to check
            // if we know also other peers on our trackers.
            if (digestInfo.getSize() > 0) {
                directHits.put(peerBean.getServerPeerAddress(), digestInfo);
            }
        }
        // with request4 we should never see random search, but just to be very
        // specific here add the flag
        else if (type == Type.REQUEST_4 && !randomSearch) {
            DigestInfo digestInfo = peerBean.getTaskManager().digest();
            if (digestInfo.getSize() > 0) {
                directHits.put(peerBean.getServerPeerAddress(), digestInfo);
            }
        }
        // peerAddresses is typically only 0 for routing. However, the user may
        // bootstrap with an empty List<PeerAddress>, which will then also be 0.
        if (peerAddresses.size() == 0) {
            futureRouting.setNeighbors(directHits, potentialHits, alreadyAsked, isBootstrap, false);
        } else {
            // if a peer bootstraps to itself, then the size of peerAddresses
            // is 1 and it contains itself. Check for that because we need to
            // know if we are routing, bootstrapping and bootstrapping to
            // ourselfs, to return the correct status for the future
            boolean isRoutingOnlyToSelf = isForceRoutingOnlyToSelf
                    || (peerAddresses.size() == 1 && peerAddresses.iterator().next()
                            .equals(peerBean.getServerPeerAddress()));
            routingRec(new AtomicReferenceArray<FutureResponse>(futureResponses), futureRouting, queueToAsk,
                    alreadyAsked, directHits, potentialHits, new AtomicInteger(0), new AtomicInteger(0),
                    new AtomicInteger(0), maxDirectHits, maxNoNewInfo, maxFailures, maxSuccess, parallel, locationKey,
                    domainKey, contentKeys, true, type, forceTCP, false, cc, isBootstrap, !isRoutingOnlyToSelf);
        }
        return futureRouting;
    }

    /**
     * Looks for a route to the given locationKey, performing recursively. Since this method is not called concurrently,
     * but sequentially, no synchronization is necessary.
     * 
     * @param futureResponses
     *            expected responses
     * @param futureRouting
     *            the current routing future used
     * @param queueToAsk
     *            all nodes which should be asked for routing information
     * @param alreadyAsked
     *            nodes which already have been asked
     * @param directHits
     *            stores direct hits received
     * @param nrNoNewInfo
     *            number of nodes contacted without any new information
     * @param nrFailures
     *            number of nodes without a response
     * @param nrSucess
     *            number of peers that responded
     * @param maxDirectHits
     *            number of direct hits to stop at
     * @param maxNoNewInfo
     *            number of nodes asked without new information to stop at
     * @param maxFailures
     *            number of failures to stop at
     * @param maxSuccess
     *            number of successful requests. To avoid looping if every peer gives a new piece of information.
     * @param parallel
     *            number of routing requests performed concurrently
     * @param locationKey
     *            the node a route should be found to
     * @param domainKey
     *            the domain of the network the current node and locationKey is in
     * @param contentKeys
     *            nodes which we got from another node
     */
    private void routingRec(final AtomicReferenceArray<FutureResponse> futureResponses,
            final FutureRouting futureRouting, final NavigableSet<PeerAddress> queueToAsk,
            final SortedSet<PeerAddress> alreadyAsked, final SortedMap<PeerAddress, DigestInfo> directHits,
            final NavigableSet<PeerAddress> potentialHits, final AtomicInteger nrNoNewInfo,
            final AtomicInteger nrFailures, final AtomicInteger nrSuccess, final int maxDirectHits,
            final int maxNoNewInfo, final int maxFailures, final int maxSucess, final int parallel,
            final Number160 locationKey, final Number160 domainKey, final Collection<Number160> contentKeys,
            final boolean cancelOnFinish, final Type type, final boolean forceTCP,
            final boolean stopCreatingNewFutures, final ChannelCreator channelCreator, final boolean isBootstrap,
            final boolean isRoutingToOthers) {
        boolean randomSearch = locationKey == null;
        int active = 0;
        for (int i = 0; i < parallel; i++) {
            if (futureResponses.get(i) == null && !stopCreatingNewFutures) {
                final PeerAddress next;
                if (randomSearch) {
                    next = Utils.pollRandom(queueToAsk, rnd);
                } else {
                    next = queueToAsk.pollFirst();
                }
                if (next != null) {
                    alreadyAsked.add(next);
                    active++;
                    // if we search for a random peer, then the peer should
                    // return the address farest away.
                    final Number160 locationKey2 = randomSearch ? next.getPeerId().xor(Number160.MAX_VALUE)
                            : locationKey;
                    futureResponses.set(i, neighbors.closeNeighbors(next, locationKey2, domainKey, contentKeys, type,
                            channelCreator, forceTCP));
                    if (logger.isDebugEnabled()) {
                        logger.debug("get close neighbors: " + next);
                    }
                }
            } else if (futureResponses.get(i) != null) {
                active++;
            }
        }
        if (active == 0) {
            futureRouting.setNeighbors(directHits, potentialHits, alreadyAsked, isBootstrap, isRoutingToOthers);
            cancel(cancelOnFinish, parallel, futureResponses);
            return;
        }
        final boolean last = active == 1;
        FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(1, false, futureResponses);
        fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
            @Override
            public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception {
                boolean finished;
                boolean stopCreatingNewFutures;
                if (future.isSuccess()) {
                    Message lastResponse = future.getLast().getResponse();
                    PeerAddress remotePeer = lastResponse.getSender();
                    potentialHits.add(remotePeer);
                    Collection<PeerAddress> newNeighbors = lastResponse.getNeighbors();
                    int resultSize = lastResponse.getInteger();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Peer (" + (resultSize > 0 ? "direct" : "none") + ") " + remotePeer + " reported "
                                + newNeighbors);
                    }
                    Number160 keyDigest = lastResponse.getKeyKey1();
                    Number160 contentDigest = lastResponse.getKeyKey2();
                    Map<Number160, Number160> keyMap = lastResponse.getKeyMap();
                    DigestInfo digestBean = new DigestInfo(keyDigest, contentDigest, resultSize);
                    if (evaluateDirectHits(keyMap, remotePeer, directHits, digestBean, maxDirectHits)) {
                        // stop immediately
                        finished = true;
                        stopCreatingNewFutures = true;
                    } else if (nrSuccess.incrementAndGet() > maxSucess) {
                        // wait until pending futures are finished
                        finished = last;
                        stopCreatingNewFutures = true;
                    } else if (evaluateInformation(newNeighbors, queueToAsk, alreadyAsked, nrNoNewInfo, maxNoNewInfo)) {
                        // wait until pending futures are finished
                        finished = last;
                        stopCreatingNewFutures = true;
                    } else {
                        // continue
                        finished = false;
                        stopCreatingNewFutures = false;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Routing finished " + finished + "/" + stopCreatingNewFutures);
                    }
                } else {
                    // if it failed but the failed is the closest one, its good to try again, since the peer might just
                    // be busy

                    finished = nrFailures.incrementAndGet() > maxFailures;
                    stopCreatingNewFutures = finished;
                }
                if (finished) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("finished routing, direct hits: " + directHits + ", potential: " + potentialHits);
                    }
                    futureRouting.setNeighbors(directHits, potentialHits, alreadyAsked, isBootstrap, isRoutingToOthers);
                    cancel(cancelOnFinish, parallel, futureResponses);
                } else {
                    routingRec(futureResponses, futureRouting, queueToAsk, alreadyAsked, directHits, potentialHits,
                            nrNoNewInfo, nrFailures, nrSuccess, maxDirectHits, maxNoNewInfo, maxFailures, maxSucess,
                            parallel, locationKey, domainKey, contentKeys, cancelOnFinish, type, forceTCP,
                            stopCreatingNewFutures, channelCreator, isBootstrap, isRoutingToOthers);
                }
            }
        });
    }

    public static void cancel(boolean cancelOnFinish, int parallel,
            AtomicReferenceArray<? extends BaseFuture> futureResponses) {
        if (cancelOnFinish) {
            for (int i = 0; i < parallel; i++) {
                BaseFuture baseFuture = futureResponses.get(i);
                if (baseFuture != null)
                    baseFuture.cancel();
            }
        }
    }

    // checks if we reached the end of our search.
    static boolean evaluateDirectHits(Map<Number160, Number160> keyMap, PeerAddress remotePeer,
            final Map<PeerAddress, DigestInfo> directHits, DigestInfo digestBean, int maxDirectHits) {
        if (digestBean.getSize() > 0) {
            directHits.put(remotePeer, digestBean);
            if (directHits.size() >= maxDirectHits)
                return true;
        }
        return false;
    }

    // checks if we reached the end of our search.
    static boolean evaluateInformation(Collection<PeerAddress> newNeighbors, final SortedSet<PeerAddress> queueToAsk,
            final Set<PeerAddress> alreadyAsked, final AtomicInteger noNewInfo, int maxNoNewInfo) {
        boolean newInformation = merge(queueToAsk, newNeighbors, alreadyAsked);
        if (newInformation) {
            noNewInfo.set(0);
            return false;
        } else {
            return noNewInfo.incrementAndGet() >= maxNoNewInfo;
        }
    }

    /**
     * Updates queueToAsk with new data, returns if we found peers closer than we already know.
     * 
     * @param queueToAsk
     *            The queue to get updated
     * @param newPeers
     *            The new peers reported from remote peers. Since the remote peers do not know what we know, we need to
     *            filter this information.
     * @param alreadyAsked
     *            The peers we already know.
     * @return True if we added peers that are closer to the target than we already knew. Please note, it will return
     *         false if we add new peers that are not closer to a target.
     */
    static boolean merge(SortedSet<PeerAddress> queueToAsk, Collection<PeerAddress> newPeers,
            Collection<PeerAddress> alreadyAsked) {
        final SortedSet<PeerAddress> result = new TreeSet<PeerAddress>(queueToAsk.comparator());
        Utils.difference(newPeers, result, alreadyAsked);
        if (result.size() == 0) {
            return false;
        }
        PeerAddress first = result.first();
        boolean newInfo = isNew(queueToAsk, first);
        queueToAsk.addAll(result);
        return newInfo;
    }

    /**
     * Checks if an item is will be the highest in a sorted set.
     * 
     * @param queueToAsk
     *            The sorted set to check
     * @param first
     *            The element to check if it will be the highest in the sorted set
     * @return True, if item will be the highest element.
     */
    private static boolean isNew(SortedSet<PeerAddress> queueToAsk, PeerAddress item) {
        if (queueToAsk.contains(item)) {
            return false;
        }
        SortedSet<PeerAddress> tmp = queueToAsk.headSet(item);
        return tmp.size() == 0;
    }
}
