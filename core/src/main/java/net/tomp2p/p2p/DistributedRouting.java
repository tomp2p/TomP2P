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
import java.util.NavigableSet;
import java.util.Random;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureWrapper;
import net.tomp2p.message.Message2;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.NeighborRPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles routing of nodes to other nodes.
 * 
 * TODO: add timing constraints for the routing. This would allow for slow routing requests to have a chance to report
 * the neighbors.
 * 
 * @author Thomas Bocek
 */
public class DistributedRouting {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedRouting.class);

    private final NeighborRPC neighbors;

    private final PeerBean peerBean;

    private final Random rnd;

    /**
     * The routing process involves multiple RPCs, mostly UDP based.
     * 
     * @param peerBean
     *            The peer bean
     * @param neighbors
     *            The neighbor RPC that will be issues
     */
    public DistributedRouting(final PeerBean peerBean, final NeighborRPC neighbors) {
        this.neighbors = neighbors;
        this.peerBean = peerBean;
        // stable random number. No need to be truly random
        rnd = new Random(peerBean.serverPeerAddress().getPeerId().hashCode());
    }

    /**
     * Bootstraps to the given peerAddresses, i.e. looking for near nodes
     * 
     * @param peerAddresses
     *            the node to which bootstrap should be performed to
     * @param routingBuilder
     *            All relevant information for the routing process
     * @param cc
     *            The channel creator
     * @return a FutureRouting object, is set to complete if the route has been found
     */
    public FutureWrapper<FutureRouting> bootstrap(final Collection<PeerAddress> peerAddresses,
            final RoutingBuilder routingBuilder, final ChannelCreator cc) {
        // search close peers
        LOG.debug("broadcast to {}", peerAddresses);

        // first we find close peers to us
        routingBuilder.setBootstrap(true);

        final FutureRouting futureRouting = routing(peerAddresses, routingBuilder, Type.REQUEST_1, cc);
        final FutureWrapper<FutureRouting> futureWrapper = new FutureWrapper<FutureRouting>();
        // to not become a Fachidiot (expert idiot), we need to know other peers
        // as well. This is important if this peer is passive and only replies on requests from other peers
        futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
            @Override
            public void operationComplete(final FutureRouting future) throws Exception {
                // setting this to null causes to search for a random number
                routingBuilder.setLocationKey(null);
                FutureRouting futureRouting = routing(peerAddresses, routingBuilder, Type.REQUEST_1, cc);
                futureWrapper.waitFor(futureRouting);
            }
        });
        return futureWrapper;
    }

    /**
     * Looks for a route to the given locationKey.
     * 
     * @param routingBuilder
     *            All relevant information for the routing process
     * @param type
     *            The type of the routing, there can at most four types
     * @param cc
     *            The channel creator
     * 
     * @return a FutureRouting object, is set to complete if the route has been found
     */
    public FutureRouting route(final RoutingBuilder routingBuilder, final Type type, final ChannelCreator cc) {
        // for bad distribution, use large NO_NEW_INFORMATION
        Collection<PeerAddress> startPeers = peerBean.peerMap().closePeers(routingBuilder.getLocationKey(),
                routingBuilder.getParallel() * 2);
        return routing(startPeers, routingBuilder, type, cc);
    }

    /**
     * Looks for a route to the given locationKey.
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
    private FutureRouting routing(final Collection<PeerAddress> peerAddresses,
            final RoutingBuilder routingBuilder, final Type type, final ChannelCreator cc) {
        if (peerAddresses == null) {
            throw new IllegalArgumentException("you need to specify some nodes");
        }
        boolean randomSearch = routingBuilder.getLocationKey() == null;
        
        final FutureRouting futureRouting = new FutureRouting();
        //
        final Comparator<PeerAddress> comparator;
        if (randomSearch) {
            comparator = peerBean.peerMap().createComparator();
        } else {
            comparator = PeerMap.createComparator(routingBuilder.getLocationKey());
        }
        final NavigableSet<PeerAddress> queueToAsk = new TreeSet<PeerAddress>(comparator);
        // we can reuse the comparator
        final SortedSet<PeerAddress> alreadyAsked = new TreeSet<PeerAddress>(comparator);
        // as presented by Kazuyuki Shudo at AIMS 2009, its better to ask random
        // peers with the data than ask peers that ar ordered by distance ->
        // this balances load.
        final SortedMap<PeerAddress, DigestInfo> directHits = new TreeMap<PeerAddress, DigestInfo>(peerBean
                .peerMap().createComparator());
        final NavigableSet<PeerAddress> potentialHits = new TreeSet<PeerAddress>(comparator);
        // fill initially
        queueToAsk.addAll(peerAddresses);
        alreadyAsked.add(peerBean.serverPeerAddress());
        potentialHits.add(peerBean.serverPeerAddress());
        // domainkey can be null if we bootstrap
        if (type == Type.REQUEST_2 && routingBuilder.getDomainKey() != null && !randomSearch) {
            DigestInfo digestBean = peerBean.storage().digest(routingBuilder.getLocationKey(),
                    routingBuilder.getDomainKey(), routingBuilder.getContentKey());
            if (digestBean.getSize() > 0) {
                directHits.put(peerBean.serverPeerAddress(), digestBean);
            }
        } else if (type == Type.REQUEST_3 && !randomSearch) {
            DigestInfo digestInfo = peerBean.trackerStorage().digest(routingBuilder.getLocationKey(),
                    routingBuilder.getDomainKey(), routingBuilder.getContentKey());
            // we always put ourselfs to the tracker list, so we need to check
            // if we know also other peers on our trackers.
            if (digestInfo.getSize() > 0) {
                directHits.put(peerBean.serverPeerAddress(), digestInfo);
            }
        }
        // with request4 we should never see random search, but just to be very
        // specific here add the flag
        // TODO: no task manager in the core
        // else if (type == Type.REQUEST_4 && !randomSearch) {
        // DigestInfo digestInfo = peerBean.taskManager().digest();
        // if (digestInfo.getSize() > 0) {
        // directHits.put(peerBean.serverPeerAddress(), digestInfo);
        // }
        // }
        // peerAddresses is typically only 0 for routing. However, the user may
        // bootstrap with an empty List<PeerAddress>, which will then also be 0.
        if (peerAddresses.size() == 0) {
            futureRouting.setNeighbors(directHits, potentialHits, alreadyAsked, routingBuilder.isBootstrap(),
                    false);
        } else {
            // if a peer bootstraps to itself, then the size of peerAddresses
            // is 1 and it contains itself. Check for that because we need to
            // know if we are routing, bootstrapping and bootstrapping to
            // ourselfs, to return the correct status for the future
            boolean isRoutingOnlyToSelf = (peerAddresses.size() == 1 && peerAddresses.iterator().next()
                    .equals(peerBean.serverPeerAddress()));
            
            RoutingMechanism routingMechanism = routingBuilder.createRoutingMechanism(futureRouting);
                 
            routingMechanism.queueToAsk(queueToAsk);
            routingMechanism.potentialHits(potentialHits);
            routingMechanism.directHits(directHits);
            routingMechanism.alreadyAsked(alreadyAsked);
            
            routingBuilder.routingOnlyToSelf(isRoutingOnlyToSelf);
            routingRec(routingBuilder, routingMechanism, type, cc);
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

    private void routingRec(final RoutingBuilder routingBuilder, final RoutingMechanism routingMechanism,
            final Type type, final ChannelCreator channelCreator) {

        final boolean randomSearch = routingBuilder.getLocationKey() == null;
        int active = 0;
        for (int i = 0; i < routingMechanism.parallel(); i++) {
            if (routingMechanism.futureResponse(i) == null && !routingMechanism.isStopCreatingNewFutures()) {
                final PeerAddress next;
                if (randomSearch) {
                    next = routingMechanism.pollRandomInQueueToAsk(rnd);
                } else {
                    next = routingMechanism.pollFirstInQueueToAsk();
                }
                if (next != null) {
                    routingMechanism.addToAlreadyAsked(next);
                    active++;
                    // if we search for a random peer, then the peer should
                    // return the address farest away.
                    final Number160 locationKey2 = randomSearch ? next.getPeerId().xor(Number160.MAX_VALUE)
                            : routingBuilder.getLocationKey();
                    routingBuilder.setLocationKey(locationKey2);
                    routingMechanism.futureResponse(i, neighbors.closeNeighbors(next,
                            routingBuilder.searchValues(), type, channelCreator, routingBuilder));
                    LOG.debug("get close neighbors: {} on {}", next, i);
                }
            } else if (routingMechanism.futureResponse(i) != null) {
                LOG.debug("activity on {}", i);
                active++;
            }
        }
        if (active == 0) {
            LOG.debug("no activity, closing");
            
            routingMechanism.setNeighbors(routingBuilder);
            routingMechanism.cancel();
            return;
        }
        final boolean last = active == 1;
        final FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(1, false,
                routingMechanism.futureResponses());
        fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
            @Override
            public void operationComplete(final FutureForkJoin<FutureResponse> future) throws Exception {
                final boolean finished;
                if (future.isSuccess()) {
                    Message2 lastResponse = future.getLast().getResponse();
                    PeerAddress remotePeer = lastResponse.getSender();
                    routingMechanism.addPotentialHits(remotePeer);
                    Collection<PeerAddress> newNeighbors = lastResponse.getNeighborsSet(0).neighbors();
                    
                    Integer resultSize = lastResponse.getInteger(0);
                    Number160 keyDigest = lastResponse.getKey(0);
                    Number160 contentDigest = lastResponse.getKey(1);
                    DigestInfo digestBean = new DigestInfo(keyDigest, contentDigest, resultSize == null? 0 : resultSize);
                    LOG.debug("Peer ({}) {} reported {}", (digestBean.getSize() > 0 ? "direct" : "none"), remotePeer,
                            newNeighbors);
                    finished = routingMechanism.evaluateSuccess(remotePeer, digestBean, newNeighbors, last);
                    LOG.debug("Routing finished {} / {}", finished,
                            routingMechanism.isStopCreatingNewFutures());
                } else {
                    // if it failed but the failed is the closest one, its good to try again, since the peer might just
                    // be busy
                    LOG.warn("routing error {}", future.getFailedReason());
                    finished = routingMechanism.evaluateFailed();
                    routingMechanism.stopCreatingNewFutures(finished);
                }
                if (finished) {
                    LOG.debug("finished routing, direct hits: {} potential: {}",
                            routingMechanism.directHits(), routingMechanism.potentialHits());
                    
                    routingMechanism.setNeighbors(routingBuilder);
                    routingMechanism.cancel();
                    // stop all operations, as we are finished, no need to go further
                } else {
                    
                    routingRec(routingBuilder, routingMechanism, type, channelCreator);
                }
            }
        });
    }
    
    public PeerMap peerMap() {
        return peerBean.peerMap();
    }

    /**
     * Cancel the future that causes the underlying futures to cancel as well.
     * 
     * @param futureResponses
     *            The array with the future responses some items may be null
     */
    /*public static void cancel(final AtomicReferenceArray<? extends BaseFuture> futureResponses) {
        int len = futureResponses.length();
        for (int i = 0; i < len; i++) {
            BaseFuture baseFuture = futureResponses.get(i);
            if (baseFuture != null) {
                baseFuture.cancel();
            }
        }
    }*/
}
