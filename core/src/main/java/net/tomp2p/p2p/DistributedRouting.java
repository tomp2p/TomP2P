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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.utils.Pair;

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
        rnd = new Random(peerBean.serverPeerAddress().peerId().hashCode());
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
    public FutureDone<Pair<FutureRouting,FutureRouting>> bootstrap(final Collection<PeerAddress> peerAddresses,
            final RoutingBuilder routingBuilder, final ChannelCreator cc) {
        // search close peers
        LOG.debug("Bootstrap to {}.", peerAddresses);
        final FutureDone<Pair<FutureRouting,FutureRouting>> futureDone = new FutureDone<Pair<FutureRouting,FutureRouting>>();

        // first we find close peers to us
        routingBuilder.bootstrap(true);

        final FutureRouting futureRouting0 = routing(peerMap().getPeerStatistics(peerAddresses), routingBuilder, Type.REQUEST_1, cc);
        // to not become a Fachidiot (expert idiot), we need to know other peers
        // as well. This is important if this peer is passive and only replies on requests from other peers
        futureRouting0.addListener(new BaseFutureAdapter<FutureRouting>() {
            @Override
            public void operationComplete(final FutureRouting future) throws Exception {
                // setting this to null causes to search for a random number
            	if(future.isSuccess()) {
            		routingBuilder.locationKey(null);
            		final FutureRouting futureRouting1 = routing(peerMap().getPeerStatistics(peerAddresses), routingBuilder, Type.REQUEST_1, cc);
            		futureRouting1.addListener(new BaseFutureAdapter<FutureRouting>() {
            			@Override
            			public void operationComplete(FutureRouting future) throws Exception {
            				final Pair<FutureRouting,FutureRouting> pair = new Pair<FutureRouting, FutureRouting>(futureRouting0, futureRouting1);
            				futureDone.done(pair);
            			}
            		});
            	} else {
            		futureDone.failed(future);
            	}
            }
        });
        return futureDone;
    }
    
    public FutureRouting quit(final RoutingBuilder routingBuilder, final ChannelCreator cc) {
    	Collection<PeerStatistic> startPeers = peerBean.peerMap().closePeers(routingBuilder.locationKey(),
                routingBuilder.parallel() * 2);
        return routing(startPeers, routingBuilder, Type.REQUEST_4, cc);
    }

    /**
     * Looks for a route to the location key given in the routing builder.
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
        Collection<PeerStatistic> startPeers = peerBean.peerMap().closePeers(routingBuilder.locationKey(),
                routingBuilder.parallel() * 2);
        return routing(startPeers, routingBuilder, type, cc);
    }

    /**
     * Looks for a route to the given peer address.
     *
     * @param peerAddresses
     *            nodes which should be asked first for a route
     * @return a FutureRouting object, is set to complete if the route has been found
     */
    private FutureRouting routing(final Collection<PeerStatistic> peerAddresses,
            final RoutingBuilder routingBuilder, final Type type, final ChannelCreator cc) {
        if (peerAddresses == null) {
            throw new IllegalArgumentException("Some nodes/addresses need to be specified.");
        }
        boolean randomSearch = routingBuilder.locationKey() == null;
        //
        final Comparator<PeerStatistic> statisticComparator;
        final Comparator<PeerAddress> addressComparator;
        if (randomSearch) {
            statisticComparator = peerMap().createStatisticComparator(peerMap().self());
            addressComparator = PeerMap.createXORAddressComparator(peerMap().self());
        } else {
            statisticComparator = peerMap().createStatisticComparator(routingBuilder.locationKey());
            addressComparator = PeerMap.createXORAddressComparator(routingBuilder.locationKey());
        }
        final UpdatableTreeSet<PeerStatistic> queueToAsk = new UpdatableTreeSet<PeerStatistic>(statisticComparator);
        // we can reuse the comparator
        final SortedSet<PeerAddress> alreadyAsked = new TreeSet<PeerAddress>(addressComparator);
        // as presented by Kazuyuki Shudo at AIMS 2009, its better to ask random
        // peers with the data than ask peers that ar ordered by distance ->
        // this balances load.
        final SortedMap<PeerAddress, DigestInfo> directHits = new TreeMap<PeerAddress, DigestInfo>(addressComparator);
        final NavigableSet<PeerAddress> potentialHits = new TreeSet<PeerAddress>(addressComparator);

        // fill initially
        queueToAsk.addAll(peerAddresses);

        //PeerStatistic serverPeer = new PeerStatistic(peerBean.serverPeerAddress());

        alreadyAsked.add(peerBean.serverPeerAddress());
        potentialHits.add(peerBean.serverPeerAddress());

        // domainkey can be null if we bootstrap
        if (type == Type.REQUEST_2 && routingBuilder.domainKey() != null && !randomSearch && peerBean.digestStorage() !=null) {
            final Number640 from;
            final Number640 to;
            if (routingBuilder.from()!=null && routingBuilder.to()!=null) {
            	from = routingBuilder.from();
            	to = routingBuilder.to();
            } else if (routingBuilder.contentKey() == null) {
                from = new Number640(routingBuilder.locationKey(), routingBuilder.domainKey(),
                        Number160.ZERO, Number160.ZERO);
                to = new Number640(routingBuilder.locationKey(), routingBuilder.domainKey(),
                        Number160.MAX_VALUE, Number160.MAX_VALUE);
            } else {
                from = new Number640(routingBuilder.locationKey(), routingBuilder.domainKey(),
                        routingBuilder.contentKey(), Number160.ZERO);
                to = new Number640(routingBuilder.locationKey(), routingBuilder.domainKey(),
                        routingBuilder.contentKey(), Number160.MAX_VALUE);
            }
            DigestInfo digestBean = peerBean.digestStorage().digest(from, to, -1, true);
            if (digestBean.size() > 0) {
                directHits.put(peerBean.serverPeerAddress(), digestBean);
            }
        } else if (type == Type.REQUEST_3 && !randomSearch && peerBean.digestTracker() != null) {
            DigestInfo digestInfo = peerBean.digestTracker().digest(routingBuilder.locationKey(),
                    routingBuilder.domainKey(), routingBuilder.contentKey());
            // we always put ourselves to the tracker list, so we need to check
            // if we know also other peers on our trackers.
            if (digestInfo.size() > 0) {
                directHits.put(peerBean.serverPeerAddress(), digestInfo);
            }
        }
        
        final FutureRouting futureRouting = new FutureRouting();
        final RoutingMechanism routingMechanism = routingBuilder.createRoutingMechanism(futureRouting);
        routingMechanism.queueToAsk(queueToAsk);
        routingMechanism.potentialHits(potentialHits);
        routingMechanism.directHits(directHits);
        routingMechanism.alreadyAsked(alreadyAsked);
        
        //evaluate direct hits early
        if(directHits.size() >= routingMechanism.getMaxDirectHits()) {
            futureRouting.neighbors(directHits, potentialHits, alreadyAsked, randomSearch, randomSearch);
            return futureRouting;
        }     
        
        if (peerAddresses.isEmpty()) {
        	routingBuilder.routingOnlyToSelf(false);
        	routingMechanism.neighbors(routingBuilder);
        } else {
            // if a peer bootstraps to itself, then the size of peerAddresses
            // is 1 and it contains itself. Check for that because we need to
            // know if we are routing, bootstrapping and bootstrapping to
            // ourselves, to return the correct status for the future
            boolean isRoutingOnlyToSelf = (peerAddresses.size() == 1 && peerAddresses.iterator().next()
                    .peerAddress().equals(peerBean.serverPeerAddress()));
            routingBuilder.routingOnlyToSelf(isRoutingOnlyToSelf);
            routingRec(routingBuilder, routingMechanism, type, cc);
        }
        return futureRouting;
    }

    /**
     * Looks for a route to the given locationKey, performing recursively. Since this method is not called concurrently,
     * but sequentially, no synchronization is necessary.
     *
     * @param routingBuilder
     * @param routingMechanism
     * @param type
     * @param channelCreator
     */
    private void routingRec(final RoutingBuilder routingBuilder, final RoutingMechanism routingMechanism,
            final Type type, final ChannelCreator channelCreator) {

        final boolean randomSearch = routingBuilder.locationKey() == null;
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
                    // If we search for a random peer, then the peer should
                    // return the address farest away.
                    final Number160 locationKey2 = randomSearch ? next.peerId().xor(Number160.MAX_VALUE)
                            : routingBuilder.locationKey();
                    routingBuilder.locationKey(locationKey2);
                     
                    
                    //routingMechanism.futureResponse(i, neighbors.closeNeighbors(next,
                    //        routingBuilder.searchValues(), type, channelCreator, routingBuilder));
                    LOG.debug("get close neighbors: {} on {}", next, i);
                }
            } else if (routingMechanism.futureResponse(i) != null) {
                LOG.debug("Activity on {}.", i);
                active++;
            }
        }
        if (active == 0) {
            LOG.debug("No activity, closing.");

            routingMechanism.neighbors(routingBuilder);
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
                    Message lastResponse = future.last().responseMessage();
                    PeerAddress remotePeer = lastResponse.sender();
                    routingMechanism.addPotentialHits(remotePeer);
                    Collection<PeerAddress> newNeighbors = lastResponse.neighborsSet(0).neighbors();
                    Collection<PeerStatistic> newNeighborStatistics = peerMap().getPeerStatistics(newNeighbors);

                    Integer resultSize = lastResponse.intAt(0);
                    Number160 keyDigest = lastResponse.key(0);
                    Number160 contentDigest = lastResponse.key(1);
                    DigestInfo digestBean = new DigestInfo(keyDigest, contentDigest, resultSize == null ? 0
                            : resultSize);
                    LOG.debug("Peer ({}) {} reported {} in message {}.", (digestBean.size() > 0 ? "direct" : "none"),
                            remotePeer, newNeighbors, lastResponse);
                    finished = routingMechanism.evaluateSuccess(remotePeer, digestBean, newNeighborStatistics, last, routingBuilder.locationKey());
                    LOG.debug("Routing finished {} / {}.", finished,
                            routingMechanism.isStopCreatingNewFutures());
                } else {
                    // if it failed but the failed is the closest one, its good to try again, since the peer might just
                    // be busy
                    LOG.debug("Routing error {}.", future.failedReason());
                    finished = routingMechanism.evaluateFailed();
                    routingMechanism.stopCreatingNewFutures(finished);
                }
                if (finished) {
                    LOG.debug("Routing finished. Direct hits: {}. Potential hits: {}.",
                            routingMechanism.directHits(), routingMechanism.potentialHits());

                    routingMechanism.neighbors(routingBuilder);
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
    /*
     * public static void cancel(final AtomicReferenceArray<? extends BaseFuture> futureResponses) { int len =
     * futureResponses.length(); for (int i = 0; i < len; i++) { BaseFuture baseFuture = futureResponses.get(i); if
     * (baseFuture != null) { baseFuture.cancel(); } } }
     */

}
