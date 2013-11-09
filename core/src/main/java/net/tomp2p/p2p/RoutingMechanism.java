package net.tomp2p.p2p;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The routing mechanism. Since this is called from Netty handlers, we don't have any visibility issues. If you want to
 * use an execution handler, then it must be an OrderedMemoryAwareThreadPoolExecutor.
 * 
 * http://stackoverflow.com/questions/8304309/keeping-state-in-a-netty-channelhandler
 * 
 * @author Thomas Bocek
 * 
 */
public class RoutingMechanism {
    
    private static final Logger LOG = LoggerFactory.getLogger(RoutingMechanism.class);
    
    private final AtomicReferenceArray<FutureResponse> futureResponses;
    private final FutureRouting futureRoutingResponse;

    private NavigableSet<PeerAddress> queueToAsk;
    private SortedSet<PeerAddress> alreadyAsked;
    private SortedMap<PeerAddress, DigestInfo> directHits;
    private NavigableSet<PeerAddress> potentialHits;

    private int nrNoNewInfo = 0;
    private int nrFailures = 0;
    private int nrSuccess = 0;

    private int maxDirectHits;
    private int maxNoNewInfo;
    private int maxFailures;
    private int maxSucess;
    private boolean stopCreatingNewFutures;

    /**
     * Creates the routing mechanism. Make sure to set the max* fields.
     * 
     * @param futureResponses
     *            The current future responeses that are running
     * @param futureRoutingResponse
     *            The reponse future from this routing request
     */
    public RoutingMechanism(final AtomicReferenceArray<FutureResponse> futureResponses,
            final FutureRouting futureRoutingResponse) {
        this.futureResponses = futureResponses;
        this.futureRoutingResponse = futureRoutingResponse;
    }

    /**
     * @return The number of parallel requests. The number is determined by the length of the future response array
     */
    public int parallel() {
        return futureResponses.length();
    }

    /**
     * @return True if we should stop creating more futures, false otherwise
     */
    public boolean isStopCreatingNewFutures() {
        return stopCreatingNewFutures;
    }

    /**
     * @param i
     *            The number of the future response to get
     * @return The future response at position i
     */
    public FutureResponse futureResponse(final int i) {
        return futureResponses.get(i);
    }

    /**
     * @param i
     *            The number of the future response to get and set
     * @param futureResponse
     *            The future response to set
     * @return The old future response
     */
    public FutureResponse futureResponse(final int i, final FutureResponse futureResponse) {
        return futureResponses.getAndSet(i, futureResponse);
    }

    /**
     * @param queueToAsk
     *            The queue that contains the peers that will be queried in the future
     * @return This class
     */
    public RoutingMechanism queueToAsk(final NavigableSet<PeerAddress> queueToAsk) {
        this.queueToAsk = queueToAsk;
        return this;
    }

    /**
     * @return The queue that contains the peers that will be queried in the future
     */
    public NavigableSet<PeerAddress> queueToAsk() {
        synchronized (this) {
            return queueToAsk;
        }
    }

    /**
     * @param alreadyAsked
     *            The peer we have already queried, we need to store them to not ask the same peers again
     * @return This class
     */
    public RoutingMechanism alreadyAsked(final SortedSet<PeerAddress> alreadyAsked) {
        this.alreadyAsked = alreadyAsked;
        return this;
    }

    /**
     * @return The peer we have already queried, we need to store them to not ask the same peers again
     */
    public SortedSet<PeerAddress> alreadyAsked() {
        synchronized (this) {
            return alreadyAsked;
        }
    }

    /**
     * @param potentialHits
     *            The potential hits are those reported by other peers that we did not check if they contain certain
     *            data.
     * @return This class
     */
    public RoutingMechanism potentialHits(final NavigableSet<PeerAddress> potentialHits) {
        this.potentialHits = potentialHits;
        return this;
    }

    /**
     * @return The potential hits are those reported by other peers that we did not check if they contain certain data.
     */
    public NavigableSet<PeerAddress> potentialHits() {
        synchronized (this) {

            return potentialHits;
        }
    }

    /**
     * @param directHits
     *            The peers that have certain data stored on it
     * @return This class
     */
    public RoutingMechanism directHits(final SortedMap<PeerAddress, DigestInfo> directHits) {
        this.directHits = directHits;
        return this;
    }

    /**
     * @return The peers that have certain data stored on it
     */
    public SortedMap<PeerAddress, DigestInfo> directHits() {
        return directHits;
    }

    public int getMaxDirectHits() {
        return maxDirectHits;
    }

    public void setMaxDirectHits(int maxDirectHits) {
        this.maxDirectHits = maxDirectHits;
    }

    public int getMaxNoNewInfo() {
        return maxNoNewInfo;
    }

    public void setMaxNoNewInfo(int maxNoNewInfo) {
        this.maxNoNewInfo = maxNoNewInfo;
    }

    public int getMaxFailures() {
        return maxFailures;
    }

    public void setMaxFailures(int maxFailures) {
        this.maxFailures = maxFailures;
    }

    public int getMaxSucess() {
        return maxSucess;
    }

    public void setMaxSucess(int maxSucess) {
        this.maxSucess = maxSucess;
    }

    public PeerAddress pollFirstInQueueToAsk() {
        synchronized (this) {
            return queueToAsk.pollFirst();
        }
    }

    public PeerAddress pollRandomInQueueToAsk(Random rnd) {
        synchronized (this) {
            return Utils.pollRandom(queueToAsk(), rnd);
        }

    }

    public void addToAlreadyAsked(PeerAddress next) {
        synchronized (this) {
            alreadyAsked.add(next);
        }
    }

    public void setNeighbors(RoutingBuilder builder) {
        synchronized (this) {

            futureRoutingResponse.setNeighbors(directHits, potentialHits, alreadyAsked,
                    builder.isBootstrap(), builder.isRoutingToOthers());
        }
    }

    /**
     * Cancel the future that causes the underlying futures to cancel as well.
     */
    public void cancel() {
        int len = futureResponses.length();
        for (int i = 0; i < len; i++) {
            BaseFuture baseFuture = futureResponses.get(i);
            if (baseFuture != null) {
                baseFuture.cancel();
            }
        }
    }

    public AtomicReferenceArray<FutureResponse> futureResponses() {
        return futureResponses;
    }

    public void addPotentialHits(PeerAddress remotePeer) {
        synchronized (this) {
            potentialHits.add(remotePeer);
        }
    }

    public void stopCreatingNewFutures(boolean stopCreatingNewFutures) {
        this.stopCreatingNewFutures = stopCreatingNewFutures;
    }

    public boolean evaluateFailed() {
        return (++nrFailures) > getMaxFailures();
    }

    public boolean evaluateSuccess(PeerAddress remotePeer, DigestInfo digestBean,
            Collection<PeerAddress> newNeighbors, boolean last) {
        boolean finished;
        synchronized (this) {
            if (evaluateDirectHits(remotePeer, directHits, digestBean, getMaxDirectHits())) {
                // stop immediately
                LOG.debug("we have enough direct hits {}", directHits);
                finished = true;
                stopCreatingNewFutures = true;
            } else if ((++nrSuccess) > getMaxSucess()) {
                // wait until pending futures are finished
                LOG.debug("we have reached max success {}", nrSuccess);
                finished = last;
                stopCreatingNewFutures = true;
            } else if (evaluateInformation(newNeighbors, queueToAsk, alreadyAsked, getMaxNoNewInfo())) {
                // wait until pending futures are finished
                LOG.debug("we have no new information for the {} time", getMaxNoNewInfo());
                finished = last;
                stopCreatingNewFutures = true;
            } else {
                // continue
                finished = false;
                stopCreatingNewFutures = false;
            }
        }
        return finished;
    }

    /**
     * For get requests we can finish earlier if we found the data we were looking for. This checks if we reached the
     * end of our search.
     * 
     * @param remotePeer
     *            The remote peer that gave us thi digest information
     * @param directHits
     *            The result map that will store how many peers reported that data is there
     * @param digestBean
     *            The digest information coming from the remote peer
     * @param maxDirectHits
     *            The max. number of direct hits, e.g. finding the value we were looking for before we can stop.
     * @return True if we can stop, false, if we should continue.
     */
    static boolean evaluateDirectHits(final PeerAddress remotePeer,
            final Map<PeerAddress, DigestInfo> directHits, final DigestInfo digestBean,
            final int maxDirectHits) {
        if (digestBean.getSize() > 0) {
            directHits.put(remotePeer, digestBean);
            if (directHits.size() >= maxDirectHits) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if we reached the end of our search.
     * 
     * @param newNeighbors
     *            The new neighbors we just received
     * @param queueToAsk
     *            The peers that are in the queue to be asked
     * @param alreadyAsked
     *            The peers we have already asked
     * @param noNewInfo
     *            counter how many times we did not find any peer that is closer to the target
     * @param maxNoNewInfo
     *            The maximum number of replies from neighbors that do not give us closer peers
     * @return True if we should stop, false if we should continue with the routing
     */
     boolean evaluateInformation(final Collection<PeerAddress> newNeighbors,
            final SortedSet<PeerAddress> queueToAsk, final Set<PeerAddress> alreadyAsked,
            final int maxNoNewInfo) {
        boolean newInformation = merge(queueToAsk, newNeighbors, alreadyAsked);
        if (newInformation) {
            nrNoNewInfo = 0;
            return false;
        } else {
            return (++nrNoNewInfo) >= maxNoNewInfo;
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
    static boolean merge(final SortedSet<PeerAddress> queueToAsk, final Collection<PeerAddress> newPeers,
            final Collection<PeerAddress> alreadyAsked) {
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
     * Checks if an item will be the highest in a sorted set.
     * 
     * @param queueToAsk
     *            The sorted set to check
     * @param item
     *            The element to check if it will be the highest in the sorted set
     * @return True, if item will be the highest element.
     */
    private static boolean isNew(final SortedSet<PeerAddress> queueToAsk, final PeerAddress item) {
        if (queueToAsk.contains(item)) {
            return false;
        }
        SortedSet<PeerAddress> tmp = queueToAsk.headSet(item);
        return tmp.size() == 0;
    }

    public int nrNoNewInfo() {
        return nrNoNewInfo;
    }

}
