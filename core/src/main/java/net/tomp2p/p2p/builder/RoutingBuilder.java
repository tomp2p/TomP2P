package net.tomp2p.p2p.builder;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.p2p.PostRoutingFilter;
import net.tomp2p.p2p.RoutingMechanism;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerMapFilter;
import net.tomp2p.rpc.NeighborRPC.SearchValues;
import net.tomp2p.rpc.SimpleBloomFilter;

public class RoutingBuilder extends DefaultConnectionConfiguration {

    private Number160 locationKey;
    private Number160 domainKey;
    private Number160 contentKey;

    private SimpleBloomFilter<Number160> keyBloomFilter;
    private SimpleBloomFilter<Number160> contentBloomFilter;
    
    private Number640 from;
    private Number640 to;
    
    private Collection<PeerMapFilter> peerMapFilters;
    private Collection<PostRoutingFilter> postRoutingFilters;

    private int maxDirectHits;
    private int maxNoNewInfo;
    private int maxFailures;
    private int maxSuccess;
    private int parallel;
    private boolean isBootstrap;
    private boolean isForceRoutingOnlyToSelf;
    private boolean isRoutingToOthers;

    public Number160 locationKey() {
        return locationKey;
    }

    public Number160 domainKey() {
        return domainKey;
    }

    /**
     * number of direct hits to stop at
     * 
     * @return
     */
    public int maxDirectHits() {
        return maxDirectHits;
    }

    public void maxDirectHits(int maxDirectHits) {
        this.maxDirectHits = maxDirectHits;
    }

    public int maxNoNewInfo() {
        return maxNoNewInfo;
    }

    public void setMaxNoNewInfo(int maxNoNewInfo) {
        this.maxNoNewInfo = maxNoNewInfo;
    }

    public int maxFailures() {
        return maxFailures;
    }

    public void maxFailures(int maxFailures) {
        this.maxFailures = maxFailures;
    }

    public int maxSuccess() {
        return maxSuccess;
    }

    public void maxSuccess(int maxSuccess) {
        this.maxSuccess = maxSuccess;
    }

    public int parallel() {
        return parallel;
    }

    public void parallel(int parallel) {
        this.parallel = parallel;
    }

    public boolean isBootstrap() {
        return isBootstrap;
    }

    public void bootstrap(boolean isBootstrap) {
        this.isBootstrap = isBootstrap;
    }

    public boolean isForceRoutingOnlyToSelf() {
        return isForceRoutingOnlyToSelf;
    }

    public void forceRoutingOnlyToSelf(boolean isForceRoutingOnlyToSelf) {
        this.isForceRoutingOnlyToSelf = isForceRoutingOnlyToSelf;
    }

    public void locationKey(Number160 locationKey) {
        this.locationKey = locationKey;
    }
    
    public void domainKey(Number160 domainKey) {
        this.domainKey = domainKey;
    }
    
    public RoutingBuilder peerMapFilters(Collection<PeerMapFilter> peerMapFilters) {
    	this.peerMapFilters = peerMapFilters;
    	return this;
    }
    
    public Collection<PeerMapFilter> peerMapFilters() {
    	return peerMapFilters;
    }
    
    public RoutingBuilder postRoutingFilters(Collection<PostRoutingFilter> postRoutingFilters) {
		this.postRoutingFilters = postRoutingFilters;
    	return this;
    }
    
    public Collection<PostRoutingFilter> postRoutingFilters() {
    	return postRoutingFilters;
    }

    /**
     * @return The search values for the neighbor request, or null if no content key is specified
     */
    public SearchValues searchValues() {
        if (contentKey() != null) {
        	return new SearchValues(locationKey, domainKey, contentKey());
        } 
        if(from !=null && to!=null) {
        	return new SearchValues(locationKey, domainKey, from, to);
        } 
        if (contentBloomFilter() == null && keyBloomFilter() != null) {
            return new SearchValues(locationKey, domainKey, keyBloomFilter());
        } 
        if (contentBloomFilter() != null && keyBloomFilter() != null) {
            return new SearchValues(locationKey, domainKey, keyBloomFilter(), contentBloomFilter());
        }
        return new SearchValues(locationKey, domainKey);
    }

    public RoutingBuilder routingOnlyToSelf(boolean isRoutingOnlyToSelf) {
        this.isRoutingToOthers = !isRoutingOnlyToSelf;
        return this;
    }
    
    public boolean isRoutingToOthers() {
        return isRoutingToOthers;
    }

    public Number160 contentKey() {
        return contentKey;
    }

    public void contentKey(Number160 contentKey) {
        this.contentKey = contentKey;
    }

    public SimpleBloomFilter<Number160> contentBloomFilter() {
        return contentBloomFilter;
    }

    public void contentBloomFilter(SimpleBloomFilter<Number160> contentBloomFilter) {
        this.contentBloomFilter = contentBloomFilter;
    }

    public SimpleBloomFilter<Number160> keyBloomFilter() {
        return keyBloomFilter;
    }

    public void keyBloomFilter(SimpleBloomFilter<Number160> keyBloomFilter) {
        this.keyBloomFilter = keyBloomFilter;
    }

    public RoutingMechanism createRoutingMechanism(FutureRouting futureRouting) {
        final FutureResponse[] futureResponses = new FutureResponse[parallel()];
        RoutingMechanism routingMechanism = new RoutingMechanism(
                new AtomicReferenceArray<FutureResponse>(futureResponses), futureRouting, peerMapFilters);
        routingMechanism.maxDirectHits(maxDirectHits());
        routingMechanism.maxFailures(maxFailures());
        routingMechanism.maxNoNewInfo(maxNoNewInfo());
        routingMechanism.maxSucess(maxSuccess());
        return routingMechanism;
    }

	public void range(Number640 from, Number640 to) {
	    this.from = from;
	    this.to = to;
    }
	
	public Number640 from() {
		return from;
	}
	
	public Number640 to() {
		return to;
	}
}