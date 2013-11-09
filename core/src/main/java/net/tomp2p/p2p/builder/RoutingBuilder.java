package net.tomp2p.p2p.builder;

import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection2.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.p2p.RoutingMechanism;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.NeighborRPC.SearchValues;
import net.tomp2p.rpc.SimpleBloomFilter;

public class RoutingBuilder extends DefaultConnectionConfiguration {

    private Number160 locationKey;
    private Number160 domainKey;

    private SimpleBloomFilter<Number160> keyBloomFilter;
    private SimpleBloomFilter<Number160> contentBloomFilter;
    private Number160 contentKey;

    private int maxDirectHits;
    private int maxNoNewInfo;
    private int maxFailures;
    private int maxSuccess;
    private int parallel;
    private boolean isBootstrap;
    private boolean isForceRoutingOnlyToSelf;
    private boolean isRoutingToOthers;

    public Number160 getLocationKey() {
        return locationKey;
    }

    public Number160 getDomainKey() {
        return domainKey;
    }

    /**
     * number of direct hits to stop at
     * 
     * @return
     */
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

    public int getMaxSuccess() {
        return maxSuccess;
    }

    public void setMaxSuccess(int maxSuccess) {
        this.maxSuccess = maxSuccess;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    

    public boolean isBootstrap() {
        return isBootstrap;
    }

    public void setBootstrap(boolean isBootstrap) {
        this.isBootstrap = isBootstrap;
    }

    public boolean isForceRoutingOnlyToSelf() {
        return isForceRoutingOnlyToSelf;
    }

    public void setForceRoutingOnlyToSelf(boolean isForceRoutingOnlyToSelf) {
        this.isForceRoutingOnlyToSelf = isForceRoutingOnlyToSelf;
    }

    public void setLocationKey(Number160 locationKey) {
        this.locationKey = locationKey;
    }
    
    public void setDomainKey(Number160 domainKey) {
        this.domainKey = domainKey;
    }

    /**
     * @return The search values for the neighbor request, or null if no content key specified
     */
    public SearchValues searchValues() {
        if (getContentKey() != null) {
            return new SearchValues(locationKey, domainKey, getContentKey());
        } else if (getContentBloomFilter() == null && getKeyBloomFilter() != null) {
            return new SearchValues(locationKey, domainKey, getKeyBloomFilter());
        } else if (getContentBloomFilter() != null && getKeyBloomFilter() != null) {
            return new SearchValues(locationKey, domainKey, getKeyBloomFilter(), getContentBloomFilter());
        } else {
            return new SearchValues(locationKey, domainKey);
        }
    }

    public RoutingBuilder routingOnlyToSelf(boolean isRoutingOnlyToSelf) {
        this.isRoutingToOthers = !isRoutingOnlyToSelf;
        return this;
    }
    
    public boolean isRoutingToOthers() {
        return isRoutingToOthers;
    }

    public Number160 getContentKey() {
        return contentKey;
    }

    public void setContentKey(Number160 contentKey) {
        this.contentKey = contentKey;
    }

    public SimpleBloomFilter<Number160> getContentBloomFilter() {
        return contentBloomFilter;
    }

    public void setContentBloomFilter(SimpleBloomFilter<Number160> contentBloomFilter) {
        this.contentBloomFilter = contentBloomFilter;
    }

    public SimpleBloomFilter<Number160> getKeyBloomFilter() {
        return keyBloomFilter;
    }

    public void setKeyBloomFilter(SimpleBloomFilter<Number160> keyBloomFilter) {
        this.keyBloomFilter = keyBloomFilter;
    }

    public RoutingMechanism createRoutingMechanism(FutureRouting futureRouting) {
        final FutureResponse[] futureResponses = new FutureResponse[getParallel()];
        RoutingMechanism routingMechanism = new RoutingMechanism(
                new AtomicReferenceArray<FutureResponse>(futureResponses), futureRouting);
        routingMechanism.setMaxDirectHits(getMaxDirectHits());
        routingMechanism.setMaxFailures(getMaxFailures());
        routingMechanism.setMaxNoNewInfo(getMaxNoNewInfo());
        routingMechanism.setMaxSucess(getMaxSuccess());
        return routingMechanism;
    }
}