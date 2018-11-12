package net.tomp2p.rpc;

import net.tomp2p.peers.Number256;

public class DefaultBloomfilterFactory  implements BloomfilterFactory {

    @Override
    public SimpleBloomFilter<Number256> createContentKeyBloomFilter() {
        return new SimpleBloomFilter<Number256>(0.01d, 1000);
    }

    @Override
    public SimpleBloomFilter<Number256> createVersionKeyBloomFilter() {
        return new SimpleBloomFilter<Number256>(0.01d, 1000);
    }
    
    @Override
    public SimpleBloomFilter<Number256> createContentBloomFilter() {
        return new SimpleBloomFilter<Number256>(0.01d, 1000);
    }

}
