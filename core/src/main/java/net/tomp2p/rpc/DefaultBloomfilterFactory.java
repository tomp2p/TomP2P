package net.tomp2p.rpc;

import net.tomp2p.peers.Number160;

public class DefaultBloomfilterFactory  implements BloomfilterFactory {

    @Override
    public SimpleBloomFilter<Number160> createContentBloomFilter() {
        return new SimpleBloomFilter<Number160>(100, 1000);
    }

    @Override
    public SimpleBloomFilter<Number160> createLoctationKeyBloomFilter() {
        return new SimpleBloomFilter<Number160>(100, 1000);
    }

    @Override
    public SimpleBloomFilter<Number160> createDomainKeyBloomFilter() {
        return new SimpleBloomFilter<Number160>(100, 1000);
    }

    @Override
    public SimpleBloomFilter<Number160> createContentKeyBloomFilter() {
        return new SimpleBloomFilter<Number160>(100, 1000);
    }

}
