package net.tomp2p.rpc;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;

public interface BloomfilterFactory {

    SimpleBloomFilter<Number160> createContentBloomFilter();

    SimpleBloomFilter<Number160> createLoctationKeyBloomFilter();

    SimpleBloomFilter<Number160> createDomainKeyBloomFilter();

    SimpleBloomFilter<Number160> createContentKeyBloomFilter();

}
