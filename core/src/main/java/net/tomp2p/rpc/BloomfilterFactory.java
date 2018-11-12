package net.tomp2p.rpc;

import net.tomp2p.peers.Number256;

public interface BloomfilterFactory {

	SimpleBloomFilter<Number256> createContentKeyBloomFilter();
	
	SimpleBloomFilter<Number256> createVersionKeyBloomFilter();
    
    SimpleBloomFilter<Number256> createContentBloomFilter();

}
