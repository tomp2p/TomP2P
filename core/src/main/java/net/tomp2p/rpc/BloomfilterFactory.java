package net.tomp2p.rpc;

import net.tomp2p.peers.Number160;

public interface BloomfilterFactory {

	SimpleBloomFilter<Number160> createContentKeyBloomFilter();
	
	SimpleBloomFilter<Number160> createVersionKeyBloomFilter();
    
    SimpleBloomFilter<Number160> createContentBloomFilter();

}
