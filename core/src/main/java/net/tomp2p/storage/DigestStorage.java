package net.tomp2p.storage;

import java.util.Collection;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number640;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.SimpleBloomFilter;

public interface DigestStorage {

	public abstract DigestInfo digest(Number640 from, Number640 to, int limit, boolean ascending);

	public abstract DigestInfo digest(Number320 locationAndDomainKey, SimpleBloomFilter<Number160> keyBloomFilter,
	        SimpleBloomFilter<Number160> contentBloomFilter, int limit, boolean ascending, boolean isBloomFilterAnd);

	public abstract DigestInfo digest(Collection<Number640> number640s);

}