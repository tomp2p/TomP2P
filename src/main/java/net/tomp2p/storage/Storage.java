package net.tomp2p.storage;
import java.security.PublicKey;
import java.util.Collection;
import java.util.SortedMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;

public interface Storage extends Digest, Responsibility
{
	// Core
	public abstract boolean put(Number480 key, Data data, PublicKey publicKey, boolean putIfAbsent,
			boolean domainProtection);

	public abstract Data get(Number480 key);

	public abstract SortedMap<Number480, Data> get(Number480 fromKey, Number480 toKey);

	public abstract SortedMap<Number480, Data> remove(Number480 fromKey, Number480 toKey,
			PublicKey publicKey);

	public abstract Data remove(Number480 key, PublicKey publicKey);

	public abstract boolean contains(Number480 key);

	public abstract void iterateAndRun(Number160 locationKey, StorageRunner runner);

	public abstract void close();

	// Replication
	public abstract Collection<Number480> storedDirectReplication();
}