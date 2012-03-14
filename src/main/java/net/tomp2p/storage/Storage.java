package net.tomp2p.storage;
import java.security.PublicKey;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;

public interface Storage extends Digest
{
	// Core
	public abstract boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data value);

	public abstract Data get(Number160 locationKey, Number160 domainKey, Number160 contentKey);
	
	public abstract boolean contains(Number160 locationKey, Number160 domainKey, Number160 contentKey);
	
	public abstract Data remove(Number160 locationKey, Number160 domainKey, Number160 contentKey);
	
	public abstract SortedMap<Number480, Data> subMap(Number160 locationKey, Number160 domainKey, 
			Number160 fromContentKey, Number160 toContentKey);
	
	public abstract Map<Number480, Data> subMap(Number160 locationKey);
	
	public abstract void close();

	// Maintenance
	public abstract void addTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey, long expiration);
	
	public abstract void removeTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey);
	
	public abstract Collection<Number480> subMapTimeout(long to);
	
	// Domain / entry protection
	public abstract boolean protectDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey);
		
	public abstract boolean isDomainProtectedByOthers(Number160 locationKey, Number160 domainKey, PublicKey publicKey);
	
	// Replication
	
	public abstract Number160 findPeerIDForResponsibleContent(Number160 locationKey);
	
	public abstract Collection<Number160> findContentForResponsiblePeerID(Number160 peerID);
	
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId);
	
	public void removeResponsibility(Number160 locationKey);
}