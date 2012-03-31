package net.tomp2p.storage;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.Number480;

public class StorageMemory extends StorageGeneric
{
	// Core
	final private NavigableMap<Number480, Data> dataMap = new ConcurrentSkipListMap<Number480, Data>();
	// Maintenance
	final private Map<Number480, Long> timeoutMap = new ConcurrentHashMap<Number480, Long>();
	final private SortedMap<Long, Set<Number480>> timeoutMapRev = new ConcurrentSkipListMap<Long, Set<Number480>>();
	// Protection
	final private Map<Number320, PublicKey> protectedMap = new ConcurrentHashMap<Number320, PublicKey>();
	
	final private StorageMemoryReplication storageMemoryReplication = new StorageMemoryReplication();
	
	final private KeyLock<Long> timeoutLock = new KeyLock<Long>();
	//Core
	@Override
	public boolean put(Number160 locationKey, Number160 domainKey, Number160 contentKey, Data value)
	{
		dataMap.put(new Number480(locationKey, domainKey, contentKey), value);
		return true;
	}
	
	@Override
	public Data get(Number160 locationKey, Number160 domainKey, Number160 contentKey)
	{
		return dataMap.get(new Number480(locationKey, domainKey, contentKey));
	}
	
	@Override
	public boolean contains(Number160 locationKey, Number160 domainKey, Number160 contentKey)
	{
		return dataMap.containsKey(new Number480(locationKey, domainKey, contentKey));
	}
	
	@Override
	public Data remove(Number160 locationKey, Number160 domainKey, Number160 contentKey)
	{
		return dataMap.remove(new Number480(locationKey, domainKey, contentKey));
	}
	
	@Override
	public SortedMap<Number480, Data> subMap(Number160 locationKey, Number160 domainKey,
			Number160 fromContentKey, Number160 toContentKey)
	{
		return dataMap.subMap(new Number480(locationKey, domainKey, fromContentKey), 
				new Number480(locationKey, domainKey, toContentKey));
	}
	
	@Override
	public Map<Number480, Data> subMap(Number160 locationKey)
	{
		return dataMap.subMap(new Number480(locationKey, Number160.ZERO, Number160.ZERO), 
				new Number480(locationKey, Number160.MAX_VALUE, Number160.MAX_VALUE));
	}
	
	@Override
	public NavigableMap<Number480, Data> map()
	{
		return dataMap;
	}
	
	// Maintenance
	@Override
	public void addTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey, long expiration)
	{
		Number480 key = new Number480(locationKey, domainKey, contentKey);
		Long oldExpiration = timeoutMap.put(key, expiration);
		Lock lock1 = timeoutLock.lock(expiration);
		try
		{
			Set<Number480> tmp = putIfAbsent2(expiration, new HashSet<Number480>());
			tmp.add(key);
		}
		finally
		{
			timeoutLock.unlock(expiration, lock1);
		}
		if(oldExpiration == null)
		{
			return;
		}
		Lock lock2 = timeoutLock.lock(oldExpiration);
		try
		{
			removeRevTimeout(key, oldExpiration);
			
		}
		finally
		{
			timeoutLock.unlock(oldExpiration, lock2);
		}
	}

	@Override
	public void removeTimeout(Number160 locationKey, Number160 domainKey, Number160 contentKey)
	{
		Number480 key = new Number480(locationKey, domainKey, contentKey);
		Long expiration = timeoutMap.remove(key);
		if(expiration == null)
		{
			return;
		}
		Lock lock = timeoutLock.lock(expiration);
		try
		{
			removeRevTimeout(key, expiration);
		}
		finally
		{
			timeoutLock.unlock(expiration, lock);
		}
	}
	
	private void removeRevTimeout(Number480 key, Long expiration)
	{
		if (expiration != null)
		{
			Set<Number480> tmp2 = timeoutMapRev.get(expiration);
			if (tmp2 != null)
			{
				tmp2.remove(key);
				if (tmp2.isEmpty())
				{
					timeoutMapRev.remove(expiration);
				}
			}
		}		
	}
	
	@Override
	public Collection<Number480> subMapTimeout(long to)
	{
		SortedMap<Long, Set<Number480>> tmp = timeoutMapRev.subMap(0L, to);
		Collection<Number480> toRemove = new ArrayList<Number480>();
		for (Map.Entry<Long, Set<Number480>> entry : tmp.entrySet())
		{
			Lock lock = timeoutLock.lock(entry.getKey());
			try
			{
				toRemove.addAll(entry.getValue());
			}
			finally
			{
				timeoutLock.unlock(entry.getKey(), lock);
			}
		}
		return toRemove;
	}
	
	// Protection	
	@Override
	public boolean protectDomain(Number160 locationKey, Number160 domainKey, PublicKey publicKey)
	{
		protectedMap.put(new Number320(locationKey, domainKey), publicKey);
		return true;
	}

	@Override
	public boolean isDomainProtectedByOthers(Number160 locationKey, Number160 domainKey,
			PublicKey publicKey)
	{
		PublicKey other = protectedMap.get(new Number320(locationKey, domainKey));
		if (other == null)
		{
			return false;
		}
		return !other.equals(publicKey);
	}
	
	//Misc
	@Override
	public void close()
	{
		dataMap.clear();
		protectedMap.clear();
		timeoutMap.clear();
		timeoutMapRev.clear();
	}

	
	
	private Set<Number480> putIfAbsent2(long expiration, Set<Number480> hashSet)
	{
		@SuppressWarnings("unchecked")
		Set<Number480> timeouts = ((ConcurrentMap<Long, Set<Number480>>)timeoutMapRev).putIfAbsent(expiration, hashSet);
		return timeouts == null ? hashSet:timeouts;
	}

	@Override
	public Number160 findPeerIDForResponsibleContent(Number160 locationKey)
	{
		return storageMemoryReplication.findPeerIDForResponsibleContent(locationKey);
	}

	@Override
	public Collection<Number160> findContentForResponsiblePeerID(Number160 peerID)
	{
		return storageMemoryReplication.findContentForResponsiblePeerID(peerID);
	}

	@Override
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId)
	{
		return storageMemoryReplication.updateResponsibilities(locationKey, peerId);
	}

	@Override
	public void removeResponsibility(Number160 locationKey)
	{
		storageMemoryReplication.removeResponsibility(locationKey);
	}
}