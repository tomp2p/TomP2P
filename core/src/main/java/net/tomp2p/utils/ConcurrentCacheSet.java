package net.tomp2p.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Wrapper of {@link ConcurrentCacheMap}
 * 
 * @author Raphael Voellmy
 * @author Thomas Bocek
 *
 * @param <E>
 */
public class ConcurrentCacheSet<E> implements Set<E> {

    private final Map<E, Boolean> map;
    
    public ConcurrentCacheSet() {
        map = new ConcurrentCacheMap<E, Boolean>();
    }
    
    public ConcurrentCacheSet(final int timeToLiveSeconds) {
        map = new ConcurrentCacheMap<E, Boolean>(timeToLiveSeconds, ConcurrentCacheMap.MAX_ENTRIES);
    }
    
    public ConcurrentCacheSet(final int timeToLiveSeconds, final int maxEntries) {
        map = new ConcurrentCacheMap<E, Boolean>(timeToLiveSeconds, maxEntries);
    }
    
    public ConcurrentCacheSet(final int timeToLiveSeconds, final int maxEntries, final boolean refreshTimeout) {
        map = new ConcurrentCacheMap<E, Boolean>(timeToLiveSeconds, maxEntries, refreshTimeout);
    }
    
    @Override
    public boolean add(final E e) {
        final Boolean retVal = map.put(e, true);
        return Objects.equals(retVal, Boolean.TRUE);
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
        boolean changed = false;
        for(E o : c) {
            changed |= add(o);
        }
        return changed;
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public boolean contains(final Object o) {
        return map.keySet().contains(o);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return map.keySet().containsAll(c);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public boolean remove(final Object o) {
        return map.remove(o) != null;
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        final Iterator<?> it = (Iterator<?>) c.iterator();
        boolean changed = false;
        while(it.hasNext()) {
            changed |= remove(it.next());
        }
        return changed;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Object[] toArray() {
        return map.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return map.keySet().toArray(a);
    }
    
    @Override
    public String toString() {
        return map.keySet().toString();
    }

}
