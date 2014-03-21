package net.tomp2p.relay;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.tomp2p.utils.ConcurrentCacheMap;

/**
 * Wrapper of {@link ConcurrentCacheMap}
 * 
 * @author Raphael Voellmy
 *
 * @param <E>
 */
public class ConcurrentCacheSet<E> implements Set<E> {

    private final Map<E, Boolean> map;
    
    public ConcurrentCacheSet() {
        map = new ConcurrentCacheMap<E, Boolean>();
    }
    
    public ConcurrentCacheSet(int timeToLive) {
        map = new ConcurrentCacheMap<E, Boolean>(timeToLive, ConcurrentCacheMap.MAX_ENTRIES);
    }
    
    public ConcurrentCacheSet(int timeToLive, int maxEntries) {
        map = new ConcurrentCacheMap<E, Boolean>(timeToLive, maxEntries);
    }
    
    public ConcurrentCacheSet(int timeToLive, int maxEntries, boolean refreshTimeout) {
        map = new ConcurrentCacheMap<E, Boolean>(timeToLive, maxEntries, refreshTimeout);
    }
    
    @Override
    public boolean add(E e) {
        boolean alreadyContained = !map.containsKey(e);
        map.put(e, false);
        return alreadyContained;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
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
    public boolean contains(Object o) {
        return map.keySet().contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
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
    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        //TODO: type safety?
        @SuppressWarnings("unchecked")
        Iterator<E> it = (Iterator<E>) c.iterator();
        boolean changed = false;
        while(it.hasNext()) {
            changed |= remove(it.next());
        }
        return changed;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Set<?> diff = map.keySet();
        diff.removeAll(c);
        return removeAll(diff);
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
