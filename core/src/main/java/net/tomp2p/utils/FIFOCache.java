package net.tomp2p.utils;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A thread-safe FIFO Cache with a fixed size based on a ConcurrentLinkedQueue
 *
 * Replace with ConcurrentLinkedDeque when Java 1.7 is available on Android.
 *
 * Created by Sebastian Stephan on 03.12.14.
 */
public class FIFOCache<E> extends ConcurrentLinkedQueue<E> {

    private static final long serialVersionUID = -5606903694547209363L;
	private final int max_size;

    public FIFOCache(int max_size) {
        this.max_size = max_size;
    }

    /**
     * Add an element to the FIFO cache. If the cache is already full, the
     * oldest element in the cache is removed.
     *
     * @param e Element to add
     * @return True if the cache changed because of the insert
     */
    @Override
    public boolean add(E e) {
        if (!super.add(e))
            return false;

        while(super.size() > max_size)
        {
            poll();
        }
        return true;
    }

    /**
     * Returns the tail element of the queue, i.e. the last inserted element
     *
     * @return The tail element or null if the list is empty
     */
    public E peekTail() {
        Iterator<E> it = iterator();
        E item = null;
        while (it.hasNext())
            item = it.next();

        return item;
    }
}
