package net.tomp2p.p2p;

import java.util.Comparator;
import java.util.TreeSet;

/**
 * A TreeSet that allows for updates. When the item is already in the Set,
 * the item is removed before adding it.
 *
 * Created by Sebastian Stephan on 10.12.14.
 */
public class UpdatableTreeSet<E> extends TreeSet<E> {

    private static final long serialVersionUID = 5532491647323982466L;

	/**
     * Creates a new empty PriorityQueueSet with the given comparator
     * @param comparator
     */
    public UpdatableTreeSet(Comparator<? super E> comparator) {
        super(comparator);
    }

    @Override
    public boolean add(E e) {
        while (contains(e)) {
            remove(e);
        }
        return super.add(e);
    }

}
