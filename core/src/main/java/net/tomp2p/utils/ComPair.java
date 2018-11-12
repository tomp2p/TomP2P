package net.tomp2p.utils;

public class ComPair<K extends Comparable,V extends Comparable> extends Pair<K,V> implements Comparable<Pair<K,V>> {
    public ComPair(K element0, V element1) {
        super(element0, element1);
    }

    public static <K extends Comparable, V extends Comparable> ComPair<K, V> of(final K element0, final V element1) {
        return new ComPair<K, V>(element0, element1);
    }

    @Override
    public int compareTo(Pair<K, V> o) {
        final int diff = element0().compareTo(o.element0());
        if(diff == 0) {
            return element1().compareTo(o.element1());
        } else {
            return diff;
        }
    }
}
