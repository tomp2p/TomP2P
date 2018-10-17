package net.tomp2p.utils;

public class Pair<K, V> {

	public final static Pair<?,?> EMPTY = empty();

	private final K element0;
	private final V element1;

    public Pair(final K element0, final V element1) {
        this.element0 = element0;
        this.element1 = element1;
    }

    public K e0() {
        return element0;
    }

    public V e1() {
        return element1;
    }

    public K element0() {
        return element0;
    }

    public V element1() {
        return element1;
    }


	public static <K, V> Pair<K, V> of(final K element0, final V element1) {
		return new Pair<K, V>(element0, element1);
	}

	public static <K, V> Pair<K, V> create(final K element0, final V element1) {
		return new Pair<K, V>(element0, element1);
	}
	
	public boolean isEmpty() {
		return this == EMPTY || (element0 == null && element1 == null);
	}
	
	public static <K,V> Pair<K, V> empty() {
		return new Pair<>(null, null);
	}

	/**
	 * Checks the two objects for equality by delegating to their respective
	 * {@link Object#equals(Object)} methods.
	 * 
	 * @param o
	 *            the {@link Pair} to which this one is to be checked for
	 *            equality
	 * @return true if the underlying objects of the Pair are both considered
	 *         equal
	 */
	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof Pair)) {
			return false;
		}
		if (this == o) {
			return true;
		}

        final Pair<?, ?> p = (Pair<?, ?>) o;
		return objectsEqual(p.element0, element0) && objectsEqual(p.element1, element1);
	}

	/**
	 * Compute a hash code using the hash codes of the underlying objects
	 * 
	 * @return a hashcode of the Pair
	 */
	@Override
	public int hashCode() {
		return (element0 == null ? 0 : element0.hashCode()) ^ (element1 == null ? 0 : element1.hashCode());
	}

	private static boolean objectsEqual(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }

	@Override
	public String toString() {
        final StringBuilder sb = new StringBuilder("Pair{");
        return sb.append(String.valueOf(element0))
                .append(',')
                .append(String.valueOf(element0))
                .append('}').toString();
	}
}