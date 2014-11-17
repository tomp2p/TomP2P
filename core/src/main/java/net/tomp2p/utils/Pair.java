package net.tomp2p.utils;

import java.util.Objects;

public class Pair<K, V> {

	private final K element0;
	private final V element1;

	public static <K, V> Pair<K, V> create(K element0, V element1) {
		return new Pair<K, V>(element0, element1);
	}

	public Pair(K element0, V element1) {
		this.element0 = element0;
		this.element1 = element1;
	}

	public K element0() {
		return element0;
	}

	public V element1() {
		return element1;
	}
	
	public Pair<K, V> element0(K element0) {
	    return new Pair<K, V>(element0, element1);
    }
	
	public Pair<K, V> element1(V element1) {
	    return new Pair<K, V>(element0, element1);
    }
	
	public boolean isEmpty() {
		return element0 == null && element1 == null;
	}
	
	public static <K,V> Pair<K, V> empty() {
		return new Pair<K, V>(null, null);
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
	public boolean equals(Object o) {
		if (!(o instanceof Pair)) {
			return false;
		}
		if (this == o) {
			return true;
		}
		Pair<?, ?> p = (Pair<?, ?>) o;
		return Objects.equals(p.element0, element0) && Objects.equals(p.element1, element1);
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

	

}