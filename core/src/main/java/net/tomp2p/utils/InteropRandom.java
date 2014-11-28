package net.tomp2p.utils;

/**
 * A pseudo-random number generator that can be used to create the same output in Java and .NET. The
 * underlying algorithm is inspired from Java's Random implementation.
 * The use of this class ensures consistency, whereas Java's Random could be updated anytime.
 * 
 * @author Christian Lüthold
 *
 */
public final class InteropRandom {

	// inspired from http://docs.oracle.com/javase/6/docs/api/java/util/Random.html

	private long seed;

	public InteropRandom(long seed) {
		this.seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
	}

	public int nextInt(int n) {
		if (n <= 0)
			throw new IllegalArgumentException("n must be positive");

		if ((n & -n) == n) // i.e., n is a power of 2
			return (int) ((n * (long) next(31)) >> 31);

		int bits, val;
		do {
			bits = next(31);
			val = bits % n;
		} while (bits - val + (n - 1) < 0);
		return val;
	}

	private int next(int bits) {
		seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);

		return (int) (seed >>> (48 - bits));
	}
}
