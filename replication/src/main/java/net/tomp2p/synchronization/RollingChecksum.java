package net.tomp2p.synchronization;

/**
 * Variation of Adler as used in Rsync. Inspired by:
 * 
 * <pre>
 * https://github.com/epeli/rollsum/blob/master/ref/adler32.py
 * http://stackoverflow.com/questions/9699315/differences-in-calculation-of-adler32-rolling-checksum-python
 * http://de.wikipedia.org/wiki/Adler-32
 * http://developer.classpath.org/doc/java/util/zip/Adler32-source.html
 * </pre>
 * 
 * @author Thomas Bocek
 * 
 */
public class RollingChecksum {

	private int a = 1;
	private int b = 0;
	private int length;
	private int offset;

	/**
	 * Resets the checksum to its initial state 1.
	 * 
	 * @return this class
	 */
	public RollingChecksum reset() {
		a = 1;
		b = 0;
		return this;
	}

	/**
	 * Iterates over the array and calculates a variation of Adler.
	 * 
	 * @param array
	 *            The array for the checksum calculation
	 * @param offset
	 *            The offset of the array
	 * @param length
	 *            The length of the data to iterate over (the length of the
	 *            sliding window). Once this is set,
	 *            {@link #updateRolling(byte[])} will use the same value
	 * @return this class
	 */
	public RollingChecksum update(final byte[] array, final int offset, final int length) {
		for (int i = 0; i < length; i++) {
			a = (a + (array[i + offset] & 0xff)) & 0xffff;
			b = (b + a) & 0xffff;
		}
		this.length = length;
		this.offset = offset;
		return this;
	}

	/**
	 * @return The calculated checksum
	 */
	public int value() {
		return (b << 16) | a;
	}

	/**
	 * Sets the checksum to this value.
	 * 
	 * @param checksum
	 *            The checksum to set
	 * @return this class
	 */
	public RollingChecksum value(final int checksum) {
		a = checksum & 0xffff;
		b = checksum >>> 16;
		return this;
	}

	/**
	 * Slide the window of the array by 1.
	 * 
	 * @param array
	 *            The array for the checksum calculation
	 * @param offset
	 *            The offset of the array
	 * @return this class
	 */
	public RollingChecksum updateRolling(final byte[] array) {
		final int removeIndex = offset;
		final int addIndex = offset + length;
		offset++;
		a = (a - (array[removeIndex] & 0xff) + (array[addIndex] & 0xff)) & 0xffff;
		b = (b - (length * (array[removeIndex] & 0xff)) + a - 1) & 0xffff;
		return this;
	}
}
