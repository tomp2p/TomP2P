/*
 * Copyright 2013 Maxat Pernebayev, Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.synchronization;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.tomp2p.storage.DataBuffer;
import net.tomp2p.utils.Utils;

/**
 * Synchronization class is responsible for efficient and optimal
 * synchronization of data resources between responsible peer and replica peers.
 * If one of replicas goes offline, the responsible peer transfers the value
 * completely to the new replica peer. In case the values at responsible peer
 * and replica peer are the same, then no data is transmitted. If the values are
 * different, then only differences are sent to the replica peer.
 * 
 * @author Maxat Pernebayev
 * @author Thomas Bocek
 * 
 */
final public class RSync {

	/**
	 * It returns an array of weak and strong checksums for the value.
	 * 
	 * @param value
	 *            The value
	 * @param size
	 *            The offset size
	 * @return The array of checksums
	 * @throws NoSuchAlgorithmException
	 */
	public static List<Checksum> checksums(final byte[] value, final int blockSize) {
		final int numberOfBlocks = (value.length + blockSize - 1) / blockSize;
		final ArrayList<Checksum> checksums = new ArrayList<Checksum>(numberOfBlocks);
		final RollingChecksum adler = new RollingChecksum();

		for (int i = 0; i < numberOfBlocks; i++) {
			int remaining = Math.min(blockSize, value.length - (i * blockSize));
			adler.reset().update(value, i * blockSize, remaining);

			final int weakChecksum = adler.value();
			final byte[] strongChecksum = Utils.makeMD5Hash(value, i * blockSize, remaining);
			checksums.add(new Checksum(weakChecksum, strongChecksum));
		}
		return checksums;
	}

	/**
	 * It checks whether a match is found or not. If it is found returns
	 * reference otherwise -1.
	 * 
	 * @param wcs
	 *            The weak checksum of offset
	 * @param offset
	 *            The offset
	 * @param checksums
	 *            The checksums
	 * @return either the reference or -1
	 */
	private static int matches(int wcs, byte[] buffer, int offset, int length, List<Checksum> checksums) {
		int checksumSize = checksums.size();
		//TODO: hashing might be a better idea, for now it works
		for (int i = 0; i < checksumSize; i++) {
			int weakChecksum = checksums.get(i).weakChecksum();
			if (weakChecksum == wcs) {
				byte[] md5 = Utils.makeMD5Hash(buffer, offset, length);
				byte[] strongChecksum = checksums.get(i).strongChecksum();
				if (Arrays.equals(strongChecksum, md5)) {
					return i;
				}
			}
		}
		// no match found, content is different
		return -1;
	}

	/**
	 * It returns the sequence of instructions each of which contains either
	 * reference to a block or literal data.
	 * 
	 * @param array
	 *            The value at responsible peer
	 * @param checksums
	 *            The array of checksums
	 * @param blockSize
	 *            The block size
	 * @return The sequence of instructions
	 */
	public static List<Instruction> instructions(byte[] array, List<Checksum> checksums, int blockSize) {

		final List<Instruction> result = new ArrayList<Instruction>(checksums.size());
		final RollingChecksum adler = new RollingChecksum();
		final int length = array.length;

		int offset = 0;
		int lastRefFound = 0;
		int remaining = Math.min(blockSize, length - offset);

		adler.update(array, offset, remaining);

		for (;;) {
			final int wcs = adler.value();
			final int reference = matches(wcs, array, offset, remaining, checksums);
			if (reference != -1) {
				if (offset > lastRefFound) {
					result.add(new Instruction(new DataBuffer(array, lastRefFound, offset - lastRefFound)));
				}
				result.add(new Instruction(reference));

				offset += remaining;
				lastRefFound = offset;
				remaining = Math.min(blockSize, length - offset);
				if (remaining == 0) {
					break;
				}
				adler.reset().update(array, offset, remaining);
			} else {
				offset++;
				if (blockSize > length - offset) {
					break;
				}
				adler.updateRolling(array);
			}
		}

		if (length > lastRefFound) {
			result.add(new Instruction(new DataBuffer(array, lastRefFound, length - lastRefFound)));
		}

		return result;
	}

	/**
	 * It reconstructs the copy of responsible peer's value using instructions
	 * and the replica's value.
	 * 
	 * @param value
	 *            The value at replica
	 * @param instructions
	 *            The sequence of instructions
	 * @param blockSize
	 *            The offset size
	 * @return The value which is identical to the responsible peer's value
	 */
	public static DataBuffer reconstruct(byte[] value, List<Instruction> instructions, int blockSize) {
		DataBuffer result = new DataBuffer();
		for (Instruction instruction : instructions) {
			int ref = instruction.reference();
			if (ref != -1) {
				int offset = blockSize * ref;
				int remaining = Math.min(blockSize, value.length - offset);
				result.add(new DataBuffer(value, offset, remaining));
			} else {
				result.add(instruction.literal());
			}
		}
		return result;
	}
	
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
	public static class RollingChecksum {

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
}
