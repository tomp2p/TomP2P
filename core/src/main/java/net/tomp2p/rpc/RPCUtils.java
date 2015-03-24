package net.tomp2p.rpc;

import java.util.BitSet;

public class RPCUtils {
	// 1.7 version
	// public static byte[] toByteArray(BitSet bitSet) {
	// return bitSet.toByteArray();
	// }

	// 1.6 version
	public static byte[] toByteArray(BitSet bitSet) {
		byte[] bytes = new byte[(bitSet.length() + 7) / 8];
		for (int i = 0; i < bitSet.length(); i++) {
			if (bitSet.get(i)) {
				//big endian
				//bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
				//litle endian
				bytes[i/8] |= 1 << (7 - i % 8);
			}
		}
		return bytes;
	}

	// 1.7 version
	// public static BitSet fromByteArray(byte[] bytes) {
	// return BitSet.valueOf(bytes);
	// }

	// 1.6 version
	public static BitSet fromByteArray(byte[] bytes) {
		BitSet bits = new BitSet();
		for (int i = 0; i < bytes.length * 8; i++) {
			//big endian
			//if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
			//litle endian
			if ((bytes[i/8] & (1 << (7 - i % 8))) > 0) {
				bits.set(i);
			}
		}
		return bits;
	}
}
