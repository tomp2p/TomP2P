package net.tomp2p.peers;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IP {
	public static class IPv4 {
		private final int bits;

		private IPv4(int bits) {
			this.bits = bits;
		}

		/**
		 * 192.168.1.2 with netmask /24 results in 192.168.1.0
		 */
		public IPv4 maskWithNetworkMask(final int networkMask) {
			if (networkMask == 32) {
				return this;
			} else {
				return new IPv4(bits & (0xFFFFFFFF << (32 - networkMask)));
			}
		}
		
		/**
		 * 192.168.1.2 with netmask /24 results in 0.0.0.2
		 */
		public IPv4 maskWithNetworkMaskInv(final int networkMask) {
			if (networkMask == 32) {
				return this;
			} else {
				return new IPv4(bits & (0xFFFFFFFF >>> networkMask));
			}
		}
		
		public IPv4 set(IPv4 maskedAddress) {
			int newBits = bits | maskedAddress.bits;
			return new IPv4(newBits);
		}
		
		public InetAddress toInetAddress() {
			byte[] ip = new byte[4];
			ip[0] = (byte) (bits >>> 24);
			ip[1] = (byte) (bits >>> 16);
			ip[2] = (byte) (bits >>> 8);
			ip[3] = (byte) (bits);
			try {
				return Inet4Address.getByAddress(ip);
			} catch (UnknownHostException e) {
				//we know the size, so this should not throw an exception
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof IPv4)) {
				return false;
			}
			IPv4 o = (IPv4) obj;
			return bits == o.bits;
		}
		
		@Override
		public int hashCode() {
			return bits;
		}
	}
	
	

	public static class IPv6 {
		private final long highBits;
		private final long lowBits;

		private IPv6(long highBits, long lowBits) {
			this.highBits = highBits;
			this.lowBits = lowBits;
		}

		public IPv6 maskWithNetworkMask(final int networkMask) {
			if (networkMask == 128) {
				return this;
			} else if (networkMask == 64) {
				return new IPv6(highBits, 0);
			} else if (networkMask > 64) {
				final int remainingPrefixLength = networkMask - 64;
				return new IPv6(highBits, lowBits & (0xFFFFFFFFFFFFFFFFL << (64 - remainingPrefixLength)));
			} else {
				return new IPv6(highBits & (0xFFFFFFFFFFFFFFFFL << (64 - networkMask)), 0);
			}
		}
		
		public InetAddress toInetAddress() throws UnknownHostException {
			byte[] ip = new byte[16];
			ip[0]  = (byte) (highBits >>> 56);
			ip[1]  = (byte) (highBits >>> 48);
			ip[2]  = (byte) (highBits >>> 40);
			ip[3]  = (byte) (highBits >>> 32);
			ip[4]  = (byte) (highBits >>> 24);
			ip[5]  = (byte) (highBits >>> 16);
			ip[6]  = (byte) (highBits >>> 8);
			ip[7]  = (byte) (highBits);
			ip[8]  = (byte) (lowBits >>> 56);
			ip[9]  = (byte) (lowBits >>> 48);
			ip[10] = (byte) (lowBits >>> 40);
			ip[11] = (byte) (lowBits >>> 32);
			ip[12] = (byte) (lowBits >>> 24);
			ip[13] = (byte) (lowBits >>> 16);
			ip[14] = (byte) (lowBits >>> 8);
			ip[15] = (byte) (lowBits);
			return Inet4Address.getByAddress(ip);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof IPv6)) {
				return false;
			}
			IPv6 o = (IPv6) obj;
			return highBits == o.highBits && lowBits == o.lowBits;
		}
		
		@Override
		public int hashCode() {
			return (int)((highBits^(highBits>>>32)) ^ (lowBits^(lowBits>>>32)));
		}
	}
	
	public static IPv4 fromInet4Address(final InetAddress inetAddress) {
		if (inetAddress == null) {
			throw new IllegalArgumentException("Cannot construct from null.");
		} else if (!(inetAddress instanceof Inet4Address)) {
			throw new IllegalArgumentException("Must be IPv4.");
		}
		byte[] buf = ((Inet4Address) inetAddress).getAddress();

		int ip = ((buf[0] & 0xFF) << 24) | ((buf[1] & 0xFF) << 16) | ((buf[2] & 0xFF) << 8)
		        | ((buf[3] & 0xFF) << 0);
		return new IPv4(ip);
	}
	
	public static IPv6 fromInet6Address(final InetAddress inetAddress) {
		if (inetAddress == null) {
			throw new IllegalArgumentException("Cannot construct from null.");
		} else if (!(inetAddress instanceof Inet6Address)) {
			throw new IllegalArgumentException("Must be IPv6.");
		}
		byte[] buf = ((Inet6Address) inetAddress).getAddress();

		long highBits = ((buf[0] & 0xFFL) << 56) | ((buf[1] & 0xFFL) << 48) | ((buf[2] & 0xFFL) << 40)
		        | ((buf[3] & 0xFFL) << 32) | ((buf[4] & 0xFFL) << 24) | ((buf[5] & 0xFFL) << 16)
		        | ((buf[6] & 0xFFL) << 8) | ((buf[7] & 0xFFL) << 0);

		long lowBits = ((buf[8] & 0xFFL) << 56) | ((buf[9] & 0xFFL) << 48) | ((buf[10] & 0xFFL) << 40)
		        | ((buf[11] & 0xFFL) << 32) | ((buf[12] & 0xFFL) << 24) | ((buf[13] & 0xFFL) << 16)
		        | ((buf[14] & 0xFFL) << 8) | ((buf[15] & 0xFFL) << 0);

		return new IPv6(highBits, lowBits);
	}
}
