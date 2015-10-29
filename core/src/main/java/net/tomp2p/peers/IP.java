package net.tomp2p.peers;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public abstract class IP {
	public static class IPv4 extends IP {
		private final int bits;

		private IPv4(final int bits) {
			this.bits = bits;
		}

		/**
		 * 192.168.1.2 with netmask /24 results in 192.168.1.0
		 */
		public IPv4 maskWithNetworkMask(final int networkMask) {
			if (networkMask == 32) {
				return this;
			} else if(networkMask == 0) { 
				return new IPv4(0);
			}
			else {
				final int mask = (0xFFFFFFFF << (32 - networkMask));
				return new IPv4(bits & mask);
			}
		}
		
		/**
		 * 192.168.1.2 with netmask /24 results in 0.0.0.2
		 */
		public IPv4 maskWithNetworkMaskInv(final int networkMask) {
			if (networkMask == 0) {
				return this;
			} else {
				return new IPv4(bits & (0xFFFFFFFF >>> networkMask));
			}
		}
		
		public IPv4 set(final IPv4 maskedAddress) {
			final int newBits = bits | maskedAddress.bits;
			return new IPv4(newBits);
		}
		
		public InetAddress toInetAddress() {
			final byte[] ip = new byte[4];
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
		
		public int toInt() {
			return bits;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof IPv4)) {
				return false;
			}
			final IPv4 o = (IPv4) obj;
			return bits == o.bits;
		}
		
		@Override
		public int hashCode() {
			return bits;
		}
		
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("/");
			sb.append((byte) (bits >>> 24));
			sb.append('.');
			sb.append((byte) (bits >>> 16));
			sb.append('.');
			sb.append((byte) (bits >>> 8));
			sb.append('.');
			sb.append((byte) bits);
			return sb.toString();
		}
	}
	
	

	public static class IPv6 extends IP {
		private final long highBits;
		private final long lowBits;

		private IPv6(final long highBits, final long lowBits) {
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
		
		public InetAddress toInetAddress() {
			final byte[] ip = new byte[16];
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
			try {
				return Inet6Address.getByAddress(ip);
			} catch (UnknownHostException e) {
				//we know the size, so this should not throw an exception
				e.printStackTrace();
				return null;
			}
		}
		
		public long toLongHi() {
			return highBits;
		}
		
		public long toLongLo() {
			return lowBits;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof IPv6)) {
				return false;
			}
			final IPv6 o = (IPv6) obj;
			return highBits == o.highBits && lowBits == o.lowBits;
		}
		
		@Override
		public int hashCode() {
			return (int)((highBits^(highBits>>>32)) ^ (lowBits^(lowBits>>>32)));
		}
		
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("/");
			sb.append(String.format("%04X ", (short) (highBits >>> 48)));
			sb.append(':');
			sb.append(String.format("%04X ", (short) (highBits >>> 32)));
			sb.append(':');
			sb.append(String.format("%04X ", (short) (highBits >>> 16)));
			sb.append(':');
			sb.append(String.format("%04X ", (short) highBits));
			sb.append(':');
			sb.append(String.format("%04X ", (short) (lowBits >>> 48)));
			sb.append(':');
			sb.append(String.format("%04X ", (short) (lowBits >>> 32)));
			sb.append(':');
			sb.append(String.format("%04X ", (short) (lowBits >>> 16)));
			sb.append(':');
			sb.append(String.format("%04X ", (short) lowBits));
			return sb.toString();
		}
	}
	
	public abstract InetAddress toInetAddress();
	
	public static IPv4 fromInt(final int bits) {
		return new IPv4(bits);
	}
	
	public static IPv4 fromInet4Address(final InetAddress inetAddress) {
		if (inetAddress == null) {
			throw new IllegalArgumentException("Cannot construct from null.");
		} else if (!(inetAddress instanceof Inet4Address)) {
			throw new IllegalArgumentException("Must be IPv4.");
		}
		final byte[] buf = ((Inet4Address) inetAddress).getAddress();

		final int ip = ((buf[0] & 0xFF) << 24) | ((buf[1] & 0xFF) << 16) | ((buf[2] & 0xFF) << 8)
		        | ((buf[3] & 0xFF));
		return new IPv4(ip);
	}
	
	public static IPv6 fromLong(long highBits, long lowBits) {
		return new IPv6(highBits, lowBits);
	}
	
	public static IPv6 fromInet6Address(final InetAddress inetAddress) {
		if (inetAddress == null) {
			throw new IllegalArgumentException("Cannot construct from null.");
		} else if (!(inetAddress instanceof Inet6Address)) {
			throw new IllegalArgumentException("Must be IPv6.");
		}
		final byte[] buf = ((Inet6Address) inetAddress).getAddress();

		final long highBits = ((buf[0] & 0xFFL) << 56) | ((buf[1] & 0xFFL) << 48) | ((buf[2] & 0xFFL) << 40)
		        | ((buf[3] & 0xFFL) << 32) | ((buf[4] & 0xFFL) << 24) | ((buf[5] & 0xFFL) << 16)
		        | ((buf[6] & 0xFFL) << 8) | ((buf[7] & 0xFFL));

		final long lowBits = ((buf[8] & 0xFFL) << 56) | ((buf[9] & 0xFFL) << 48) | ((buf[10] & 0xFFL) << 40)
		        | ((buf[11] & 0xFFL) << 32) | ((buf[12] & 0xFFL) << 24) | ((buf[13] & 0xFFL) << 16)
		        | ((buf[14] & 0xFFL) << 8) | ((buf[15] & 0xFFL));

		return new IPv6(highBits, lowBits);
	}
}
