/*
 * Copyright 2013 Thomas Bocek
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

package net.tomp2p.peers;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Collection;

/**
 * Filter peers if the IP is the same. TODO: make subnetting configurable, IPv4
 * and IPv6. Beeing too strict does not mean to harm the network. Other peers
 * will have the information about the peer even if you excluded it.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerIPFilter implements PeerMapFilter {

	final int mask4;
	final int mask6;

	public PeerIPFilter(int mask4, int mask6) {
		this.mask4 = mask4;
		this.mask6 = mask6;
	}

	@Override
	public boolean rejectPeerMap(final PeerAddress peerAddress, final PeerMap peerMap) {
		return rejectPreRouting(peerAddress, peerMap.all());
	}
	
	@Override
    public boolean rejectPreRouting(PeerAddress peerAddress, Collection<PeerAddress> all) {
		if (peerAddress.inetAddress() instanceof Inet4Address) {
			IPv4 ipv4 = IPv4.fromInetAddress(peerAddress.inetAddress());
			for (PeerAddress inMap : all) {
				if (inMap.inetAddress() instanceof Inet4Address) {
					IPv4 ipv4Test = IPv4.fromInetAddress(inMap.inetAddress());
					if (ipv4.maskWithNetworkMask(mask4).equals(ipv4Test.maskWithNetworkMask(mask4))) {
						return true;
					}
				}
			}
		} else {
			IPv6 ipv6 = IPv6.fromInetAddress(peerAddress.inetAddress());
			for (PeerAddress inMap : all) {
				if (inMap.inetAddress() instanceof Inet6Address) {
					IPv6 ipv6Test = IPv6.fromInetAddress(inMap.inetAddress());
					if (ipv6.maskWithNetworkMask(mask6).equals(ipv6Test.maskWithNetworkMask(mask6))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private static class IPv4 {
		private final int bits;

		private IPv4(int bits) {
			this.bits = bits;
		}

		public IPv4 maskWithNetworkMask(final int networkMask) {
			if (networkMask == 32) {
				return this;
			} else {
				return new IPv4(bits & (0xFFFFFFFF << (32 - networkMask)));
			}
		}

		public static IPv4 fromInetAddress(final InetAddress inetAddress) {
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

	private static class IPv6 {
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

		public static IPv6 fromInetAddress(final InetAddress inetAddress) {
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
			return (int)(highBits ^ lowBits);
		}
	}
}
