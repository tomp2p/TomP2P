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
			IP.IPv4 ipv4 = IP.fromInet4Address(peerAddress.inetAddress());
			for (PeerAddress inMap : all) {
				if (inMap.inetAddress() instanceof Inet4Address) {
					IP.IPv4 ipv4Test = IP.fromInet4Address(inMap.inetAddress());
					if (ipv4.maskWithNetworkMask(mask4).equals(ipv4Test.maskWithNetworkMask(mask4))) {
						return true;
					}
				}
			}
		} else {
			IP.IPv6 ipv6 = IP.fromInet6Address( peerAddress.inetAddress());
			for (PeerAddress inMap : all) {
				if (inMap.inetAddress() instanceof Inet6Address) {
					IP.IPv6 ipv6Test = IP.fromInet6Address(inMap.inetAddress());
					if (ipv6.maskWithNetworkMask(mask6).equals(ipv6Test.maskWithNetworkMask(mask6))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	
}
