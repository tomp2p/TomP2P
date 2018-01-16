/*
 * Copyright 2009 Thomas Bocek
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

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;

import io.netty.buffer.ByteBuf;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.Wither;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 * A PeerAddress contains the node ID and how to contact this node using both TCP and UDP. This class is thread safe (or
 * it does not matter if its not). The format looks as follows:
 * 
 * <pre>
 * 20 bytes - Number160
 * 2 bytes - Header 
 *  - 1 byte options: IPv6, firewalled UDP, is relayed
 *  - 1 byte relays:
 *    - first 3 bits: number of relays (max 5.)
 *    - second 5 bits: if the 5 relays are IPv6 (bit set) or not (no bit set)
 * 2 bytes - UDP port
 * 4 or 16 bytes - Inet Address
 * 0-5 relays:
 *  - 2 bytes - TCP port
 *  - 2 bytes - UDP port
 *  - 4 or 16 bytes - Inet Address
 * </pre>
 * 
 * @author Thomas Bocek
 */

@Builder
@RequiredArgsConstructor
@Accessors(fluent = true, chain = true)
public final class PeerAddress implements Comparable<PeerAddress>, Serializable {
	
	private static final long serialVersionUID = 8483270473601620720L;
	
	public static final int HEADER_SIZE = 2;
	public static final int MIN_SIZE = HEADER_SIZE + Number160.BYTE_ARRAY_SIZE; //22
	public static final int MIN_SIZE_HEADER = MIN_SIZE + PeerSocketAddress.PORT_SIZE; //24
    public static final int MAX_SIZE = MIN_SIZE + (PeerSocket4Address.SIZE + PeerSocket6Address.SIZE) + (7 * PeerSocket6Address.SIZE); //172
    
    //1 Byte - 8 bits
    private static final int IPV4 = 0x1;					// xxxxxxxx xxxxxxx1 //peeraddress can have both, IPv4 and IPv6
    private static final int IPV6 = 0x2;					// xxxxxxxx xxxxxx1x
    														// xxxxxxxx xxxxxx00 // no IP

    private static final int REACHABLE4_UDP = 0x4;			// xxxxxxxx xxxxx1xx
    private static final int REACHABLE6_UDP = 0x8;			// xxxxxxxx xxxx1xxx
    														// xxxxxxxx xxxx00xx - unreachable
    
    private static final int SKIP_IP = 0x10;				// xxxxxxxx xxx1xxxx
    
    private static final int RELAY_SIZE_MASK = 0xe0;		// xxxxxxxx 111xxxxx
    
    //next 1 Byte - 8 bits - we can have at most 7 relays
    
    private static final int PORT_PRESERVIVG = 0x100;			// xxxxxxx1 xxxxxxxx
    private static final int RELAY_TYPE_MASK = 0xfe00;		// 1111111x xxxxxxxx
    

    @Getter @Wither private final int ipInternalNetworkPrefix;
    //
    @Getter @Wither private final PeerSocket4Address ipv4Socket;
    @Getter @Wither private final PeerSocket6Address ipv6Socket;
    
    @Getter @Wither private final boolean ipv4Flag;
    @Getter @Wither private final boolean ipv6Flag;
    
    //@Getter @Wither private final boolean net6Flag; // unused
    @Getter @Wither private final boolean reachable4UDP;
    @Getter @Wither private final boolean reachable6UDP;
    
    // network information
    @Getter @Wither private final Number160 peerId;
    // connection information
    @Getter @Wither private final int relaySize;
    @Getter @Wither private final boolean portPreserving;
    @Getter @Wither private final boolean skipIP;
    
    @Wither private final BitSet relayTypes;

    // we can make the hash final as it never changes, and this class is used
    // multiple times in maps. A new peer is always added to the peermap, so
    // making this final saves CPU time. The maps used are removedPeerCache,
    // that is always checked, peerMap that is either added or checked if
    // already present. Also peers from the neighbor list sent over the wire are
    // added to the peermap.
    //private final int hashCode;

    //relays
    @Wither(value=AccessLevel.PRIVATE) private final Collection<PeerSocketAddress> relays0;
    public static final Collection<PeerSocketAddress> EMPTY_PEER_SOCKET_ADDRESSES = Collections.emptySet();
    public static final byte EMPTY_RELAY_TYPES = 0;
    
    public static final boolean preferIPv6Addresses;
    
    static {
    	preferIPv6Addresses = "true".equalsIgnoreCase(System.getProperty("java.net.preferIPv6Addresses"));
    }
    
    //Change lomboks default behavior
	public static class PeerAddressBuilder {
		public PeerAddressBuilder relay(PeerSocketAddress relay) {
			if(relaySize == 7) {
				throw new IllegalArgumentException("cannot have more than 7 relays");
			}
			if (this.relays0 == null) {
				this.relays0 = new ArrayList<PeerSocketAddress>(1);
			}
			if(this.relayTypes == null) {
				this.relayTypes = new BitSet(8);
			}
			if(relay instanceof PeerSocket4Address) {
				this.relayTypes.set(this.relaySize++, false);
			}
			else {
				this.relayTypes.set(this.relaySize++, true);
			}
			this.relays0.add(relay);
			return this;
		}
		
		private PeerAddressBuilder relay(PeerSocketAddress relay, int index) {
			if(relaySize > 7) {
				throw new IllegalArgumentException("cannot have more than 7 relays");
			}
			if (this.relays0 == null) {
				this.relays0 = new ArrayList<PeerSocketAddress>(relaySize);
			}
			if(this.relayTypes == null) {
				this.relayTypes = new BitSet(8);
			}
			if(relay instanceof PeerSocket4Address) {
				this.relayTypes.set(index, false);
			}
			else {
				this.relayTypes.set(index, true);
			}
			this.relays0.add(relay);
			return this;
		}

		public PeerAddressBuilder relay(Collection<? extends PeerSocketAddress> relays) {
			if (this.relays0 == null) {
				this.relays0 = new ArrayList<PeerSocketAddress>(relays.size());
			}

			for (PeerSocketAddress relay : relays) {
				relay(relay);
			}
			return this;
		}
		
		public PeerAddressBuilder ipv4Socket(PeerSocket4Address ipv4Socket) {
			this.ipv4Flag = true;
			this.ipv4Socket = ipv4Socket;
			return this;
		}
		
		public PeerAddressBuilder ipv6Socket(PeerSocket6Address ipv6Socket) {
			this.ipv6Flag = true;
			this.ipv6Socket = ipv6Socket;
			return this;
		}
	}
	
	//@Override
	public PeerAddress withRelays(Collection<PeerSocketAddress> relays) {
		if(this.relays0 == relays) {
			return this;
		}
		
		final int relaySize;
		final BitSet relayTypes = new BitSet(8);
		if(relays != null) {
			relaySize = relays.size();
			int index=0;
			for(PeerSocketAddress relay: relays) {
				if(relay instanceof PeerSocket4Address) {
					relayTypes.set(index++, false);
				}
				else {
					relayTypes.set(index++, true);
				}
			}
		} else {
			relaySize = 0;
		}

		return withRelays0(relays).withRelaySize(relaySize).withRelayTypes(relayTypes);
	  }
	
	
	
	public Collection<PeerSocketAddress> relays() {
		if(relays0 == null) {
			return EMPTY_PEER_SOCKET_ADDRESSES;
		}
		return relays0;
	}
	
	public byte relayTypes() {
		if(relayTypes == null) {
			return EMPTY_RELAY_TYPES;
		}
		byte[] tmp = relayTypes.toByteArray();
		return tmp.length != 1 ? EMPTY_RELAY_TYPES : tmp[0];
	}

    /**
     * Creates a peer address, where the byte array has to be in the rigth format and in the right size. The new
     * offset can be accessed with offset().
     *
     */
    public static Pair<PeerAddress, Integer> decode(final byte[] array) {
    	return decode(array, 0);
    }

    public static Pair<PeerAddress, Integer> decode(final byte[] array, int offset) {
    	final int header = Utils.byteArrayToShort(array, offset);
		offset+=HEADER_SIZE;
		return decode(header, array, offset);
    }
    public static Pair<PeerAddress, Integer> decode(final int header, final byte[] array, int offset) {
    	final PeerAddressBuilder builder = new PeerAddressBuilder();
    	
    	decodeHeader(builder, header);
			
		//relays
    	for(int i = 0; i<builder.relaySize; i++) {
    		final Pair<? extends PeerSocketAddress, Integer> pair;
			if(builder.relayTypes.get(i)) {
				builder.relay((pair = PeerSocket6Address.decode(array, offset)).element0(), i);
			} else {
				builder.relay((pair = PeerSocket4Address.decode(array, offset)).element0(), i);
			}
			offset = pair.element1();
		}
    	
    	if(builder.ipv4Flag) {
    		final Pair<PeerSocket4Address, Integer> pair;
			builder.ipv4Socket((pair = PeerSocket4Address.decode(array, offset, builder.skipIP)).element0());
			offset = pair.element1();
		}
		if(builder.ipv6Flag) {
			final Pair<PeerSocket6Address, Integer> pair;
			builder.ipv6Socket((pair = PeerSocket6Address.decode(array, offset, builder.skipIP)).element0());
			offset = pair.element1();
		}
    	
		final Pair<Number160, Integer> pair;
		final PeerAddress peerAddress = builder.peerId((pair = Number160.decode(array, offset)).element0())
			.build();
		return new Pair<PeerAddress, Integer>(peerAddress, pair.element1());
    }

    public static PeerAddress decode(final ByteBuf buf) {
    	final int header = buf.readUnsignedShort();
    	return decode(header, buf);
    }
    public static PeerAddress decode(final int header, final ByteBuf buf) {
    	final PeerAddressBuilder builder = new PeerAddressBuilder();
		decodeHeader(builder, header);
		
		//relays
		for(int i=0;i<builder.relaySize;i++) {
			if(builder.relayTypes.get(i)) {
				builder.relay(PeerSocket6Address.decode(buf), i);
			} else {
				builder.relay(PeerSocket4Address.decode(buf), i);
			}
		}
		if(builder.ipv4Flag) {
			builder.ipv4Socket(PeerSocket4Address.decode(buf, builder.skipIP));
		}
		if(builder.ipv6Flag) {
			builder.ipv6Socket(PeerSocket6Address.decode(buf, builder.skipIP));
		}
		return builder
				.peerId(Number160.decode(buf))
				.build();
    }

    //we have a 2 byte header
    private static PeerAddressBuilder decodeHeader(final PeerAddressBuilder builder, final int header) {
    	final byte[] tmp = new byte[]{(byte) ((header & RELAY_TYPE_MASK) >>> 9)};
    	builder.ipv4Flag((header & IPV4) == IPV4)
			.ipv6Flag((header & IPV6) == IPV6)
			.reachable4UDP((header & REACHABLE4_UDP) == REACHABLE4_UDP)
    		.reachable6UDP((header & REACHABLE6_UDP) == REACHABLE6_UDP)
    		.relaySize((header & RELAY_SIZE_MASK) >>> 5)
			.portPreserving((header & PORT_PRESERVIVG) == PORT_PRESERVIVG)
			.skipIP((header & SKIP_IP) == SKIP_IP)
			.relayTypes(BitSet.valueOf(tmp));
		return builder;
	}

    /**
     * @return The size of the serialized peer address, calculated using header information only
     */
    public int size() {
        int size = HEADER_SIZE + Number160.BYTE_ARRAY_SIZE;
        for(int i=0;i<relaySize;i++) {
			size += relayTypes.get(i) ? PeerSocket6Address.SIZE : PeerSocket4Address.SIZE;
		}
        if(ipv4Flag) {
        	size += skipIP ? PeerSocketAddress.PORT_SIZE : PeerSocket4Address.SIZE;
		}
		if(ipv6Flag) {
			size += skipIP ? PeerSocketAddress.PORT_SIZE : PeerSocket6Address.SIZE;
		}
        return size;
    }
    
    public static int size(final int header) {
    	final PeerAddressBuilder builder = new PeerAddressBuilder();
		return decodeHeader(builder, header).build().size();
    }

    /**
     * Serializes to a new array with the proper size.
     * 
     * @return The serialized representation.
     */
    public byte[] encode() {
        final byte[] retVal  = new byte[size()];
        encode(retVal, 0);
        return retVal;
    }

    /**
     * Serializes to an existing array.
     *
     * @param offset
     *            The offset where to start to save the result in the byte array
     * @return The new offset.
     */
    public int encode(final byte[] array, int offset) {
    	offset = Utils.shortToByteArray(encodeHeader(), array, offset);
    	
    	for(final PeerSocketAddress relay:relays()) {
    		offset = relay.encode(array, offset);
    	}
    	
    	if(ipv4Flag) {
    		offset = ipv4Socket.encode(array, offset, skipIP);
		}
		if(ipv6Flag) {
			offset = ipv6Socket.encode(array, offset, skipIP);
		}
    	
    	offset = peerId.encode(array, offset);
        return offset;
    }
    
    public PeerAddress encode(final ByteBuf buf) {
    	final int header = encodeHeader() ;
    	buf.writeShort(header);
    	
    	for(final PeerSocketAddress relay:relays()) {
    		relay.encode(buf);
    	}
    	
    	if(ipv4Flag) {
    		ipv4Socket.encode(buf, skipIP);
		}
		if(ipv6Flag) {
			ipv6Socket.encode(buf, skipIP);
		}
    	
    	peerId.encode(buf);
        return this;
    }
    
    private int encodeHeader() {
    	return    (ipv4Flag ? IPV4 : 0)
    			| (ipv6Flag ? IPV6 : 0)
				| (reachable4UDP ? REACHABLE4_UDP : 0)
				| (reachable6UDP ? REACHABLE6_UDP : 0)
				| ((relaySize << 5) & RELAY_SIZE_MASK)
				| (portPreserving ? PORT_PRESERVIVG : 0)
				| (skipIP ? SKIP_IP : 0)
				| ((relayTypes() << 9) & RELAY_TYPE_MASK);
	}
    


    @Override
    public String toString() {
		StringBuilder sb = new StringBuilder("paddr[");
		
		if(ipv4Flag) {
			sb.append("4:");
			sb.append(ipv4Socket);
    		if(!reachable4UDP) {
    			sb.append('!');
    		}
		}
		if(ipv6Flag) {
			sb.append("6:");
			sb.append(ipv6Socket);
			if(!reachable6UDP) {
    			sb.append('!');
    		}
		}
		sb.append('{');
		if(portPreserving) {
			sb.append('*');
		}
		if(skipIP) {
			sb.append('s');
		}
		sb.append('}');
		
		sb.append('r').append(relaySize).append('(');
		for(final PeerSocketAddress relay:relays()) {
			sb.append(relay);
    	}
		sb.append(")-");
    	
		sb.append(peerId);
    	
		return sb.append(']').toString();
    }

    @Override
    public int compareTo(final PeerAddress nodeAddress) {
        // the id determines if two peers are equal, the address does not matter
        return peerId.compareTo(nodeAddress.peerId);
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof PeerAddress)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        PeerAddress pa = (PeerAddress) obj;
        return peerId.equals(pa.peerId);
    }

    @Override
    public int hashCode() {
        return peerId.hashCode();
    }

	public PeerAddress withIPSocket(PeerSocketAddress ps) {
		if(ps instanceof PeerSocket4Address) {
			return this.withIpv4Socket((PeerSocket4Address) ps);
		} else {
			return this.withIpv6Socket((PeerSocket6Address) ps);
		}
	}
	
	public static PeerAddress create(final Number160 peerId) {
		return builder().peerId(peerId).build();
	}
	
	public static PeerAddress create(final Number160 peerId, String host, int udpPort) throws UnknownHostException {
		return create(peerId, InetAddress.getByName(host), udpPort);
	}
	
	
	public static PeerAddress create(final Number160 peerId, InetSocketAddress inetSocket) {
		return create(peerId, inetSocket.getAddress(), inetSocket.getPort()); 
	}
	
	public static PeerAddress create(final Number160 peerId, InetAddress inet, int udpPort) {
		if(inet instanceof Inet4Address) {
			final PeerSocket4Address ps4a = PeerSocket4Address.builder()
					.ipv4(IP.fromInet4Address((Inet4Address)inet))
					.udpPort(udpPort)
					.build();
			return builder()
					.peerId(peerId)
					.ipv4Socket(ps4a)
					.build();
		} else {
			final PeerSocket6Address ps6a = PeerSocket6Address.builder()
					.ipv6(IP.fromInet6Address((Inet6Address)inet))
					.udpPort(udpPort)
					.build();
			return builder()
					.peerId(peerId)
					.ipv6Socket(ps6a)
					.build();
		}
	}

	public InetSocketAddress createUDPSocket(final PeerAddress other) {
		return createUDPSocket(other, false);
	}

	public InetSocketAddress createUDPSocket(final PeerAddress other, boolean forceUnreachable) {
		
		final boolean canIPv6 = ipv6Flag && other.ipv6Flag;
		final boolean canIPv4 = ipv4Flag && other.ipv4Flag;
		if((preferIPv6Addresses && canIPv6) || 
				(!canIPv4 && canIPv6)) {
			if(ipv6Socket == null) {
				throw new RuntimeException("Flag indicates that ipv6 is present, but its not");
			}
			//check if reachable
			if(reachable6UDP || forceUnreachable) {
				return ipv6Socket.createUDPSocket();
			} else if(relaySize > 0) {
				for(PeerSocketAddress relay:relays()) {
					if(relay instanceof PeerSocket6Address) {
						return ((PeerSocket6Address) relay).createUDPSocket();
					}
				}
				for(PeerSocketAddress relay:relays()) {
					if(relay instanceof PeerSocket4Address) {
						return ((PeerSocket4Address) relay).createUDPSocket();
					}
				}
				return null; //unreachable and no relay defined, cannot contact!
			} else {
				return null; //unreachable and no relay defined, cannot contact!
			}
		} else if(canIPv4) {
			if(ipv4Socket == null) {
				throw new RuntimeException("Flag indicates that ipv4 is present, but its not");
			}
			//check if reachable
			if(reachable4UDP || forceUnreachable) {
				return ipv4Socket.createUDPSocket();
			} else if(relaySize > 0) {
				for(PeerSocketAddress relay:relays()) {
					if(relay instanceof PeerSocket4Address) {
						return ((PeerSocket4Address) relay).createUDPSocket();
					}
				}
				for(PeerSocketAddress relay:relays()) {
					if(relay instanceof PeerSocket6Address) {
						return ((PeerSocket6Address) relay).createUDPSocket();
					}
				}
				return null; //unreachable and no relay defined, cannot contact!
			} else {
				return null; //unreachable and no relay defined, cannot contact!
			}
		}
		else {
			throw new RuntimeException("No matching protocal found");
		}
	}

	public InetSocketAddress createSocket(final PeerAddress other, final int port) {
		final boolean canIPv6 = ipv6Flag && other.ipv6Flag;
		final boolean canIPv4 = ipv4Flag && other.ipv4Flag;
		if((preferIPv6Addresses && canIPv6) || 
				(!canIPv4 && canIPv6)) {
			if(ipv6Socket == null) {
				throw new RuntimeException("Flag indicates that ipv6 is present, but its not");
			}
			return ipv6Socket.createSocket(port);
		} else if(canIPv4) {
			if(ipv4Socket == null) {
				throw new RuntimeException("Flag indicates that ipv4 is present, but its not");
			}
			return ipv4Socket.createSocket(port);
		}
		else {
			throw new RuntimeException("No matching protocal found");
		}
	}
	
	/*private int prefix4(InetAddress inetAddress) {
		NetworkInterface networkInterface;
		try {
			networkInterface = NetworkInterface.getByInetAddress(internalPeerSocketAddress.inetAddress());
			if(networkInterface == null) {
				return -1;
			}
			if(networkInterface.getInterfaceAddresses().size() <= 0) {
				return -1;
			}
			for(InterfaceAddress iface: networkInterface.getInterfaceAddresses()) {
				if(iface == null) {
					continue;
				}
				if(iface.getAddress() instanceof Inet4Address) {
					return iface.getNetworkPrefixLength();
				}
			}
			return -1;
		} catch (SocketException e) {
			e.printStackTrace();
			return -1;
		}
	}*/
	
	/*public PeerSocketAddress strip() {
		if(internalPeerSocketAddress == null) {
			throw new IllegalArgumentException("internal peer socket must be set");
		}
		if(internalNetworkPrefix == -1) {
			return internalPeerSocketAddress;
		}
		IP.IPv4 masked = IP.fromInet4Address(internalPeerSocketAddress.inetAddress());
		masked = masked.maskWithNetworkMaskInv(internalNetworkPrefix);
		final InetAddress inet = masked.toInetAddress();
		return new PeerSocketAddress(inet, internalPeerSocketAddress.tcpPort(), internalPeerSocketAddress.udpPort());
	}*/
	
	
}
