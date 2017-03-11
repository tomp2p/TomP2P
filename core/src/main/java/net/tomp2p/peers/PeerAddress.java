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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Wither;
import net.tomp2p.peers.IP.IPv4;
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
 *  - 1 byte options: IPv6, firewalled UDP, firewalled TCP, is relayed
 *  - 1 byte relays:
 *    - first 3 bits: number of relays (max 5.)
 *    - second 5 bits: if the 5 relays are IPv6 (bit set) or not (no bit set)
 * 2 bytes - TCP port
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
@Accessors(fluent = true, chain = true)
public final class PeerAddress implements Comparable<PeerAddress>, Serializable {
	
	private static final long serialVersionUID = 8483270473601620720L;
	
	public static final int HEADER_SIZE = 3;
	public static final int MIN_SIZE = HEADER_SIZE + Number160.BYTE_ARRAY_SIZE; //23
	public static final int MIN_SIZE_HEADER = MIN_SIZE + 6; //29
    public static final int MAX_SIZE = MIN_SIZE + (2 * PeerSocket4Address.SIZE) + (8 * PeerSocket6Address.SIZE); //219
    
    //1 Byte - 8 bits
    private static final int IPV4 = 0x1;					// xxxxxxxx xxxxxxxx xxxxxxx1
    private static final int IPV6 = 0x2;					// xxxxxxxx xxxxxxxx xxxxxx1x
    
    private static final int REACHABLE4_UDP = 0x4;			// xxxxxxxx xxxxxxxx xxxxx1xx
    private static final int REACHABLE4_TCP = 0x8;			// xxxxxxxx xxxxxxxx xxxx1xxx
    private static final int REACHABLE4_UDT = 0x10;			// xxxxxxxx xxxxxxxx xxx1xxxx
    
    private static final int REACHABLE6_UDP = 0x20;			// xxxxxxxx xxxxxxxx xx1xxxxx
    private static final int REACHABLE6_TCP = 0x40;			// xxxxxxxx xxxxxxxx x1xxxxxx
    private static final int REACHABLE6_UDT = 0x80;			// xxxxxxxx xxxxxxxx 1xxxxxxx
   
    //next 1 Byte - 8 bits
    private static final int RELAY_SIZE_MASK = 0x700;		// xxxxxxxx xxxxx111 xxxxxxxx
    // indicates that a relay is used. If its relayed it can be slow (buffered) or holepunching
    														// xxxxxxxx xxx00xxx xxxxxxxx - no flags
    private static final int HOLE_PUNCHING = 0x1000;		// xxxxxxxx xxx10xxx xxxxxxxx
    // if relayed, indicates that the peer is slow
    private static final int SLOW = 0x800;					// xxxxxxxx xxx01xxx xxxxxxxx
    // if relayed, indicate if HP is possible
    private static final int RELAY_FLAGS_MASK = 0x1800;		// xxxxxxxx xxx11xxx xxxxxxxx - unused
    private static final int UNREACHABLE = 0x2000;			// xxxxxxxx xx1xxxxx xxxxxxxx
    // indicate IPv4 with the internal IP
    private static final int NET4_INTERNAL = 0x4000;		// xxxxxxxx x1xxxxxx xxxxxxxx //hopefully we'll never use this for IPv6
    private static final int SKIP_IPV4 = 0x8000;			// xxxxxxxx 1xxxxxxx xxxxxxxx 
    private static final int SKIP_IPV6 = 0x10000;			// xxxxxxx1 xxxxxxxx xxxxxxxx 
    
    //next 1 Byte - 8 bits - we can have at most 7 relays
    private static final int RELAY_TYPE_MASK = 0xfe0000;	// 1111111x xxxxxxxx xxxxxxxx

    @Getter @Wither private final PeerSocket4Address ipInternalSocket;
    @Getter @Wither private final int ipInternalNetworkPrefix;
    //
    @Getter @Wither private final PeerSocket4Address ipv4Socket;
    @Getter @Wither private final PeerSocket6Address ipv6Socket;
    
    @Getter @Wither private final boolean ipv4Flag;
    @Getter @Wither private final boolean ipv6Flag;
    
    //@Getter @Wither private final boolean net6Flag; // unused
    @Getter @Wither private final boolean reachable4UDP;
    @Getter @Wither private final boolean reachable4TCP;
    @Getter @Wither private final boolean reachable4UDT;
    @Getter @Wither private final boolean reachable6UDP;
    @Getter @Wither private final boolean reachable6TCP;
    @Getter @Wither private final boolean reachable6UDT;
    
    // network information
    @Getter @Wither private final Number160 peerId;
    // connection information
    @Getter @Wither private final int relaySize;
    @Getter @Wither private final boolean slow;
    @Getter @Wither private final boolean holePunching;
    //@Getter @Wither private final boolean relayFlag; // unused
    @Getter @Wither private final boolean unreachable;
    @Getter @Wither private final boolean net4Internal;
    @Getter @Wither private final boolean skipIPv4;
    @Getter @Wither private final boolean skipIPv6;
    
    @Wither private final BitSet relayTypes;

    // we can make the hash final as it never changes, and this class is used
    // multiple times in maps. A new peer is always added to the peermap, so
    // making this final saves CPU time. The maps used are removedPeerCache,
    // that is always checked, peerMap that is either added or checked if
    // already present. Also peers from the neighbor list sent over the wire are
    // added to the peermap.
    private final int hashCode;

    //relays
    private final Collection<PeerSocketAddress> relays;
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
			if (this.relays == null) {
				this.relays = new ArrayList<PeerSocketAddress>(1);
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
			this.relays.add(relay);
			return this;
		}
		
		private PeerAddressBuilder relay(PeerSocketAddress relay, int index) {
			if(relaySize > 7) {
				throw new IllegalArgumentException("cannot have more than 7 relays");
			}
			if (this.relays == null) {
				this.relays = new ArrayList<PeerSocketAddress>(relaySize);
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
			this.relays.add(relay);
			return this;
		}

		public PeerAddressBuilder relay(Collection<? extends PeerSocketAddress> relays) {
			if (this.relays == null) {
				this.relays = new ArrayList<PeerSocketAddress>(relays.size());
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
		
		public PeerAddressBuilder ipInternalSocket(PeerSocket4Address ipInternalSocket) {
			this.net4Internal = true;
			this.ipInternalSocket = ipInternalSocket;
			return this;
		}
		
	}
	
	public PeerAddress withRelays(Collection<PeerSocketAddress> relays) {
		if(this.relays == relays) {
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
		
		
		return this.relays == relays ? this : new PeerAddress(ipInternalSocket, 
		    		 ipInternalNetworkPrefix, 
		    		 ipv4Socket, 
		    		 ipv6Socket, 
		    		 ipv4Flag, 
		    		 ipv6Flag, 
		    		 reachable4UDP, 
		    		 reachable4TCP, 
		    		 reachable4UDT, 
		    		 reachable6UDP, 
		    		 reachable6TCP, 
		    		 reachable6UDT, 
		    		 peerId, 
		    		 relaySize, 
		    		 slow, 
		    		 holePunching, 
		    		 unreachable, 
		    		 net4Internal, 
		    		 skipIPv4, 
		    		 skipIPv6, 
		    		 relayTypes, 
		    		 hashCode, 
		    		 relays);
	  }
	
	
	
	public Collection<PeerSocketAddress> relays() {
		if(relays == null) {
			return EMPTY_PEER_SOCKET_ADDRESSES;
		}
		return relays;
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
    	final int header = Utils.byteArrayToMedium(array, offset);
		offset+=3;
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
			builder.ipv4Socket((pair = PeerSocket4Address.decode(array, offset, builder.skipIPv4)).element0());
			offset = pair.element1();
		}
		if(builder.net4Internal) {
			final Pair<PeerSocket4Address, Integer> pair;
			builder.ipInternalSocket((pair = PeerSocket4Address.decode(array, offset)).element0());
			offset = pair.element1();
		}
		if(builder.ipv6Flag) {
			final Pair<PeerSocket6Address, Integer> pair;
			builder.ipv6Socket((pair = PeerSocket6Address.decode(array, offset, builder.skipIPv6)).element0());
			offset = pair.element1();
		}
    	
		final Pair<Number160, Integer> pair;
		final PeerAddress peerAddress = builder.peerId((pair = Number160.decode(array, offset)).element0())
			.hashCode(builder.peerId.hashCode())
			.build();
		return new Pair<PeerAddress, Integer>(peerAddress, pair.element1());
    }

    public static PeerAddress decode(final ByteBuf buf) {
    	final int header = buf.readUnsignedMedium();
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
			builder.ipv4Socket(PeerSocket4Address.decode(buf, builder.skipIPv4));
		}
		if(builder.net4Internal) {
			builder.ipInternalSocket(PeerSocket4Address.decode(buf));
		}
		if(builder.ipv6Flag) {
			builder.ipv6Socket(PeerSocket6Address.decode(buf, builder.skipIPv6));
		}
		return builder
				.peerId(Number160.decode(buf))
				.hashCode(builder.peerId.hashCode())
				.build();
    }

    //we have a 2 byte header
    private static PeerAddressBuilder decodeHeader(final PeerAddressBuilder builder, final int header) {
    	final byte[] tmp = new byte[]{(byte) ((header & RELAY_TYPE_MASK) >>> 17)};
    	builder.ipv4Flag((header & IPV4) == IPV4)
			.ipv6Flag((header & IPV6) == IPV6)
			.reachable4UDP((header & REACHABLE4_UDP) == REACHABLE4_UDP)
			.reachable4TCP((header & REACHABLE4_TCP) == REACHABLE4_TCP)
			.reachable4UDT((header & REACHABLE4_UDT) == REACHABLE4_UDT)
    		.reachable6UDP((header & REACHABLE6_UDP) == REACHABLE6_UDP)
    		.reachable6TCP((header & REACHABLE6_TCP) == REACHABLE6_TCP)
    		.reachable6UDT((header & REACHABLE6_UDT) == REACHABLE6_UDT)
    		.relaySize((header & RELAY_SIZE_MASK) >>> 8)
			.holePunching((header & RELAY_FLAGS_MASK) == HOLE_PUNCHING)
			.slow((header & RELAY_FLAGS_MASK) == SLOW)
			.unreachable((header & UNREACHABLE) == UNREACHABLE)
			.net4Internal((header & NET4_INTERNAL) == NET4_INTERNAL)
			.skipIPv4((header & SKIP_IPV4) == SKIP_IPV4)
			.skipIPv6((header & SKIP_IPV6) == SKIP_IPV6)
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
        	size += skipIPv4 ? PeerSocketAddress.PORT_SIZE : PeerSocket4Address.SIZE;
		}
		if(net4Internal) {
			size += PeerSocket4Address.SIZE;
		}
		if(ipv6Flag) {
			size += skipIPv6 ? PeerSocketAddress.PORT_SIZE : PeerSocket6Address.SIZE;
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
    	offset = Utils.mediumToByteArray(encodeHeader(), array, offset);
    	
    	for(final PeerSocketAddress relay:relays()) {
    		offset = relay.encode(array, offset);
    	}
    	
    	if(ipv4Flag) {
    		offset = ipv4Socket.encode(array, offset, skipIPv4);
		}
    	if(net4Internal) {
    		offset = ipInternalSocket.encode(array, offset);
		}
		if(ipv6Flag) {
			offset = ipv6Socket.encode(array, offset, skipIPv6);
		}
    	
    	offset = peerId.encode(array, offset);
        return offset;
    }
    
    public PeerAddress encode(final ByteBuf buf) {
    	final int header = encodeHeader() ;
    	buf.writeMedium(header);
    	
    	for(final PeerSocketAddress relay:relays()) {
    		relay.encode(buf);
    	}
    	
    	if(ipv4Flag) {
    		ipv4Socket.encode(buf, skipIPv4);
		}
    	if(net4Internal) {
    		ipInternalSocket.encode(buf);
		}
		if(ipv6Flag) {
			ipv6Socket.encode(buf, skipIPv6);
		}
    	
    	peerId.encode(buf);
        return this;
    }
    
    private int encodeHeader() {
    	return    (ipv4Flag ? IPV4 : 0)
				| (ipv6Flag ? IPV6 : 0)
				| (reachable4UDP ? REACHABLE4_UDP : 0)
				| (reachable4TCP ? REACHABLE4_TCP : 0)
				| (reachable4UDT ? REACHABLE4_UDT : 0)
				| (reachable6UDP ? REACHABLE6_UDP : 0)
				| (reachable6TCP ? REACHABLE6_TCP : 0)
				| (reachable6UDT ? REACHABLE6_UDT : 0)
				| ((relaySize << 8) & RELAY_SIZE_MASK)
				| (holePunching ? HOLE_PUNCHING : 0)
				| (slow ? SLOW : 0)
				| (unreachable ? UNREACHABLE : 0)
				| (net4Internal ? NET4_INTERNAL : 0)
				| (skipIPv4 ? SKIP_IPV4 : 0)
				| (skipIPv6 ? SKIP_IPV6 : 0)
				| ((relayTypes() << 17) & RELAY_TYPE_MASK);
	}
    


    @Override
    public String toString() {
		StringBuilder sb = new StringBuilder("paddr[");
		
		if(ipv4Flag) {
			sb.append(ipv4Socket);
    		if(!reachable4UDP) {
    			sb.append("u!");
    		}
    		if(!reachable4TCP) {
    			sb.append("t!");
    		}
    		if(!reachable4UDT) {
    			sb.append("d!");
    		}
		}
    	if(net4Internal) {
    		sb.append(ipInternalSocket);
		}
		if(ipv6Flag) {
			sb.append(ipv6Socket);
			if(!reachable6UDP) {
    			sb.append("u!");
    		}
    		if(!reachable6TCP) {
    			sb.append("t!");
    		}
    		if(!reachable6UDT) {
    			sb.append("d!");
    		}
		}
		sb.append('{');
		if(holePunching) {
			sb.append('h');
		}
		if(slow) {
			sb.append('s');
		}
		if(unreachable) {
			sb.append('u');
		}
		if(skipIPv4) {
			sb.append('4');
		}
		if(skipIPv6) {
			sb.append('6');
		}
		sb.append('}');
		
		sb.append("r:").append(relaySize).append("(");
		for(final PeerSocketAddress relay:relays()) {
			sb.append(relay);
    	}
		sb.append(')');
    	
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
        // don't calculate all the time, only once.
        return this.hashCode;
    }
    
    public IPv4 calcInternalInetAddress(IPv4 remote) {
		if(ipInternalNetworkPrefix == -1) {
			throw new IllegalArgumentException("network prefix must be set");
		}
		if(ipInternalSocket == null) {
			throw new IllegalArgumentException("internal peer socket must be set");
		}
		IP.IPv4 mask = ipInternalSocket.ipv4().maskWithNetworkMask(ipInternalNetworkPrefix);
		IP.IPv4 masked = remote.maskWithNetworkMaskInv(ipInternalNetworkPrefix);
		return mask.set(masked);
	}

	public PeerAddress withIPSocket(PeerSocketAddress ps) {
		if(ps instanceof PeerSocket4Address) {
			return withIpv4Socket((PeerSocket4Address) ps);
		} else {
			return withIpv6Socket((PeerSocket6Address) ps);	
		}
	}
	
	public static PeerAddress create(final Number160 peerId) {
		return builder().peerId(peerId).build();
	}
	
	public static PeerAddress create(final Number160 peerId, String host, int udpPort, int tcpPort, int udtPort) throws UnknownHostException {
		return create(peerId, InetAddress.getByName(host), udpPort, tcpPort, udtPort);
	}
	
	public static PeerAddress create(final Number160 peerId, InetAddress inet, int port) {
		return create(peerId, inet, port, port, port + 1); 
	}
	
	public static PeerAddress create(final Number160 peerId, InetSocketAddress inetSocket) {
		return create(peerId, inetSocket.getAddress(), inetSocket.getPort(), inetSocket.getPort(), inetSocket.getPort() + 1); 
	}
	
	public static PeerAddress create(final Number160 peerId, InetAddress inet, int udpPort, int tcpPort, int udtPort) {
		if(inet instanceof Inet4Address) {
			final PeerSocket4Address ps4a = PeerSocket4Address.builder()
					.ipv4(IP.IPv4.fromInet4Address(inet))
					.udpPort(udpPort)
					.tcpPort(tcpPort)
					.udtPort(udtPort)
					.build();
			return builder()
					.peerId(peerId)
					.ipv4Socket(ps4a)
					.build();
		} else {
			final PeerSocket6Address ps6a = PeerSocket6Address.builder()
					.ipv6(IP.IPv6.fromInet6Address(inet))
					.udpPort(udpPort)
					.tcpPort(tcpPort)
					.udtPort(udtPort)
					.build();
			return builder()
					.peerId(peerId)
					.ipv6Socket(ps6a)
					.build();
		}
	}

	

	public InetSocketAddress createUDPSocket(final PeerAddress other) {
		final boolean canIPv6 = ipv6Flag && other.ipv6Flag;
		final boolean canIPv4 = ipv4Flag && other.ipv4Flag;
		if((preferIPv6Addresses && canIPv6) || 
				(!canIPv4 && canIPv6)) {
			if(ipv6Socket == null) {
				throw new RuntimeException("Flag indicates that ipv6 is present, but its not");
			}
			return ipv6Socket.createUDPSocket();
		} else if(canIPv4) {
			if(ipv4Socket == null) {
				throw new RuntimeException("Flag indicates that ipv4 is present, but its not");
			}
			return ipv4Socket.createUDPSocket();
		}
		else {
			throw new RuntimeException("No matching protocal found");
		}
	}
	
	public InetSocketAddress createTCPSocket(final PeerAddress other) {
		final boolean canIPv6 = ipv6Flag && other.ipv6Flag;
		final boolean canIPv4 = ipv4Flag && other.ipv4Flag;
		if((preferIPv6Addresses && canIPv6) || 
				(!canIPv4 && canIPv6)) {
			if(ipv6Socket == null) {
				throw new RuntimeException("Flag indicates that ipv6 is present, but its not");
			}
			return ipv6Socket.createTCPSocket();
		} else if(canIPv4) {
			if(ipv4Socket == null) {
				throw new RuntimeException("Flag indicates that ipv4 is present, but its not");
			}
			return ipv4Socket.createTCPSocket();
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
