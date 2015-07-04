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

import io.netty.buffer.ByteBuf;

import java.io.Serializable;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;

import net.tomp2p.utils.Utils;

/**
 * A PeerAddress contains the node ID and how to contact this node using both TCP and UDP. This class is thread safe (or
 * it does not matter if its not). The format looks as follows:
 * 
 * <pre>
 * 20 bytes - Number160
 * 2 bytes - Header 
 *  - 1 byte options: IPv6, firewalled UDP, firewalled TCP, is relayed, has registration
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
 * 0 or 64 bytes - encoded registration reference
 * </pre>
 * 
 * @author Thomas Bocek
 */
public final class PeerAddress implements Comparable<PeerAddress>, Serializable {

    public static final int MAX_SIZE = 206;
    public static final int MIN_SIZE = 30;
    private static final long serialVersionUID = -1316622724169272306L;

    private static final int NET6 = 1;				// 00000001
    private static final int FIREWALL_UDP = 2;		// 00000010
    private static final int FIREWALL_TCP = 4;		// 00000100
    // indicates that a relay is used.			
    private static final int RELAYED = 8;			// 00001000
    // indicates that the peer is slow (because relayed)
    private static final int SLOW = 16;				// 00010000
    // indicates the peer forwarded the ports either manually or with UPNP
    private static final int PORT_FORWARDING = 32; 	// 00100000
    // indicate IPv4 with the internal IP
    private static final int NET4_PRIVATE = 64;		// 01000000
    // indicates that peer address includes registration reference
    private static final int REGISTRATION = 128; // 1000000
    // network information
    private final Number160 peerId;
    private final PeerSocketAddress peerSocketAddress;
    private final PeerSocketAddress internalPeerSocketAddress;
    private final int internalNetworkPrefix;

    // connection information
    private final boolean net6;
    private final boolean firewalledUDP;
    private final boolean firewalledTCP;
    private final boolean relayed;
    private final boolean slow;
    private final boolean portForwarding;
    private final boolean net4Private;

    // registration information
    private final boolean hasRegistration;
    private final byte[] registration;

    // we can make the hash final as it never changes, and this class is used
    // multiple times in maps. A new peer is always added to the peermap, so
    // making this final saves CPU time. The maps used are removedPeerCache,
    // that is always checked, peerMap that is either added or checked if
    // already present. Also peers from the neighbor list sent over the wire are
    // added to the peermap.
    private final int hashCode;

    // if deserialized from a byte array using the constructor, then we need to
    // report how many data we processed.
    private final int offset;

    private final int size;
    private final int relaySize;
    private final BitSet relayType;
    private static final BitSet EMPTY_RELAY_TYPE = new BitSet(0);
    //relays
    private final Collection<PeerSocketAddress> peerSocketAddresses;
    public static final Collection<PeerSocketAddress> EMPTY_PEER_SOCKET_ADDRESSES = Collections.emptySet();

    private static final int TYPE_BIT_SIZE = 5;
    private static final int HEADER_SIZE = 2;
    // count both ports, UDP and TCP
    private static final int PORTS_SIZE = 4;

    private static final int REGISTRATION_BYTE_SIZE = 64;

    // used for the relay bit shifting
    private static final int MASK_1F = 0x1f;	// 0001 1111
    private static final int MASK_07 = 0x7;		// 0000 0111

    

    /**
     * Creates a peer address, where the byte array has to be in the right format and in the right size. The new
     * offset can be accessed with offset().
     * 
     * @param me
     *            The serialized array
     */
    public PeerAddress(final byte[] me) {
        this(me, 0);
    }

    /**
     * Creates a PeerAddress from a continuous byte array. This is useful if you don't know the size beforehand. The new
     * offset can be accessed with offset().
     * 
     * @param me
     *            The serialized array
     * @param initialOffset
     *            the offset, where to start
     */
    public PeerAddress(final byte[] me, final int initialOffset) {
        // get the peer ID, this is independent of the type
        int offset = initialOffset;
        // get the type
        final int options = me[offset++] & Utils.MASK_FF;
        this.net6 = (options & NET6) > 0;
        this.firewalledUDP = (options & FIREWALL_UDP) > 0;
        this.firewalledTCP = (options & FIREWALL_TCP) > 0;
        this.relayed = (options & RELAYED) > 0;
        this.slow = (options & SLOW) > 0;
        this.portForwarding = (options & PORT_FORWARDING) > 0;
        this.net4Private = (options & NET4_PRIVATE) > 0;
        this.hasRegistration = (options & REGISTRATION) > 0;
        final int relays = me[offset++] & Utils.MASK_FF;
        // first: three bits are the size 1,2,4
        // 000 means no relays
        // 001 means 1 relay
        // 010 means 2 relays
        // 011 means 3 relays
        // 100 means 4 relays
        // 101 means 5 relays
        // 110 is not used
        // 111 is not used
        // second: five bits indicate if IPv6 or IPv4 -> in total we can save 5 addresses
        this.relaySize = (relays >>> TYPE_BIT_SIZE) & MASK_07;
        final byte b = (byte) (relays & MASK_1F);
        this.relayType = Utils.createBitSet(b);
        // now comes the ID
        final byte[] tmp = new byte[Number160.BYTE_ARRAY_SIZE];
        System.arraycopy(me, offset, tmp, 0, Number160.BYTE_ARRAY_SIZE);
        this.peerId = new Number160(tmp);
        offset += Number160.BYTE_ARRAY_SIZE;

        this.peerSocketAddress = PeerSocketAddress.create(me, isIPv4(), offset);
        offset = this.peerSocketAddress.offset();
        if(isNet4Private()) {
        	this.internalPeerSocketAddress = PeerSocketAddress.create(me, true, offset);
        	offset = this.internalPeerSocketAddress.offset();
        } else {
        	this.internalPeerSocketAddress = null;
        }
        this.internalNetworkPrefix = -1;
        
        if (relaySize > 0) {
            this.peerSocketAddresses = new ArrayList<PeerSocketAddress>(relaySize);
            for (int i = 0; i < relaySize; i++) {
                PeerSocketAddress psa = PeerSocketAddress.create(me, !relayType.get(i), offset);
                peerSocketAddresses.add(psa);
                offset = psa.offset();
            }
        } else {
            this.peerSocketAddresses = EMPTY_PEER_SOCKET_ADDRESSES;
        }
        if(this.hasRegistration) {
            this.registration = new byte[REGISTRATION_BYTE_SIZE];
            System.arraycopy(me, offset, this.registration, 0, REGISTRATION_BYTE_SIZE);
            offset += REGISTRATION_BYTE_SIZE;
        } else {
            this.registration = null;
        }
        this.size = offset - initialOffset;
        this.offset = offset;
        this.hashCode = peerId.hashCode();
    }

    /**
     * Creates a PeerAddress from a Netty ByteBuf.
     * 
     * @param channelBuffer
     *            The channel buffer to read from
     */
    public PeerAddress(final ByteBuf channelBuffer) {
        // get the peer ID, this is independent of the type

        int readerIndex = channelBuffer.readerIndex();
        // get the type
        final int options = channelBuffer.readUnsignedByte();
        this.net6 = (options & NET6) > 0;
        this.firewalledUDP = (options & FIREWALL_UDP) > 0;
        this.firewalledTCP = (options & FIREWALL_TCP) > 0;
        this.relayed = (options & RELAYED) > 0;
        this.slow = (options & SLOW) > 0;
        this.portForwarding = (options & PORT_FORWARDING) > 0;
        this.net4Private = (options & NET4_PRIVATE) > 0;
        this.hasRegistration = (options & REGISTRATION) > 0;
        final int relays = channelBuffer.readUnsignedByte();
        // first: three bits are the size 1,2,4
        // 000 means no relays
        // 001 means 1 relay
        // 010 means 2 relays
        // 011 means 3 relays
        // 100 means 4 relays
        // 101 means 5 relays
        // 110 is not used
        // 111 is not used
        // second: five bits indicate if IPv6 or IPv4 -> in total we can save 5 addresses
        this.relaySize = (relays >>> TYPE_BIT_SIZE) & MASK_07;
        final byte b = (byte) (relays & MASK_1F);
        this.relayType = Utils.createBitSet(b);
        // now comes the ID
        byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
        channelBuffer.readBytes(me);
        this.peerId = new Number160(me);

        this.peerSocketAddress = PeerSocketAddress.create(channelBuffer, isIPv4());
        this.internalPeerSocketAddress = isNet4Private() ? PeerSocketAddress.create(channelBuffer, true) : null; 
        this.internalNetworkPrefix = -1;

        if (relaySize > 0) {
            this.peerSocketAddresses = new ArrayList<PeerSocketAddress>(relaySize);
            for (int i = 0; i < relaySize; i++) {
                this.peerSocketAddresses.add(PeerSocketAddress.create(channelBuffer, !relayType.get(i)));
            }
        } else {
            this.peerSocketAddresses = EMPTY_PEER_SOCKET_ADDRESSES;
        }

        if (hasRegistration) {
            byte[] reg = new byte[64];
            channelBuffer.readBytes(reg);
            this.registration = reg;
        } else {
            this.registration = null;
        }

        this.size = channelBuffer.readerIndex() - readerIndex;
        // not used here
        this.offset = -1;
        this.hashCode = peerId.hashCode();
    }

    /**
     * If you only need to know the id.
     * 
     * @param id
     *            The id of the peer
     */
    public PeerAddress(final Number160 id) {
        this(id, (InetAddress) null, -1, -1);
    }

    /**
     * If you only need to know the id and InetAddress.
     * 
     * @param id
     *            The id of the peer
     * @param address
     *            The InetAddress of the peer
     */
    public PeerAddress(final Number160 id, final InetAddress address) {
        this(id, address, -1, -1);
    }

    /**
     * Creates a PeerAddress if all the values are known.
     * 
     * @param id
     *            The id of the peer
     * @param peerSocketAddress
     *            The peer socket address including both ports UDP and TCP
     * @param firewalledUDP
     *            Indicates if peer is not reachable via UDP
     * @param firewalledTCP
     *            Indicates if peer is not reachable via TCP
     * @param isRelayed
     *            Indicates if peer is used as a relay
     * @param isSlow
     * 			  Indicates if a peer is slow
     * @param peerSocketAddresses
     *            the relay peers
     */
    public PeerAddress(final Number160 id, final PeerSocketAddress peerSocketAddress, final PeerSocketAddress internalPeerSocketAddress,
            final boolean firewalledTCP, final boolean firewalledUDP, final boolean isRelayed,
            boolean isSlow, boolean portForwarding, boolean net4Private, byte[] registration, final Collection<PeerSocketAddress> peerSocketAddresses) {
        this.peerId = id;
        int size = Number160.BYTE_ARRAY_SIZE;
        this.peerSocketAddress = peerSocketAddress;
        this.internalPeerSocketAddress = internalPeerSocketAddress;
        this.internalNetworkPrefix = internalPeerSocketAddress == null ? -1: prefix(internalPeerSocketAddress.inetAddress());
        this.hashCode = id.hashCode();
        this.net6 = peerSocketAddress.inetAddress() instanceof Inet6Address;
        this.firewalledUDP = firewalledUDP;
        this.firewalledTCP = firewalledTCP;
        this.relayed = isRelayed;
        this.slow = isSlow;
        this.portForwarding = portForwarding;
        this.net4Private = net4Private;
        // header + TCP port + UDP port
        size += HEADER_SIZE + PORTS_SIZE + (net6 ? Utils.IPV6_BYTES : Utils.IPV4_BYTES);
        size += net4Private? Utils.IPV4_BYTES + PORTS_SIZE : 0;
        if (peerSocketAddresses == null) {
            this.peerSocketAddresses = EMPTY_PEER_SOCKET_ADDRESSES;
            this.relayType = EMPTY_RELAY_TYPE;
            relaySize = 0;
        } else {
            relaySize = peerSocketAddresses.size();
            if (relaySize > TYPE_BIT_SIZE) {
                throw new IllegalArgumentException(String.format("Can only store up to %s relay peers. Tried to store %s relay peers.", TYPE_BIT_SIZE , relaySize));
            }
            this.peerSocketAddresses = peerSocketAddresses;
            this.relayType = new BitSet(relaySize);
        }
        int index = 0;
        for(PeerSocketAddress psa : this.peerSocketAddresses) {
            boolean isIPV6 = psa.inetAddress() instanceof Inet6Address;
            this.relayType.set(index, isIPV6);
            size += psa.size();
            index++;
        }
        this.registration = registration;
        if(registration == null) {
            this.hasRegistration = false;
        } else {
            this.hasRegistration = true;
            size += REGISTRATION_BYTE_SIZE;
        }
        this.size = size;
        // unused here
        this.offset = -1;
    }

    /**
     * Facade for PeerAddress(Number160, PeerSocketAddress, boolean, boolean, boolean, Collection-PeerSocketAddress>).
     * 
     * @param peerId
     *            The id of the peer
     * @param inetAddress
     *            The internet address of the peer
     * @param tcpPort
     *            The TCP port of the peer
     * @param udpPort
     *            The UDP port of the peer
     */
    public PeerAddress(final Number160 peerId, final InetAddress inetAddress, final int tcpPort,
            final int udpPort) {
        this(peerId, new PeerSocketAddress(inetAddress, tcpPort, udpPort), null, false, false, false, false, false, false, null, EMPTY_PEER_SOCKET_ADDRESSES);
    }

    /**
     * Facade for PeerAddress(...)
     * 
     * @param id
     *            The id of the peer
     * @param inetAddress
     *            The address of the peer, how to reach this peer
     * @param tcpPort
     *            The TCP port how to reach the peer
     * @param udpPort
     *            The UDP port how to reach the peer
     * @param options
     *            The options for the created PeerAddress.
     */
    public PeerAddress(final Number160 id, final InetAddress inetAddress, final int tcpPort,
            final int udpPort, final int options) {
        this(id, new PeerSocketAddress(inetAddress, tcpPort, udpPort), null, isFirewalledTCP(options),
                isFirewalledUDP(options), isRelay(options), isSlow(options), isPortForwarding(options), 
                isNet4Private(options), null, EMPTY_PEER_SOCKET_ADDRESSES);
    }
    
    /**
     * Facade for PeerAddress(Number160, InetAddress, int, int).
     * 
     * @param id
     *            The id of the peer
     * @param address
     *            The address of the peer, how to reach this peer
     * @param tcpPort
     *            The TCP port how to reach the peer
     * @param udpPort
     *            The UDP port how to reach the peer
     * @throws UnknownHostException
     *             If no IP address for the host could be found, or if a scope_id was specified for a global IPv6
     *             address.
     */
    public PeerAddress(final Number160 id, final String address, final int tcpPort, final int udpPort)
            throws UnknownHostException {
        this(id, InetAddress.getByName(address), tcpPort, udpPort);
    }

    /**
     * Facade for PeerAddress(Number160, InetAddress, int, int).
     * 
     * @param id
     *            The id of the peer
     * @param inetSocketAddress
     *            The socket address of the peer, how to reach this peer. Both TCP and UDP will be set to the same port
     */
    public PeerAddress(final Number160 id, final InetSocketAddress inetSocketAddress) {
        this(id, inetSocketAddress.getAddress(), inetSocketAddress.getPort(), inetSocketAddress.getPort());
    }

    /**
     * When deserializing, we need to know how much we deserialized from the constructor call.
     * 
     * @return The new offset
     */
    public int offset() {
        return offset;
    }

    /**
     * @return The size of the serialized peer address
     */
    public int size() {
        return size;
    }

    /**
     * Serializes to a new array with the proper size.
     * 
     * @return The serialized representation.
     */
    public byte[] toByteArray() {
        byte[] me = new byte[size];
        toByteArray(me, 0);
        return me;
    }

    /**
     * Serializes to an existing array.
     * 
     * @param me
     *            The array where the result should be stored
     * @param offset
     *            The offset where to start to save the result in the byte array
     * @return The new offset.
     */
    public int toByteArray(final byte[] me, final int offset) {
        // save the peer id
        int newOffset = offset;
        me[newOffset++] = options();
        me[newOffset++] = relays();
        newOffset = peerId.toByteArray(me, newOffset);

        // we store both the addresses of the peer and the relays. Currently this is not needed, as we don't consider
        // asymmetric relays. But in future we may.
        newOffset = peerSocketAddress.toByteArray(me, newOffset);
        if(isNet4Private()) {
        	newOffset = strip().toByteArray(me, newOffset);
        }

        for (PeerSocketAddress psa : peerSocketAddresses) {
            newOffset = psa.toByteArray(me, newOffset);
        }
        if(registration != null) {
            System.arraycopy(registration, 0, me, newOffset, REGISTRATION_BYTE_SIZE);
            newOffset += REGISTRATION_BYTE_SIZE;
        }
        return newOffset;
    }
    
    public PeerSocketAddress peerSocketAddress() {
    	return peerSocketAddress;
    }

    /**
     * Returns the address or null if no address set.
     * 
     * @return The address of this peer
     */
    public InetAddress inetAddress() {
        return peerSocketAddress.inetAddress();
    }

    /**
     * Creates and returns the socket address using the TCP port.
     * 
     * @return The socket address how to reach this peer.
     */
    public InetSocketAddress createSocketTCP() {
        return new InetSocketAddress(peerSocketAddress.inetAddress(), peerSocketAddress.tcpPort());
    }

    /**
     * Creates and returns the socket address using the UDP port.
     * 
     * @return The socket address how to reach this peer
     */
    public InetSocketAddress createSocketUDP() {
        return new InetSocketAddress(peerSocketAddress.inetAddress(), peerSocketAddress.udpPort());
    }
    
    public InetSocketAddress createSocketUDP(int port) {
        return new InetSocketAddress(peerSocketAddress.inetAddress(), port);
    }

    /**
     * The id of the peer. A peer cannot change its id.
     * 
     * @return Id of the peer
     */
    public Number160 peerId() {
        return peerId;
    }

    /**
     * @return The encoded options
     */
    public byte options() {
        byte result = 0;
        if (net6) {
            result |= NET6;
        }
        if (firewalledUDP) {
            result |= FIREWALL_UDP;
        }
        if (firewalledTCP) {
            result |= FIREWALL_TCP;
        }
        if (relayed) {
            result |= RELAYED;
        }
        if(slow) {
        	result |= SLOW;
        }
        if(portForwarding) {
        	result |= PORT_FORWARDING;
        }
        if(net4Private) {
            result |= NET4_PRIVATE;
        }
        if(hasRegistration) {
            result |= REGISTRATION;
        }
        return result;
    }

    /**
     * @return The encoded relays. There are maximal 5 relays.
     */
    public byte relays() {
        if (relaySize > 0) {
            byte result = (byte) (relaySize << TYPE_BIT_SIZE);
            byte types = Utils.createByte(relayType);
            result |= (byte) (types & MASK_1F);
            return result;
        }
        return 0;
    }

    @Override
    public String toString() {
		StringBuilder sb = new StringBuilder("paddr[");
		sb.append(peerId.toString()).append(peerSocketAddress.toString()).append("]");
		sb.append("/relay(").append(relayed);
		if (relayed) {
			sb.append(",").append(peerSocketAddresses.size()).append(")=")
					.append(Arrays.toString(peerSocketAddresses.toArray()));
		} else {
			sb.append(")");
		}

		sb.append("/slow(").append(slow).append(")");

		return sb.toString();
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

    /**
     * @return UDP port
     */
    public int udpPort() {
        return peerSocketAddress.udpPort();
    }

    /**
     * @return TCP port
     */
    public int tcpPort() {
        return peerSocketAddress.tcpPort();
    }

    /**
     * @return True if the peer cannot be reached via UDP
     */
    public boolean isFirewalledUDP() {
        return firewalledUDP;
    }

    /**
     * @return True if the peer cannot be reached via TCP
     */
    public boolean isFirewalledTCP() {
        return firewalledTCP;
    }

    /**
     * @return True if the inet address is IPv6
     */
    public boolean isIPv6() {
        return net6;
    }

    /**
     * @return True if the inet address is IPv4
     */
    public boolean isIPv4() {
        return !net6;
    }

    /**
     * @return If this peer address is used as a relay
     */
    public boolean isRelayed() {
        return relayed;
    }
    
    /**
     * @return if the peer is slow (might be a relayed mobile device)
     */
    public boolean isSlow() {
    	return slow;
    }
    
    public boolean isPortForwarding() {
    	return portForwarding;
    }
    
    public boolean isNet4Private() {
    	return net4Private;
    }
    
    /**
     * Create a new PeerAddress and change the relayed status.

     * @param isRelayed 
     *           the new relay status
     * @return The newly created peer address
     */
    public PeerAddress changeRelayed(final boolean isRelayed) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, isRelayed, slow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }
    
    /**
     * Create a new PeerAddress and change the slow status

     * @param isSlow 
     *           the new status
     * @return The newly created peer address
     */
    public PeerAddress changeSlow(final boolean isSlow) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, isSlow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }
    
    public PeerAddress changePortForwarding(final boolean portForwarding) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the firewall UDP status.
     * 
     * @param firewalledUDP
     *            the new status
     * @return The newly created peer address
     */
    public PeerAddress changeFirewalledUDP(final boolean firewalledUDP) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the firewall TCP status.
     * 
     * @param firewalledTCP
     *            the new status
     * @return The newly created peer address
     */
    public PeerAddress changeFirewalledTCP(final boolean firewalledTCP) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the TCP and UDP ports.
     * 
     * @param tcpPort
     *            The new TCP port
     * @param udpPort
     *            The new UDP port
     * @return The newly created peer address
     */
    public PeerAddress changePorts(final int tcpPort, final int udpPort) {
        return new PeerAddress(peerId, new PeerSocketAddress(peerSocketAddress.inetAddress(), tcpPort,
                udpPort), internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private, null, peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the InetAddress.
     * 
     * @param inetAddress
     *            The new InetAddress
     * @return The newly created peer address
     */
    public PeerAddress changeAddress(final InetAddress inetAddress) {
        return new PeerAddress(peerId, new PeerSocketAddress(inetAddress, peerSocketAddress.tcpPort(),
                peerSocketAddress.udpPort()), internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private, null, peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the peer id.
     * 
     * @param peerId
     *            the new peer id
     * @return The newly created peer address
     */
    public PeerAddress changePeerId(final Number160 peerId) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }

    public PeerAddress changePeerSocketAddresses(Collection<PeerSocketAddress> peerSocketAddresses) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }
    
    public PeerAddress changePeerSocketAddress(PeerSocketAddress peerSocketAddress) {
    	return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private,
                null, peerSocketAddresses);
    }
    
    public PeerAddress changeInternalPeerSocketAddress(PeerSocketAddress internalPeerSocketAddress) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, internalPeerSocketAddress!=null,
                null, peerSocketAddresses);
    }

    public PeerAddress changeRegistration(final byte[] registration) {
        return new PeerAddress(peerId, peerSocketAddress, internalPeerSocketAddress, firewalledTCP, firewalledUDP, relayed, slow, portForwarding, net4Private,
                registration, peerSocketAddresses);
    }
    /**
     * @return The relay peers
     */
    public Collection<PeerSocketAddress> peerSocketAddresses() {
        return peerSocketAddresses;
    }

    /**
     * Checks if option has IPv6 set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return <code>True</code> if its IPv6
     */
    private static boolean isNet6(final int options) {
        return ((options & Utils.MASK_FF) & NET6) > 0;
    }

    /**
     * Checks if option has firewall TCP set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return <code>True</code> if it firewalled via TCP
     */
    private static boolean isFirewalledTCP(final int options) {
        return ((options & Utils.MASK_FF) & FIREWALL_TCP) > 0;
    }

    /**
     * Checks if option has firewall UDP set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return <code>True</code> if it firewalled via UDP
     */
    private static boolean isFirewalledUDP(final int options) {
        return ((options & Utils.MASK_FF) & FIREWALL_UDP) > 0;
    }

    /**
     * Checks if option has relay flag set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return <code>True</code> if it is used as a relay
     */
    private static boolean isRelay(final int options) {
        return ((options & Utils.MASK_FF) & RELAYED) > 0;
    }
    
    /**
     * Checks if option has slow flag set
     * 
     * @param options
     * 			 The option field
     * @return <code>true</code> if the peer is slow
     */
    private static boolean isSlow(final int options) {
    	 return ((options & Utils.MASK_FF) & SLOW) > 0;
    }
    
    private static boolean isPortForwarding(final int options) {
   	 	return ((options & Utils.MASK_FF) & PORT_FORWARDING) > 0;
    }
    
    private static boolean isNet4Private(final int options) {
   	 	return ((options & Utils.MASK_FF) & NET4_PRIVATE) > 0;
    }

    private static boolean hasRegistration(final int options) {
        return ((options & Utils.MASK_FF) & REGISTRATION) > 0;
    }

    /**
     * Calculates the size based on the two header bytes.
     * 
     * @param header
     *            The header in the lower 16 bits of this integer.
     * @return the expected size of the peer address
     */
    public static int size(final int header) {
        int options = (header >>> Utils.BYTE_BITS) & Utils.MASK_FF;
        int relays = header & Utils.MASK_FF;
        return size(options, relays);
    }

    /**
     * Calculates the size based on the two header bytes.
     * 
     * @param options
     *            The option tells us if the inet address is IPv4 or IPv6
     * @param relays
     *            The relays tells us how many relays we have and of what type they are.
     * @return returns the expected size of the peer address
     */
    public static int size(final int options, final int relays) {
        // header + tcp port + udp port + peer id
        int size = HEADER_SIZE + PORTS_SIZE + Number160.BYTE_ARRAY_SIZE;
        if (isNet6(options)) {
            size += Utils.IPV6_BYTES;
        } else if(isNet4Private(options)) {
        	size += Utils.IPV4_BYTES + Utils.IPV4_BYTES + PORTS_SIZE;
        }
        else {
            size += Utils.IPV4_BYTES;
        }
        // count the relays
        final int relaySize = (relays >>> TYPE_BIT_SIZE) & MASK_07;
        final byte b = (byte) (relays & MASK_1F);
        final BitSet relayType = Utils.createBitSet(b);
        for (int i = 0; i < relaySize; i++) {
            size += PORTS_SIZE;
            if (relayType.get(i)) {
                size += Utils.IPV6_BYTES;
            } else {
                size += Utils.IPV4_BYTES;
            }
        }
        //check for registration
        if (hasRegistration(options)) {
            size += REGISTRATION_BYTE_SIZE;
        }
        return size;
    }

	public int relaySize() {
	    return relaySize;
    }
	
	public PeerSocketAddress internalPeerSocketAddress() {
		return internalPeerSocketAddress;
	}
	
	public PeerSocketAddress strip() {
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
	}
	
	public InetAddress calcInternalInetAddress(InetAddress remote) {
		if(internalNetworkPrefix == -1) {
			throw new IllegalArgumentException("network prefix must be set");
		}
		if(internalPeerSocketAddress == null) {
			throw new IllegalArgumentException("internal peer socket must be set");
		}
		IP.IPv4 mask = IP.fromInet4Address(internalPeerSocketAddress.inetAddress());
		mask = mask.maskWithNetworkMask(internalNetworkPrefix);
		IP.IPv4 masked = IP.fromInet4Address(remote);
		masked = masked.maskWithNetworkMaskInv(internalNetworkPrefix);
		return mask.set(masked).toInetAddress();
	}
	
	private int prefix(InetAddress inetAddress) {
		NetworkInterface networkInterface;
		try {
			networkInterface = NetworkInterface.getByInetAddress(internalPeerSocketAddress.inetAddress());
			if(networkInterface == null) {
				return -1;
			}
			if(networkInterface.getInterfaceAddresses().size() <= 0) {
				return -1;
			}
			InterfaceAddress iface = networkInterface.getInterfaceAddresses().get(0);
			if(iface == null) {
				return -1;
			}
			return iface.getNetworkPrefixLength(); 
		} catch (SocketException e) {
			e.printStackTrace();
			return -1;
		}
	}

    public byte[] getRegistration() {
        return registration;
    }
}
