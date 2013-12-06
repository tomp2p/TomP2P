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
import java.net.UnknownHostException;
import java.util.BitSet;

import net.tomp2p.utils.Utils;

/**
 * A PeerAddress contains the node ID and how to contact this node using both TCP and UDP. This class is thread safe (or
 * it does not matter if its not). The format looks as follows:
 * 
 * <pre>
 * 20 bytes - Number160
 * 2 bytes - Header 
 *  - 1 byte options: IPv6, firewalled UDP, firewalled TCP
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
public final class PeerAddress implements Comparable<PeerAddress>, Serializable {

    public static final int MAX_SIZE = 142;
    public static final int MIN_SIZE = 30;
    public static final int MAX_RELAYS = 5;
    private static final long serialVersionUID = -1316622724169272306L;

    private static final int NET6 = 1;
    private static final int FIREWALL_UDP = 2;
    private static final int FIREWALL_TCP = 4;
    // indicates that a relay is used.
    private static final int IS_RELAY = 8;

    // network information
    private final Number160 peerId;
    private final PeerSocketAddress peerSocketAddress;

    // connection info
    private final boolean net6;
    private final boolean firewalledUDP;
    private final boolean firewalledTCP;
    private final boolean isRelay;

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
    private final PeerSocketAddress[] peerSocketAddresses;
    private static final PeerSocketAddress[] EMPTY_PEER_SOCKET_ADDRESSES = new PeerSocketAddress[0];

    private static final int TYPE_BIT_SIZE = 5;
    private static final int HEADER_SIZE = 2;
    // count both ports, UDP and TCP
    private static final int PORTS_SIZE = 4;

    // used for the relay bit shifting
    private static final int MASK_1F = 0x1f;
    private static final int MASK_7 = 0x7;
    

    /**
     * Creates a new peeraddress, where the byte array has to be in the rigth format and in the right size. The new
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
        this.isRelay = (options & IS_RELAY) > 0;
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
        this.relaySize = (relays >>> TYPE_BIT_SIZE) & MASK_7;
        final byte b = (byte) (relays & MASK_1F);
        this.relayType = Utils.createBitSet(b);
        // now comes the ID
        final byte[] tmp = new byte[Number160.BYTE_ARRAY_SIZE];
        System.arraycopy(me, offset, tmp, 0, Number160.BYTE_ARRAY_SIZE);
        this.peerId = new Number160(tmp);
        offset += Number160.BYTE_ARRAY_SIZE;

        this.peerSocketAddress = PeerSocketAddress.create(me, isIPv4(), offset);
        offset = this.peerSocketAddress.getOffset();
        if (relaySize > 0) {
            this.peerSocketAddresses = new PeerSocketAddress[relaySize];
            for (int i = 0; i < relaySize; i++) {
                peerSocketAddresses[i] = PeerSocketAddress.create(me, relayType.get(i), offset);
                offset = peerSocketAddresses[i].getOffset();
            }
        } else {
            this.peerSocketAddresses = EMPTY_PEER_SOCKET_ADDRESSES;
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
        this.isRelay = (options & IS_RELAY) > 0;
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
        this.relaySize = (relays >>> TYPE_BIT_SIZE) & MASK_7;
        final byte b = (byte) (relays & MASK_1F);
        this.relayType = Utils.createBitSet(b);
        // now comes the ID
        byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
        channelBuffer.readBytes(me);
        this.peerId = new Number160(me);

        this.peerSocketAddress = PeerSocketAddress.create(channelBuffer, isIPv4());

        if (relaySize > 0) {
            this.peerSocketAddresses = new PeerSocketAddress[relaySize];
            for (int i = 0; i < relaySize; i++) {
                peerSocketAddresses[i] = PeerSocketAddress.create(channelBuffer, relayType.get(i));
            }
        } else {
            this.peerSocketAddresses = EMPTY_PEER_SOCKET_ADDRESSES;
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
     * @param isRelay
     *            Indicates if peer used as a relay
     * @param peerSocketAddresses
     *            the relay peers
     */
    public PeerAddress(final Number160 id, final PeerSocketAddress peerSocketAddress,
            final boolean firewalledTCP, final boolean firewalledUDP, final boolean isRelay,
            final PeerSocketAddress[] peerSocketAddresses) {
        this.peerId = id;
        int size = Number160.BYTE_ARRAY_SIZE;
        this.peerSocketAddress = peerSocketAddress;
        this.hashCode = id.hashCode();
        this.net6 = peerSocketAddress.getInetAddress() instanceof Inet6Address;
        this.firewalledUDP = firewalledUDP;
        this.firewalledTCP = firewalledTCP;
        this.isRelay = isRelay;
        // header + TCP port + UDP port
        size += HEADER_SIZE + PORTS_SIZE + (net6 ? Utils.IPV6_BYTES : Utils.IPV4_BYTES);
        if (peerSocketAddresses == null) {
            this.peerSocketAddresses = EMPTY_PEER_SOCKET_ADDRESSES;
            this.relayType = EMPTY_RELAY_TYPE;
            relaySize = 0;
        } else {
            relaySize = peerSocketAddresses.length;
            if (relaySize > TYPE_BIT_SIZE) {
                throw new IllegalArgumentException("Can only store up to 5 relay peers");
            }
            this.peerSocketAddresses = peerSocketAddresses;
            this.relayType = new BitSet(relaySize);
        }
        for (int i = 0; i < relaySize; i++) {
            boolean isIPV6 = peerSocketAddresses[i].getInetAddress() instanceof Inet6Address;
            this.relayType.set(i, isIPV6);
        }
        this.size = size;
        // unused here
        this.offset = -1;
    }

    /**
     * Facade for {@link #PeerAddress(Number160, InetAddress, int, int, boolean, boolean, PeerSocketAddress[])}.
     * 
     * @param peerId
     *            The id of the peer
     * @param inetAddress
     *            The inetAddress of the peer, how to reach this peer
     * @param tcpPort
     *            The TCP port how to reach the peer
     * @param udpPort
     *            The UDP port how to reach the peer
     */
    public PeerAddress(final Number160 peerId, final InetAddress inetAddress, final int tcpPort,
            final int udpPort) {
        this(peerId, new PeerSocketAddress(inetAddress, tcpPort, udpPort), false, false, false,
                EMPTY_PEER_SOCKET_ADDRESSES);
    }

    /**
     * Facade for {@link #PeerAddress(Number160, InetAddress, int, int, boolean, boolean, PeerSocketAddress[])}.
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
     * Facade for {@link #PeerAddress(Number160, InetAddress, int, int, boolean, boolean, PeerSocketAddress[])}.
     * 
     * @param id
     *            The id of the peer
     * @param inetSocketAddress
     *            The socket address of the peer, how to reach this peer. Both UPD and TCP will be set to the same port
     */
    public PeerAddress(final Number160 id, final InetSocketAddress inetSocketAddress) {
        this(id, inetSocketAddress.getAddress(), inetSocketAddress.getPort(), inetSocketAddress.getPort());
    }

    /**
     * Facade for {@link #PeerAddress(Number160, InetAddress, int, int, boolean, boolean, PeerSocketAddress[])}.
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
     *            Set options
     */
    public PeerAddress(final Number160 id, final InetAddress inetAddress, final int tcpPort,
            final int udpPort, final int options) {
        this(id, new PeerSocketAddress(inetAddress, tcpPort, udpPort), isFirewalledTCP(options),
                isFirewalledUDP(options), isRelay(options), EMPTY_PEER_SOCKET_ADDRESSES);
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
        me[newOffset++] = getOptions();
        me[newOffset++] = getRelays();
        newOffset = peerId.toByteArray(me, newOffset);

        // we store both the address of the peer and the relays. Currently this is not needed, as we don't consider
        // asymmetric relays. But in future we may.
        newOffset = peerSocketAddress.toByteArray(me, newOffset);

        for (int i = 0; i < relaySize; i++) {
            newOffset = peerSocketAddresses[0].toByteArray(me, newOffset);
        }

        return newOffset;
    }

    /**
     * Returns the address or null if no address set.
     * 
     * @return The address of this peer
     */
    public InetAddress getInetAddress() {
        return peerSocketAddress.getInetAddress();
    }

    /**
     * Returns the socket address.
     * 
     * @return The socket address how to reach this peer
     */
    public InetSocketAddress createSocketTCP() {
        return new InetSocketAddress(peerSocketAddress.getInetAddress(), peerSocketAddress.getTcpPort());
    }

    /**
     * Returns the socket address.
     * 
     * @return The socket address how to reach this peer
     */
    public InetSocketAddress createSocketUDP() {
        return new InetSocketAddress(peerSocketAddress.getInetAddress(), peerSocketAddress.getUdpPort());
    }

    /**
     * The id of the peer. A peer cannot change its id.
     * 
     * @return Id of the peer
     */
    public Number160 getPeerId() {
        return peerId;
    }

    /**
     * @return The encoded options
     */
    public byte getOptions() {
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
        if (isRelay) {
            result |= IS_RELAY;
        }
        return result;
    }

    /**
     * @return The encoded relays. There are maximum 5 relays
     */
    public byte getRelays() {
        if (relaySize > 0) {
            byte result = (byte) (relaySize << TYPE_BIT_SIZE);
            byte types = Utils.createByte(relayType);
            result |= (byte) (types & MASK_1F);
            return result;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PeerAddr[");
        return sb.append(peerSocketAddress.toString()).append(",ID:").append(peerId.toString()).append("]")
                .toString();
    }

    @Override
    public int compareTo(final PeerAddress nodeAddress) {
        // the id determines if two peer are equal, the address does not matter
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
        return peerId.equals((pa).peerId);
    }

    @Override
    public int hashCode() {
        // don't calculate all the time, only once.
        return this.hashCode;
    }

    /**
     * @return TCP port
     */
    public int udpPort() {
        return peerSocketAddress.getUdpPort();
    }

    /**
     * @return UDP port
     */
    public int tcpPort() {
        return peerSocketAddress.getTcpPort();
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
    public boolean isRelay() {
        return isRelay;
    }

    /**
     * Create a new PeerAddress and change the firewallUDP status.
     * 
     * @param firewalledUDP
     *            the new status
     * @return The newly created peer address
     */
    public PeerAddress changeFirewalledUDP(final boolean firewalledUDP) {
        return new PeerAddress(peerId, peerSocketAddress, firewalledTCP, firewalledUDP, isRelay,
                peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the firewalledTCP status.
     * 
     * @param firewalledTCP
     *            the new status
     * @return The newly created peer address
     */
    public PeerAddress changeFirewalledTCP(final boolean firewalledTCP) {
        return new PeerAddress(peerId, peerSocketAddress, firewalledTCP, firewalledUDP, isRelay,
                peerSocketAddresses);
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
        return new PeerAddress(peerId, new PeerSocketAddress(peerSocketAddress.getInetAddress(), tcpPort,
                udpPort), firewalledTCP, firewalledUDP, isRelay, peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the InetAddress.
     * 
     * @param inetAddress
     *            The new InetAddress
     * @return The newly created peer address
     */
    public PeerAddress changeAddress(final InetAddress inetAddress) {
        return new PeerAddress(peerId, new PeerSocketAddress(inetAddress, peerSocketAddress.getTcpPort(),
                peerSocketAddress.getUdpPort()), firewalledTCP, firewalledUDP, isRelay, peerSocketAddresses);
    }

    /**
     * Create a new PeerAddress and change the peer id.
     * 
     * @param peerId
     *            the new peer id
     * @return The newly created peer address
     */
    public PeerAddress changePeerId(final Number160 peerId) {
        return new PeerAddress(peerId, peerSocketAddress, firewalledTCP, firewalledUDP, isRelay,
                peerSocketAddresses);
    }

    /**
     * @return The relay peers
     */
    public PeerSocketAddress[] getPeerSocketAddresses() {
        return peerSocketAddresses;
    }

    /**
     * Checks if option has IPv6 set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return True if its IPv6
     */
    private static boolean isNet6(final int options) {
        return ((options & Utils.MASK_FF) & NET6) > 0;
    }

    /**
     * Checks if option has firewall TCP set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return True if it firewalled via TCP
     */
    private static boolean isFirewalledTCP(final int options) {
        return ((options & Utils.MASK_FF) & FIREWALL_TCP) > 0;
    }

    /**
     * Checks if option has firewall UDP set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return True if it firewalled via UDP
     */
    private static boolean isFirewalledUDP(final int options) {
        return ((options & Utils.MASK_FF) & FIREWALL_UDP) > 0;
    }

    /**
     * Checks if option has relay flag set.
     * 
     * @param options
     *            The option field, lowest 8 bit
     * @return True if it is used as a relay
     */
    private static boolean isRelay(final int options) {
        return ((options & Utils.MASK_FF) & IS_RELAY) > 0;
    }

    /**
     * Calculates the size based on the two header bytes.
     * 
     * @param header
     *            The header is in the lower 16 bits
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
     *            The relays tells us how many relays we have and what type it is
     * @return returns the expected size of the peer address
     */
    public static int size(final int options, final int relays) {
        // header + tcp port + udp port + peer id
        int size = HEADER_SIZE + PORTS_SIZE + Number160.BYTE_ARRAY_SIZE;
        if (isNet6(options)) {
            size += Utils.IPV6_BYTES;
        } else {
            size += Utils.IPV4_BYTES;
        }
        // count the relays
        final int relaySize = (relays >>> TYPE_BIT_SIZE) & MASK_7;
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
        return size;
    }
}
