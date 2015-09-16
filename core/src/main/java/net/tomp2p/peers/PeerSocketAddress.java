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

import io.netty.buffer.ByteBuf;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import net.tomp2p.utils.Utils;

/**
 * A PeerSocketAddress includes always both ports, UDP and TCP.
 * 
 * @author Thomas Bocek
 * 
 */
public class PeerSocketAddress implements Serializable {
    private static final long serialVersionUID = 8483270473601620720L;
    private final InetAddress inetAddress;
    private final int tcpPort;
    private final int udpPort;
    private final int offset;

    /**
     * Creates a PeerSocketAddress including both UDP and TCP ports.
     * 
     * @param inetAddress
     *            The InetAddress of the peer. Can be IPv4 or IPv6
     * @param tcpPort
     *            The TCP port
     * @param udpPort
     *            The UDP port
     */
    public PeerSocketAddress(final InetAddress inetAddress, final int tcpPort, final int udpPort) {
        this(inetAddress, tcpPort, udpPort, -1);
    }

    /**
     * Creates a PeerSocketAddress including both UDP and TCP ports. 
     * This constructor is used mostly internally as the offset is stored as well.
     * 
     * @param inetAddress
     *            The InetAddress of the peer. Can be IPv4 or IPv6
     * @param tcpPort
     *            The TCP port
     * @param udpPort
     *            The UDP port
     * @param offset
     *            The offset that we processed
     */
    public PeerSocketAddress(final InetAddress inetAddress, final int tcpPort, final int udpPort, final int offset) {
        this.inetAddress = inetAddress;
        this.tcpPort = tcpPort;
        this.udpPort = udpPort;
        this.offset = offset;
    }

    /**
	 * Converts a byte array into a PeerSocketAddress.
	 * 
	 * @param me
	 *            The byte array
	 * @param isIPv4
	 *            Whether its IPv4 or IPv6
	 * @param offsetOriginal
	 *            The offset from where to start reading in the array
	 * @return the PeerSocketAddress and the new offset
	 */
	public static PeerSocketAddress create(final byte[] me, final boolean isIPv4, final int offsetOriginal) {
	    int offset = offsetOriginal;
	    final int tcpPort = ((me[offset++] & Utils.MASK_FF) << Utils.BYTE_BITS) + (me[offset++] & Utils.MASK_FF);
	    final int udpPort = ((me[offset++] & Utils.MASK_FF) << Utils.BYTE_BITS) + (me[offset++] & Utils.MASK_FF);

	    final InetAddress address;
	    if (isIPv4) {
	        address = Utils.inet4FromBytes(me, offset);
	        offset += Utils.IPV4_BYTES;
	    } else {
	        address = Utils.inet6FromBytes(me, offset);
	        offset += Utils.IPV6_BYTES;
	    }
	    return new PeerSocketAddress(address, tcpPort, udpPort, offset);
	}

	/**
	 * Decodes a PeerSocketAddress from a Netty buffer.
	 * 
	 * @param buf
	 *            The Netty buffer
	 * @param isIPv4
	 *            Whether the address is IPv4 or IPv6
	 * @return the PeerSocketAddress and the new offset
	 */
	public static PeerSocketAddress create(final ByteBuf buf, final boolean isIPv4) {
	    final int tcpPort = buf.readUnsignedShort();
	    final int udpPort = buf.readUnsignedShort();

	    final InetAddress address;
	    final byte[] me;
	    if (isIPv4) {
	        me = new byte[Utils.IPV4_BYTES];
	        buf.readBytes(me);
	        address = Utils.inet4FromBytes(me, 0);
	    } else {
	        me = new byte[Utils.IPV6_BYTES];
	        buf.readBytes(me);
	        address = Utils.inet6FromBytes(me, 0);
	    }
	    return new PeerSocketAddress(address, tcpPort, udpPort, buf.readerIndex());
	}

	/**
	 * Creates the socket address to reach this peer with TCP.
	 * 
	 * @param peerSocketAddress
	 * 				The peer's PeerSocketAddress
	 * 
	 * @return The socket address to reach this peer with TCP.
	 */
	public static InetSocketAddress createSocketTCP(PeerSocketAddress peerSocketAddress) {
	    return new InetSocketAddress(peerSocketAddress.inetAddress(), peerSocketAddress.tcpPort());
	}

	/**
	 * Creates the socket address to reach this peer with UDP.
	 * 
	 * @param peerSocketAddress
	 * 				The peer's PeerSocketAddress
	 * 
	 * @return The socket address to reach this peer with UDP.
	 */
	public static InetSocketAddress createSocketUDP(PeerSocketAddress peerSocketAddress) {
	    return new InetSocketAddress(peerSocketAddress.inetAddress(), peerSocketAddress.udpPort());
	}

	/**
     * @return The inernet address, which is IPv4 or IPv6.
     */
    public InetAddress inetAddress() {
        return inetAddress;
    }

    /**
     * @return The TCP port.
     */
    public int tcpPort() {
        return tcpPort;
    }

    /**
     * @return The UDP port.
     */
    public int udpPort() {
        return udpPort;
    }

    /**
     * @return The offset.
     */
    public int offset() {
        return offset;
    }
    
    /**
	 * Serializes the PeerSocketAddress to a byte array. First, the ports (TCP, UDP) are serialized, then the address.
	 * 
	 * @return The serialized PeerSocketAddress
	 */
    public byte[] toByteArray() {
    	int size = size();
    	byte[] retVal = new byte[size];
    	int size2 = toByteArray(retVal, 0);
    	if(size!=size2) {
    		throw new RuntimeException("Sizes do not match.");
    	}
    	return retVal;
    }
    
    /**
	 * Serializes the PeerSocketAddress to a byte array. First, the ports (TCP, UDP) are serialized, then the address.
	 * 
	 * @param me
	 *            The byte array to serialize to
	 * @param offset
	 *            The offset from where to start
	 * @return How many data has been written
	 */
	public int toByteArray(final byte[] me, final int offset) {
	    int offset2 = offset;
	    me[offset2++] = (byte) (tcpPort >>> Utils.BYTE_BITS);
	    me[offset2++] = (byte) tcpPort;
	    me[offset2++] = (byte) (udpPort >>> Utils.BYTE_BITS);
	    me[offset2++] = (byte) udpPort;
	
	    if (inetAddress instanceof Inet4Address) {
	        System.arraycopy(inetAddress.getAddress(), 0, me, offset2, Utils.IPV4_BYTES);
	        offset2 += Utils.IPV4_BYTES;
	    } else {
	        System.arraycopy(inetAddress.getAddress(), 0, me, offset2, Utils.IPV6_BYTES);
	        offset2 += Utils.IPV6_BYTES;
	    }
	    return offset2;
	}

	/**
	 * Calculates the size of this PeerSocketAddress in bytes.
	 * Format: 2 bytes TCP port, 2 bytes UDP port, 4/16 bytes IPv4/IPv6 address.
	 * 
	 * @return The size of this PeerSocketAddress in bytes.
	 */
	public int size() {
    	return size(isIPv4());
    }
    
	/**
	 * Calculates the size of a PeerSocketAddress in bytes.
	 * Format: 2 bytes TCP port, 2 bytes UDP port, 4/16 bytes IPv4/IPv6 address.
	 * 
	 * @param isIPv4
	 * 			   Whether the address is IPv4 or IPv6. 				
	 * 
	 * @return The size of this PeerSocketAddress in bytes.
	 */
    public static int size(boolean isIPv4) {
    	return 2 + 2 + (isIPv4 ? Utils.IPV4_BYTES:Utils.IPV6_BYTES);
    }

    public boolean isIPv4() {
    	return inetAddress instanceof Inet4Address;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(inetAddress);
        if(tcpPort == udpPort) {
        	sb.append(",").append(tcpPort);
        } else {
        	sb.append(",t:").append(tcpPort).append(",u:").append(udpPort);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof PeerSocketAddress)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        PeerSocketAddress psa = (PeerSocketAddress) obj;
        return psa.inetAddress.equals(inetAddress) && psa.tcpPort == tcpPort && psa.udpPort == udpPort;
    }
    
    @Override
    public int hashCode() {
        return inetAddress.hashCode() ^ tcpPort ^ udpPort;
    }
}
