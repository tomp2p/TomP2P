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
     * Creates a new PeerSocketAddress including both UDP and TCP ports.
     * 
     * @param inetAddress
     *            The InetAddress of the peer. Can be IPv6 or IPv4
     * @param tcpPort
     *            The TCP port
     * @param udpPort
     *            The UDP port
     */
    public PeerSocketAddress(final InetAddress inetAddress, final int tcpPort, final int udpPort) {
        this(inetAddress, tcpPort, udpPort, -1);
    }

    /**
     * Creates a new PeerSocketAddress including both UDP and TCP ports. This is used mostly internally as the offset is
     * stored as well.
     * 
     * @param inetAddress
     *            The InetAddress of the peer. Can be IPv6 or IPv4
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
     * @return The IPv4 or IPv6 address.
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
     * @return Returns the offset.
     */
    public int offset() {
        return offset;
    }
    
    public byte[] toByteArray() {
    	int size = size();
    	byte[] retVal = new byte[size];
    	int size2 = toByteArray(retVal, 0);
    	if(size!=size2) {
    		throw new RuntimeException("sizes do not match");
    	}
    	return retVal;
    }
    
    public int size() {
    	return 2 + 2 + (isIPv4()?Utils.IPV4_BYTES:Utils.IPV6_BYTES);
    }
    
    public static int size(boolean isIPv4) {
    	return 2 + 2 + (isIPv4 ? Utils.IPV4_BYTES:Utils.IPV6_BYTES);
    }

    /**
     * Serializes a peer socket address to a byte array. First the ports are serialized: TCP and UDP, then the address.
     * 
     * @param me
     *            The byte array to store the serialization
     * @param offset
     *            The offset where to start
     * @return How many data have been written
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
    
    public boolean isIPv4() {
    	return inetAddress instanceof Inet4Address;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(inetAddress);
        if(tcpPort == udpPort) {
        	return sb.append(",").append(tcpPort).append("]").toString();
        } else {
        	return sb.append(",t:").append(tcpPort).append(",u:").append(udpPort).append("]").toString();
        }
    }

    /**
     * Converts an byte array into a peer socket address.
     * 
     * @param me
     *            The byte array
     * @param isIPv4
     *            Indicates if its IPv4 or IPv6
     * @param offsetOriginal
     *            The offset where to start reading in the array
     * @return the PeerSocketAddress and the new offset
     */
    public static PeerSocketAddress create(final byte[] me, final boolean isIPv4, final int offsetOriginal) {
        int offset = offsetOriginal;
        final int portTCP = ((me[offset++] & Utils.MASK_FF) << Utils.BYTE_BITS) + (me[offset++] & Utils.MASK_FF);
        final int portUDP = ((me[offset++] & Utils.MASK_FF) << Utils.BYTE_BITS) + (me[offset++] & Utils.MASK_FF);
        //
        final InetAddress address;
        if (isIPv4) {
            address = Utils.inet4FromBytes(me, offset);
            // IPv4 is 32 bit
            offset += Utils.IPV4_BYTES;
        } else {
            address = Utils.inet6FromBytes(me, offset);
            // IPv6 is 128 bit
            offset += Utils.IPV6_BYTES;
        }
        return new PeerSocketAddress(address, portTCP, portUDP, offset);
    }

    /**
     * Converts an ChannelBuffer into a peer socket address.
     * 
     * @param ch
     *            The channel buffer
     * @param isIPv4
     *            if the IP is v4 or v6
     * @return the PeerSocketAddress and the new offset
     */
    public static PeerSocketAddress create(final ByteBuf ch, final boolean isIPv4) {
        final int portTCP = ch.readUnsignedShort();
        final int portUDP = ch.readUnsignedShort();
        //
        final InetAddress address;
        final byte[] me;
        if (isIPv4) {
            // IPv4 is 32 bit
            me = new byte[Utils.IPV4_BYTES];
            ch.readBytes(me);
            address = Utils.inet4FromBytes(me, 0);
        } else {
            // IPv6 is 128 bit
            me = new byte[Utils.IPV6_BYTES];
            ch.readBytes(me);
            address = Utils.inet6FromBytes(me, 0);
        }
        return new PeerSocketAddress(address, portTCP, portUDP, ch.readerIndex());
    }
    
    /**
     * Returns the socket address.
     * 
     * @return The socket address how to reach this peer
     */
    public static InetSocketAddress createSocketTCP(PeerSocketAddress peerSocketAddress) {
        return new InetSocketAddress(peerSocketAddress.inetAddress(), peerSocketAddress.tcpPort());
    }

    /**
     * Returns the socket address.
     * 
     * @return The socket address how to reach this peer
     */
    public static InetSocketAddress createSocketUDP(PeerSocketAddress peerSocketAddress) {
        return new InetSocketAddress(peerSocketAddress.inetAddress(), peerSocketAddress.udpPort());
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
