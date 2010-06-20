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

/**
 * A PeerAddress contains the node ID and how to contact this node using both
 * TCP and UDP. This class is thread safe (or it does not matter if its not).
 * The serialized size of this class is for IPv4 (20 + 4 + 1 + 4=29), for IPv6
 * (20 + 4 + 1 + 16=41)
 * 
 * @author Thomas Bocek
 * 
 */
public final class PeerAddress implements Comparable<PeerAddress>, Serializable
{
	final private static int NET6 = 1;
	final private static int FORWARD = 2;
	final private static int FIREWALL_UDP = 4;
	final private static int FIREWALL_TCP = 8;
	// final private static int RESERVED_3 = 16;
	// final private static int RESERVED_4 = 32;
	// final private static int RESERVED_5 = 64;
	final private static long serialVersionUID = -1316622724169272306L;
	final private Number160 id;
	// a peer can change its IP, so this is not final.
	final private InetAddress address;
	final private int portUDP;
	final private int portTCP;
	// connection info
	final private boolean net6;
	final private boolean forwarded;
	final private boolean firewalledUDP;
	final private boolean firewalledTCP;
	// we can make the hash final as it never changes, and this class is used
	// multiple times in maps. A new peer is always added to the peermap, so
	// making this final saves CPU time. The maps used are removedPeerCache,
	// that is always checked, peerMap that is either added or checked if
	// already present. Also peers from the neighbor list sent over the wire are
	// added to the peermap.
	final private int hashCode;
	// if deserialized from a byte array using the constructor, then we need to
	// report how many data we processed.
	final private int offset;
	// 5 is one byte for type and two time two shorts for the ports
	final public static int SIZE_IPv6 = 16 + 5 + Number160.BYTE_ARRAY_SIZE;
	final public static int SIZE_IPv4 = 4 + 5 + Number160.BYTE_ARRAY_SIZE;

	/**
	 * Creates a new peeraddress, where the byte array has to be in the rigth
	 * format and in the rigth size. The new offset can be accessed with
	 * offset().
	 * 
	 * @param me The serialized array
	 * @throws UnknownHostException Using InetXAddress.getByAddress creates this
	 *         exception.
	 */
	public PeerAddress(final byte[] me) throws UnknownHostException
	{
		this(me, 0);
	}

	/**
	 * Creates a PeerAddress from a continuous byte array. This is useful if you
	 * don't know the size beforehand. The new offset can be accessed with
	 * offset().
	 * 
	 * @param me The serialized array
	 * @param offset the offset, where to start
	 * @throws UnknownHostException Using InetXAddress.getByAddress creates this
	 *         exception.
	 */
	public PeerAddress(final byte[] me, int offset) throws UnknownHostException
	{
		byte tmp[] = new byte[Number160.BYTE_ARRAY_SIZE];
		System.arraycopy(me, offset, tmp, 0, Number160.BYTE_ARRAY_SIZE);
		this.id = new Number160(tmp);
		this.portTCP = ((me[offset + Number160.BYTE_ARRAY_SIZE] & 0xff) << 8)
				+ (me[offset + Number160.BYTE_ARRAY_SIZE + 1] & 0xff);
		this.portUDP = ((me[offset + Number160.BYTE_ARRAY_SIZE + 2] & 0xff) << 8)
				+ (me[offset + Number160.BYTE_ARRAY_SIZE + 3] & 0xff);
		int types = me[offset + Number160.BYTE_ARRAY_SIZE + 4] & 0xff;
		//
		this.net6 = (types & NET6) > 0;
		this.forwarded = (types & FORWARD) > 0;
		this.firewalledUDP = (types & FIREWALL_UDP) > 0;
		this.firewalledTCP = (types & FIREWALL_TCP) > 0;
		//
		if (!isIPv6())
		{
			// IPv4
			tmp = new byte[4];
			System.arraycopy(me, offset + Number160.BYTE_ARRAY_SIZE + 5, tmp, 0, 4);
			this.address = Inet4Address.getByAddress(tmp);
			this.offset = offset + Number160.BYTE_ARRAY_SIZE + 5 + 4;
		}
		else
		{
			// IPv6
			tmp = new byte[16];
			System.arraycopy(me, offset + Number160.BYTE_ARRAY_SIZE + 5, tmp, 0, 16);
			this.address = Inet6Address.getByAddress(tmp);
			this.offset = offset + Number160.BYTE_ARRAY_SIZE + 5 + 16;
		}
		this.hashCode = id.hashCode();
	}

	/**
	 * This is usually used for debugging, the address will be null and ports -1
	 * 
	 * @param id The id of the peer
	 */
	public PeerAddress(Number160 id)
	{
		this(id, null, -1, -1);
	}

	/**
	 * Creates a PeerAddress
	 * 
	 * @param id The id of the peer
	 * @param address The address of the peer, how to reach this peer
	 * @param portTCP The tcp port how to reach the peer
	 * @param portUDP The udp port how to reach the peer
	 */
	public PeerAddress(Number160 id, InetAddress address, int portTCP, int portUDP,
			boolean forwarded, boolean firewalledUDP, boolean firewalledTCP)
	{
		this.id = id;
		this.address = address;
		this.portTCP = portTCP;
		this.portUDP = portUDP;
		this.hashCode = id.hashCode();
		this.net6 = address instanceof Inet6Address;
		this.forwarded = forwarded;
		this.firewalledUDP = firewalledUDP;
		this.firewalledTCP = firewalledTCP;
		// unused here
		this.offset = -1;
	}

	public PeerAddress(Number160 id, InetAddress address, int portTCP, int portUDP)
	{
		this(id, address, portTCP, portUDP, false, false, false);
	}

	public PeerAddress(Number160 id, InetSocketAddress inetSocketAddress)
	{
		this(id, inetSocketAddress.getAddress(), inetSocketAddress.getPort(), inetSocketAddress
				.getPort());
	}

	public PeerAddress(Number160 id, PeerAddress parent)
	{
		this(id, parent.address, parent.portTCP, parent.portUDP);
	}

	public PeerAddress(Number160 id, InetAddress address, int portTCP, int portUDP, byte optionType)
	{
		this(id, address, portTCP, portUDP, isForward(optionType), isFirewalledUDP(optionType),
				isFirewalledTCP(optionType));
	}

	/**
	 * When deserializeg, we need to know how much we deserialized from the
	 * constructor call.
	 * 
	 * @return The new offset
	 */
	public int offset()
	{
		return offset;
	}

	/**
	 * Serializes to a new array with the proper size
	 * 
	 * @return The serialized representation.
	 */
	public byte[] toByteArray()
	{
		byte[] me;
		if (address instanceof Inet4Address)
			me = new byte[20 + 4 + 1 + 4];
		else
			me = new byte[20 + 4 + 1 + 16];
		toByteArray(me, 0);
		return me;
	}

	/**
	 * Serializes to an existing array
	 * 
	 * @param me The array where the result should be stored
	 * @param offset The offset where to start to save the result in the byte
	 *        array
	 * @return The new offest.
	 */
	public int toByteArray(byte[] me, int offset)
	{
		int delta;
		if (address instanceof Inet4Address)
		{
			me[offset + Number160.BYTE_ARRAY_SIZE + 4] = createType();
			System
					.arraycopy(address.getAddress(), 0, me, offset + Number160.BYTE_ARRAY_SIZE + 5,
							4);
			delta = Number160.BYTE_ARRAY_SIZE + 5 + 4;
		}
		else
		{
			me[offset + Number160.BYTE_ARRAY_SIZE + 4] = createType();
			System.arraycopy(address.getAddress(), 0, me, offset + Number160.BYTE_ARRAY_SIZE + 5,
					16);
			delta = Number160.BYTE_ARRAY_SIZE + 5 + 16;
		}
		System.arraycopy(id.toByteArray(), 0, me, offset + 0, Number160.BYTE_ARRAY_SIZE);
		// save ports tcp
		me[offset + Number160.BYTE_ARRAY_SIZE + 0] = (byte) (portTCP >>> 8);
		me[offset + Number160.BYTE_ARRAY_SIZE + 1] = (byte) portTCP;
		me[offset + Number160.BYTE_ARRAY_SIZE + 2] = (byte) (portUDP >>> 8);
		me[offset + Number160.BYTE_ARRAY_SIZE + 3] = (byte) portUDP;
		return offset + delta;
	}

	/**
	 * Returns the address or null if no address set
	 * 
	 * @return The address of this peer
	 */
	public InetAddress getInetAddress()
	{
		return address;
	}

	/**
	 * Returns the socket address. The socket address will be created if
	 * necessary.
	 * 
	 * @return The socket address how to reach this peer
	 */
	public InetSocketAddress createSocketTCP()
	{
		return new InetSocketAddress(address, portTCP);
	}

	/**
	 * Returns the socket address. The socket address will be created if
	 * necessary.
	 * 
	 * @return The socket address how to reach this peer
	 */
	public InetSocketAddress createSocketUDP()
	{
		return new InetSocketAddress(address, portUDP);
	}

	/**
	 * The id of the peer. A peer cannot change its id.
	 * 
	 * @return Id of the peer
	 */
	public Number160 getID()
	{
		return id;
	}

	public byte createType()
	{
		byte result = 0;
		if (net6)
			result |= NET6;
		if (forwarded)
			result |= FORWARD;
		if (firewalledUDP)
			result |= FIREWALL_UDP;
		if (firewalledTCP)
			result |= FIREWALL_TCP;
		return result;
	}

	public static boolean isNet6(int type)
	{
		return ((type & 0xff) & NET6) > 0;
	}

	public static boolean isForward(int type)
	{
		return ((type & 0xff) & FORWARD) > 0;
	}

	public static boolean isFirewalledTCP(int type)
	{
		return ((type & 0xff) & FIREWALL_TCP) > 0;
	}

	public static boolean isFirewalledUDP(int type)
	{
		return ((type & 0xff) & FIREWALL_UDP) > 0;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("PeerAddr[");
		return sb.append(address).append(",udp:").append(portUDP).append(",tcp:").append(portTCP)
				.append(",ID:").append(id.toString()).append("]").toString();
	}

	@Override
	public int compareTo(PeerAddress nodeAddress)
	{
		// the id determines if two peer are equal, the address does not matter
		return id.compareTo(nodeAddress.id);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj instanceof PeerAddress)
			return id.equals(((PeerAddress) obj).id);
		else
			return false;
	}

	@Override
	public int hashCode()
	{
		// dont calculate all the time, only once.
		return this.hashCode;
	}

	/**
	 * @return TCP port
	 */
	public int portTCP()
	{
		return portTCP;
	}

	/**
	 * @return UDP port
	 */
	public int portUDP()
	{
		return portUDP;
	}

	public boolean isForwarded()
	{
		return forwarded;
	}

	public boolean isFirewalledUDP()
	{
		return firewalledUDP;
	}

	public boolean isFirewalledTCP()
	{
		return firewalledTCP;
	}

	public boolean isIPv6()
	{
		return net6;
	}

	public PeerAddress notFirewalledUDP()
	{
		return new PeerAddress(id, address, portTCP, portUDP, forwarded, false, firewalledTCP);
	}

	public PeerAddress notFirewalledTCP()
	{
		return new PeerAddress(id, address, portTCP, portUDP, forwarded, firewalledUDP, false);
	}

	public PeerAddress forward(InetAddress inetAddress)
	{
		return new PeerAddress(id, inetAddress, portTCP, portUDP, true, firewalledUDP, firewalledTCP);
	}
}
