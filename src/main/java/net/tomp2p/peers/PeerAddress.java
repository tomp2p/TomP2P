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
import java.util.Arrays;

import com.google.common.net.InetAddresses;

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
	final private static int FIREWALL_UDP = 2;
	final private static int FIREWALL_TCP = 4;
	// final private static int RESERVED_1 = 8;
	final private static long serialVersionUID = -1316622724169272306L;
	final private Number160 id;
	// a peer can change its IP, so this is not final.
	final private InetAddress address;
	final private int portUDP;
	final private int portTCP;
	// connection info
	final private boolean net6;
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
	final private int readBytes;
	// one byte for type and two time two shorts for the ports, plus IP
	final public static int SIZE_IP_SOCKv6 = 1 + 4 + 16;
	final public static int SIZE_IP_SOCKv4 = 1 + 4 + 4;
	final public static int SIZE_IPv6 = SIZE_IP_SOCKv6 + Number160.BYTE_ARRAY_SIZE;
	final public static int SIZE_IPv4 = SIZE_IP_SOCKv4 + Number160.BYTE_ARRAY_SIZE;
	final public static PeerAddress EMPTY_IPv4 = new PeerAddress(new byte[SIZE_IPv4]);

	/**
	 * Creates a new peeraddress, where the byte array has to be in the rigth
	 * format and in the right size. The new offset can be accessed with
	 * offset().
	 * 
	 * @param me
	 *            The serialized array
	 * @throws UnknownHostException
	 *             Using InetXAddress.getByAddress creates this exception.
	 */
	public PeerAddress(final byte[] me)
	{
		this(me, 0);
	}

	/**
	 * Creates a PeerAddress from a continuous byte array. This is useful if you
	 * don't know the size beforehand. The new offset can be accessed with
	 * offset().
	 * 
	 * @param me
	 *            The serialized array
	 * @param offset
	 *            the offset, where to start
	 * @throws UnknownHostException
	 *             Using InetXAddress.getByAddress creates this exception.
	 */
	public PeerAddress(final byte[] me, int offset)
	{
		// get the peer ID, this is independent of the type
		final int offsetOld = offset;
		final byte tmp[] = new byte[Number160.BYTE_ARRAY_SIZE];
		System.arraycopy(me, offset, tmp, 0, Number160.BYTE_ARRAY_SIZE);
		this.id = new Number160(tmp);
		offset += Number160.BYTE_ARRAY_SIZE;
		// get the type
		final int options = me[offset] & 0xff;
		this.net6 = (options & NET6) > 0;
		this.firewalledUDP = (options & FIREWALL_UDP) > 0;
		this.firewalledTCP = (options & FIREWALL_TCP) > 0;
		offset++;
		// get the port for UDP and TCP
		this.portTCP = ((me[offset] & 0xff) << 8) + (me[offset + 1] & 0xff);
		this.portUDP = ((me[offset + 2] & 0xff) << 8) + (me[offset + 3] & 0xff);
		offset += 4;
		//
		final byte tmp2[];
		if (isIPv4())
		{
			// IPv4 is 32 bit
			tmp2 = new byte[4];
			System.arraycopy(me, offset, tmp2, 0, 4);
			InetAddresses.fromInteger(22);
			try 
			{
				this.address = Inet4Address.getByAddress(tmp2);
			} 
			catch (UnknownHostException e) 
			{
				/*
			     * This really shouldn't happen in practice since all our byte
			     * sequences have the right length.
			     *
			     * However {@link InetAddress#getByAddress} is documented as
			     * potentially throwing this "if IP address is of illegal length".
			     *
			     */
			    throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.", Arrays.toString(tmp2)), e);
			}
			offset += 4;
		}
		else
		{
			// IPv6 is 128 bit
			tmp2 = new byte[16];
			System.arraycopy(me, offset, tmp2, 0, 16);
			try 
			{
				this.address = Inet6Address.getByAddress(tmp2);
			} 
			catch (UnknownHostException e) 
			{
				/*
			     * This really shouldn't happen in practice since all our byte
			     * sequences have the right length.
			     *
			     * However {@link InetAddress#getByAddress} is documented as
			     * potentially throwing this "if IP address is of illegal length".
			     *
			     */
			    throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.", Arrays.toString(tmp2)), e);
			}
			offset += 16;
		}
		this.readBytes = offset - offsetOld;
		this.offset = offset;
		this.hashCode = id.hashCode();
	}

	public PeerAddress(Number160 id, final byte[] me, int offset)
	{
		final int offsetOld = offset;
		// get the peer ID, this is independent of the type
		this.id = id;
		// get the type
		final int options = me[offset] & 0xff;
		this.net6 = (options & NET6) > 0;
		this.firewalledUDP = (options & FIREWALL_UDP) > 0;
		this.firewalledTCP = (options & FIREWALL_TCP) > 0;
		offset++;
		// get the port for UDP and TCP
		this.portTCP = ((me[offset] & 0xff) << 8) + (me[offset + 1] & 0xff);
		this.portUDP = ((me[offset + 2] & 0xff) << 8) + (me[offset + 3] & 0xff);
		offset += 4;
		//
		final byte tmp2[];
		if (isIPv4())
		{
			// IPv4 is 32 bit
			tmp2 = new byte[4];
			System.arraycopy(me, offset, tmp2, 0, 4);
			try 
			{
				this.address = Inet4Address.getByAddress(tmp2);
			} 
			catch (UnknownHostException e) 
			{
				/*
			     * This really shouldn't happen in practice since all our byte
			     * sequences have the right length.
			     *
			     * However {@link InetAddress#getByAddress} is documented as
			     * potentially throwing this "if IP address is of illegal length".
			     *
			     */
			    throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.", Arrays.toString(tmp2)), e);
			}
			offset += 4;
		}
		else
		{
			// IPv6 is 128 bit
			tmp2 = new byte[16];
			System.arraycopy(me, offset, tmp2, 0, 16);
			try 
			{
				this.address = Inet6Address.getByAddress(tmp2);
			} 
			catch (UnknownHostException e) 
			{
				/*
			     * This really shouldn't happen in practice since all our byte
			     * sequences have the right length.
			     *
			     * However {@link InetAddress#getByAddress} is documented as
			     * potentially throwing this "if IP address is of illegal length".
			     *
			     */
			    throw new IllegalArgumentException(String.format("Host address '%s' is not a valid IPv4 address.", Arrays.toString(tmp2)), e);
			}
			offset += 16;
		}
		this.readBytes = offset - offsetOld;
		this.offset = offset;
		this.hashCode = id.hashCode();
	}

	/**
	 * The format of the peer address can also be split.
	 * 
	 * @param peerAddress
	 *            The 160bit number for the peer ID
	 * @param socketAddress
	 *            The socket address with type port and IP
	 * @throws UnknownHostException
	 */
	public PeerAddress(byte[] peerAddress, byte[] socketAddress)
	{
		this(new Number160(peerAddress), socketAddress, 0);
	}

	/**
	 * If you only need to know the id
	 * 
	 * @param id
	 *            The id of the peer
	 */
	public PeerAddress(Number160 id)
	{
		this(id, null, -1, -1);
	}
	
	/**
	 *  If you only need to know the id and InetAddress
	 * 
	 * @param id  The id of the peer
	 * @param address The InetAddress of the peer
	 */
	public PeerAddress(Number160 id, InetAddress address)
	{
		this(id, address, -1, -1);
	}

	/**
	 * Creates a PeerAddress
	 * 
	 * @param id
	 *            The id of the peer
	 * @param address
	 *            The address of the peer, how to reach this peer
	 * @param portTCP
	 *            The tcp port how to reach the peer
	 * @param portUDP
	 *            The udp port how to reach the peer
	 */
	public PeerAddress(Number160 id, InetAddress address, int portTCP, int portUDP, boolean firewalledUDP,
			boolean firewalledTCP)
	{
		this.id = id;
		this.address = address;
		this.portTCP = portTCP;
		this.portUDP = portUDP;
		this.hashCode = id.hashCode();
		this.net6 = address instanceof Inet6Address;
		this.firewalledUDP = firewalledUDP;
		this.firewalledTCP = firewalledTCP;
		// unused here
		this.offset = -1;
		this.readBytes = -1;
	}

	public PeerAddress(Number160 id, InetAddress address, int portTCP, int portUDP)
	{
		this(id, address, portTCP, portUDP, false, false);
	}

	public PeerAddress(Number160 id, InetSocketAddress inetSocketAddress)
	{
		this(id, inetSocketAddress.getAddress(), inetSocketAddress.getPort(), inetSocketAddress.getPort());
	}

	public PeerAddress(Number160 id, PeerAddress parent)
	{
		this(id, parent.address, parent.portTCP, parent.portUDP);
	}

	public PeerAddress(Number160 id, InetAddress address, int portTCP, int portUDP, int options)
	{
		this(id, address, portTCP, portUDP, isFirewalledUDP(options), isFirewalledTCP(options));
	}

	/**
	 * When deserializing, we need to know how much we deserialized from the
	 * constructor call.
	 * 
	 * @return The new offset
	 */
	public int offset()
	{
		return offset;
	}

	public int readBytes()
	{
		return readBytes;
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
			me = new byte[SIZE_IPv4];
		else
			me = new byte[SIZE_IPv6];
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
	 * @return The new offest.
	 */
	public int toByteArray(byte[] me, int offset)
	{
		// save the peer id
		int newOffset = id.toByteArray(me, offset);
		return toByteArraySocketAddress(me, newOffset);
	}

	/**
	 * Please use toByteArraySocketAddress
	 * 
	 * @return
	 */
	@Deprecated
	public byte[] getSocketAddress()
	{
		return toByteArraySocketAddress();
	}

	public byte[] toByteArraySocketAddress()
	{
		byte[] me = new byte[getSocketAddressSize()];
		toByteArraySocketAddress(me, 0);
		return me;
	}

	/**
	 * Please use toByteArraySocketAddress
	 * 
	 * @return
	 */
	@Deprecated
	public int getSocketAddress(byte[] me, int offset)
	{
		return toByteArraySocketAddress(me, offset);
	}

	public int toByteArraySocketAddress(byte[] me, int offset)
	{
		// save the type
		me[offset] = getOptions();
		int delta = 1;
		// save ports tcp
		int tmp = offset + delta;
		me[tmp + 0] = (byte) (portTCP >>> 8);
		me[tmp + 1] = (byte) portTCP;
		me[tmp + 2] = (byte) (portUDP >>> 8);
		me[tmp + 3] = (byte) portUDP;
		delta += 4;
		// save the ip address
		if (address instanceof Inet4Address)
		{
			System.arraycopy(address.getAddress(), 0, me, offset + delta, 4);
			delta += 4;
		}
		else
		{
			System.arraycopy(address.getAddress(), 0, me, offset + delta, 16);
			delta += 16;
		}
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

	public byte getOptions()
	{
		byte result = 0;
		if (net6)
			result |= NET6;
		if (firewalledUDP)
			result |= FIREWALL_UDP;
		if (firewalledTCP)
			result |= FIREWALL_TCP;
		return result;
	}

	public static boolean isNet6(int options)
	{
		return ((options & 0xff) & NET6) > 0;
	}

	public static boolean isFirewalledTCP(int options)
	{
		return ((options & 0xff) & FIREWALL_TCP) > 0;
	}

	public static boolean isFirewalledUDP(int options)
	{
		return ((options & 0xff) & FIREWALL_UDP) > 0;
	}

	public static int expectedLength(int options)
	{
		if (isNet6(options))
			return SIZE_IPv6;
		else
			return SIZE_IPv4;
	}

	public static int expectedSocketLength(int options)
	{
		if (isNet6(options))
			return SIZE_IP_SOCKv6;
		else
			return SIZE_IP_SOCKv4;
	}

	public int expectedLength()
	{
		return isIPv6() ? SIZE_IPv6 : SIZE_IPv4;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("PeerAddr[");
		return sb.append(address).append(",udp:").append(portUDP).append(",tcp:").append(portTCP).append(",ID:")
				.append(id.toString()).append("]").toString();
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

	public boolean isIPv4()
	{
		return !net6;
	}

	public PeerAddress changeFirewalledUDP(boolean status)
	{
		return new PeerAddress(id, address, portTCP, portUDP, status, firewalledTCP);
	}

	public PeerAddress changeFirewalledTCP(boolean status)
	{
		return new PeerAddress(id, address, portTCP, portUDP, firewalledUDP, status);
	}

	public PeerAddress changePorts(int portUDP, int portTCP)
	{
		return new PeerAddress(id, address, portTCP, portUDP, firewalledUDP, firewalledTCP);
	}
	
	public PeerAddress changeAddress(InetAddress address)
	{
		return new PeerAddress(id, address, portTCP, portUDP, firewalledUDP, firewalledTCP);
	}

	public PeerAddress changePeerId(Number160 id2)
	{
		return new PeerAddress(id2, address, portTCP, portUDP, firewalledUDP, firewalledTCP);
	}

	public int getSocketAddressSize()
	{
		return (address instanceof Inet4Address) ? SIZE_IP_SOCKv4 : SIZE_IP_SOCKv6;
	}
}
