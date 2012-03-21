/*
 * Copyright 2011 Thomas Bocek
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
package net.tomp2p.connection;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Gathers information about interface bindings. Here a user can set the
 * preferences to which addresses to bind the socket.
 * 
 * This class contains two types of information: 1.) the interface/address to
 * listen for incoming connections and 2.) how other peers see us. The default
 * is to listen to all interfaces and our outside address is set to the first
 * interface it finds.
 * 
 * If more than one search hint is used, then the combination operation will be
 * "and"
 * 
 * @author Thomas Bocek
 * 
 */
public class Bindings
{
	final public static int DEFAULT_PORT = 7700;
	final private static Random RND = new Random();
	// IANA recommends to use ports higher than 49152
	final private static int RANGE = 65535 - 49152;
	public enum Protocol
	{
		IPv4, IPv6, Any
	};
	// set this to true to ignore the specific addresses to listen to
	final private List<InetAddress> listenAddresses = new ArrayList<InetAddress>(1);
	// results are stored here
	final private List<InetAddress> broadcastAddresses = new ArrayList<InetAddress>(1);
	final private List<Inet4Address> listenAddresses4 = new ArrayList<Inet4Address>(1);
	final private List<Inet6Address> listenAddresses6 = new ArrayList<Inet6Address>(1);
	final private List<String> listenInterfaceHints = new ArrayList<String>(1);
	final private Protocol listenProtocolHint;
	// provide this information if you know your mapping beforehand, i.e. manual
	// port-forwarding
	final private InetAddress externalAddress;
	final private int externalTCPPort;
	final private int externalUDPPort;

	/**
	 * Creates a Binding class that binds to everything
	 */
	public Bindings()
	{
		this(Protocol.Any, null, 0, 0);
	}

	/**
	 * Creates a Binding class that binds to a specified address
	 * 
	 * @param bind The address to bind to
	 */
	public Bindings(InetAddress bind)
	{
		this(Protocol.Any, null, 0, 0);
		addAddress(bind);
	}

	/**
	 * Creates a Binding class that binds to a specified interface
	 * 
	 * @param iface The interface to bind to
	 */
	public Bindings(String iface)
	{
		this(Protocol.Any, null, 0, 0);
		addInterface(iface);
	}

	/**
	 * Creates a Binding class that binds to a specified protocol
	 * 
	 * @param protocol The protocol to bind to
	 */
	public Bindings(Protocol protocol)
	{
		this(protocol, null, 0, 0);
	}

	/**
	 * Creates a Binding class that binds to a specified protocol and interface
	 * 
	 * @param protocol The protocol to bind to
	 * @param iface The interface to bind to
	 */
	public Bindings(Protocol protocol, String iface)
	{
		this(protocol, null, 0, 0);
		addInterface(iface);
	}

	/**
	 * Creates a Binding class that binds to a specified protocol and interface
	 * and address
	 * 
	 * @param protocol The protocol to bind to
	 * @param iface The interface to bind to
	 * @param bind The address to bind to
	 */
	public Bindings(Protocol protocol, String iface, InetAddress bind)
	{
		this(protocol, null, 0, 0);
		addInterface(iface);
	}

	/**
	 * Creates a Binding class that binds to everything and provides information
	 * about manual port forwarding
	 * 
	 * @param externalAddress The external address, how other peers will see us
	 * @param externalTCPPort The external port, how other peers will see us
	 * @param externalUDPPort The external port, how other peers will see us
	 */
	public Bindings(InetAddress externalAddress, int externalTCPPort, int externalUDPPort)
	{
		this(Protocol.Any, externalAddress, externalTCPPort, externalUDPPort);
	}

	/**
	 * Creates a Binding class that binds to a specified protocol and provides
	 * information about manual port forwarding
	 * 
	 * @param protocol The protocol to bind to
	 * @param externalAddress The external address, how other peers will see us.
	 *        Use null if you don't want to use external address
	 * @param externalTCPPort The external port, how other peers will see us, if
	 *        0 is provided, a random port will be used
	 * @param externalUDPPort The external port, how other peers will see us, if
	 *        0 is provided, a random port will be used
	 */
	public Bindings(Protocol protocol, InetAddress externalAddress, int externalTCPPort,
			int externalUDPPort)
	{
		if (externalTCPPort < 0 || externalUDPPort < 0)
			throw new IllegalArgumentException("port needs to be >= 0");
		this.externalAddress = externalAddress;
		this.externalTCPPort = externalTCPPort == 0 ? (RND.nextInt(RANGE) + 49152)
				: externalTCPPort;
		this.externalUDPPort = externalUDPPort == 0 ? (RND.nextInt(RANGE) + 49152)
				: externalUDPPort;
		this.listenProtocolHint = protocol;
	}

	/**
	 * Adds an address that we want to listen to. If the address is not found,
	 * it will be ignored
	 * 
	 * @param address The current class
	 */
	Bindings addFoundAddress(InetAddress address)
	{
		if (address == null)
		{
			throw new IllegalArgumentException("Cannot add null");
		}
		if (address instanceof Inet4Address)
		{
			listenAddresses4.add((Inet4Address) address);
		}
		else if (address instanceof Inet6Address)
		{
			listenAddresses6.add((Inet6Address) address);
		}
		else
		{
			throw new IllegalArgumentException("Unknown address family " + address.getClass());
		}
		return this;
	}

	/**
	 * Returns a list of InetAddresses to listen to. First Inet4Addresses, then
	 * Inet6Addresses are present in the list. This returns the matching
	 * addresses from DiscoverNetworks.
	 * 
	 * @return A list of InetAddresses to listen to
	 */
	public List<InetAddress> getFoundAddresses()
	{
		// first return ipv4, then ipv6
		List<InetAddress> listenAddresses = new ArrayList<InetAddress>();
		listenAddresses.addAll(listenAddresses4);
		listenAddresses.addAll(listenAddresses6);
		return listenAddresses;
	}

	/**
	 * Adds an address that we want to listen to. If the address is not found,
	 * it will be ignored
	 * 
	 * @param address The current class
	 */
	public Bindings addAddress(InetAddress address)
	{
		listenAddresses.add(address);
		return this;
	}

	/**
	 * @return A list of the addresses provided by the user
	 */
	public List<InetAddress> getAddresses()
	{
		return listenAddresses;
	}

	/**
	 * Adds an broadcast address. This is used internally, but if you have Java
	 * 1.5, you may need to set this manually.
	 * 
	 * @param broadcastAddress Adds a broadcast address used for pinging other
	 *        peers on layer 2.
	 * @return The current class
	 */
	public Bindings addBroadcastAddress(InetAddress broadcastAddress)
	{
		if (broadcastAddress == null)
			throw new IllegalArgumentException("Cannot add null");
		broadcastAddresses.add(broadcastAddress);
		return this;
	}

	/**
	 * @return A list of broadcast addresses. If using Java 1.6, this is
	 *         automatically filled.
	 */
	public List<InetAddress> getBroadcastAddresses()
	{
		return broadcastAddresses;
	}

	/**
	 * Adds an interface that will be searched for. If the interface is not
	 * found, it will be ignored
	 * 
	 * @param interfaceHint The interface, e.g. eth0
	 * @return The same instance
	 */
	public Bindings addInterface(String interfaceHint)
	{
		if (interfaceHint == null)
			throw new IllegalArgumentException("Cannot add null");
		listenInterfaceHints.add(interfaceHint);
		return this;
	}

	/**
	 * @return A list of interfaces to listen to
	 */
	public List<String> getInterfaces()
	{
		return listenInterfaceHints;
	}

	/**
	 * @return The protocol to listen to
	 */
	public Protocol getProtocol()
	{
		return listenProtocolHint;
	}

	/**
	 * Clears all lists: listenInterfaceHints, listenAddresses,
	 * broadcastAddresses.
	 */
	public void clear()
	{
		listenInterfaceHints.clear();
		listenAddresses.clear();
		listenAddresses4.clear();
		listenAddresses6.clear();
		broadcastAddresses.clear();
	}

	/**
	 * @return Checks if the user sets any addresses
	 */
	public boolean isAllAddresses()
	{
		return listenAddresses.size() == 0;
	}

	/**
	 * @return Checks if the user sets any interfaces
	 */
	public boolean isAllInterfaces()
	{
		return listenInterfaceHints.size() == 0;
	}

	/**
	 * @return Checks if the user sets any protocols
	 */
	public boolean isAllProtocols()
	{
		return listenProtocolHint == Protocol.Any;
	}

	/**
	 * @return Checks if the user sets protocol to anything or IPv4
	 */
	public boolean isIPv4()
	{
		return isAllProtocols() || listenProtocolHint == Protocol.IPv4;
	}

	/**
	 * @return Checks if the user sets protocol to anything or IPv6
	 */
	public boolean isIPv6()
	{
		return isAllProtocols() || listenProtocolHint == Protocol.IPv6;
	}

	/**
	 * @return Checks if the user sets anything at all
	 */
	public boolean isListenAll()
	{
		return isAllProtocols() && isAllInterfaces() && isAllAddresses();
	}

	/**
	 * Checks if the user provided an interface hint
	 * 
	 * @param name The name of the interface reported by the system
	 * @return True if the user added the interface
	 */
	public boolean containsInterface(String name)
	{
		return listenInterfaceHints.contains(name);
	}

	/**
	 * @return Checks if the user set the external address
	 */
	public boolean isExternalAddress()
	{
		return externalAddress != null && externalTCPPort != 0 && externalUDPPort != 0;
	}

	/**
	 * @return Returns the externalAddress, how other peers see us
	 */
	public InetAddress getExternalAddress()
	{
		return externalAddress;
	}

	/**
	 * @return Returns the external port, how other peers see us
	 */
	public int getOutsideTCPPort()
	{
		return externalTCPPort;
	}

	/**
	 * @return Returns the external port, how other peers see us
	 */
	public int getOutsideUDPPort()
	{
		return externalUDPPort;
	}

	/**
	 * Adds the results from an other binding. This is useful because you can
	 * add within one Bindings hints only with "and", with add() you have the
	 * option "or" as well.
	 * 
	 * E.g., Bindings b1 = new Bindings(IPv4, eth0); Bindings b2 = new
	 * Bindings(IPv6, eth1); b2.add(b1) -> this will bind to all IPv4 addresses
	 * on eth0 and all IPv6 addresses on eth1
	 * 
	 * @param other The other instance to get the results from
	 * @return The same instance
	 */
	public Bindings add(Bindings other)
	{
		this.listenAddresses4.addAll(other.listenAddresses4);
		this.listenAddresses6.addAll(other.listenAddresses6);
		this.broadcastAddresses.addAll(other.broadcastAddresses);
		return this;
	}
}