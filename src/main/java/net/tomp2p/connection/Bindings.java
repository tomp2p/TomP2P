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
 * preferences to which addresses to bind the socket. This class contains two
 * types of information: 1.) the interface/address to listen for incoming
 * connections and 2.) how other peers see us. The default is to listen to all
 * interfaces and our outside address is set to the first interface it finds. If
 * more than one search hint is used, then the combination operation will be
 * "and"
 * 
 * @author Thomas Bocek
 */
public class Bindings {
    /**
     * Types of protocols. This was a boolean, but there needs to be the type
     * "Any".
     */
    public enum Protocol {
        /**
         * 32 bit IP version 4.
         */
        IPv4,
        /**
         * 128 bit IP version 6.
         */
        IPv6,
        /**
         * This type indicates that the address to bind to can be either IPv4 or
         * IPv6.
         */
        Any
    };

    /**
     * The number of maximum ports, 2^16.
     */
    public static final int MAX_PORT = 65535;

    /**
     * IANA recommends to use ports higher than 49152.
     */
    public static final int MIN_DYN_PORT = 49152;

    /**
     * The default port of TomP2P.
     */
    public static final int DEFAULT_PORT = 7700;

    private static final Random RND = new Random();

    // IANA recommends to use ports higher than 49152
    private static final int RANGE = MAX_PORT - MIN_DYN_PORT;

    // This can be set by the user. The discover process will not use this field
    // to store anything
    private final List<InetAddress> listenAddresses = new ArrayList<InetAddress>(1);

    private final List<String> listenInterfaceHints = new ArrayList<String>(1);

    private final Protocol listenProtocolHint;

    // provide this information if you know your mapping beforehand, i.e. manual
    // port-forwarding
    private final InetAddress externalAddress;

    private final int externalTCPPort;

    private final int externalUDPPort;

    private final boolean setExternalPortsManually;

    // here are the found address stored. This is not set by the user
    private final List<InetAddress> foundBroadcastAddresses = new ArrayList<InetAddress>(1);

    // here are the found address stored. This is not set by the user
    private final List<Inet4Address> foundAddresses4 = new ArrayList<Inet4Address>(1);

    // here are the found address stored. This is not set by the user
    private final List<Inet6Address> foundAddresses6 = new ArrayList<Inet6Address>(1);

    /**
     * Creates a binding class that binds to everything.
     */
    public Bindings() {
        this(Protocol.Any, null, 0, 0);
    }

    /**
     * Creates a binding class that binds to a specified address.
     * 
     * @param bind
     *            the address to bind to
     */
    public Bindings(final InetAddress bind) {
        this(Protocol.Any, null, 0, 0);
        addAddress(bind);
    }

    /**
     * Creates a Binding class that binds to a specified interface.
     * 
     * @param iface
     *            The interface to bind to
     */
    public Bindings(final String iface) {
        this(Protocol.Any, null, 0, 0);
        addInterface(iface);
    }

    /**
     * Creates a Binding class that binds to a specified protocol.
     * 
     * @param protocol
     *            The protocol to bind to
     */
    public Bindings(final Protocol protocol) {
        this(protocol, null, 0, 0);
    }

    /**
     * Creates a Binding class that binds to a specified protocol and interface.
     * 
     * @param protocol
     *            The protocol to bind to
     * @param iface
     *            The interface to bind to
     */
    public Bindings(final Protocol protocol, final String iface) {
        this(protocol, null, 0, 0);
        addInterface(iface);
    }

    /**
     * Creates a Binding class that binds to a specified protocol and interface
     * and address.
     * 
     * @param protocol
     *            The protocol to bind to
     * @param iface
     *            The interface to bind to
     * @param bind
     *            The address to bind to
     */
    public Bindings(final Protocol protocol, final String iface, final InetAddress bind) {
        this(protocol, null, 0, 0);
        addInterface(iface);
    }

    /**
     * Creates a Binding class that binds to everything and provides information
     * about manual port forwarding.
     * 
     * @param externalAddress
     *            The external address, how other peers will see us
     * @param externalTCPPort
     *            The external port, how other peers will see us
     * @param externalUDPPort
     *            The external port, how other peers will see us
     */
    public Bindings(final InetAddress externalAddress, final int externalTCPPort, final int externalUDPPort) {
        this(Protocol.Any, externalAddress, externalTCPPort, externalUDPPort);
    }

    /**
     * Creates a Binding class that binds to a specified protocol and provides
     * information about manual port forwarding.
     * 
     * @param protocol
     *            The protocol to bind to
     * @param externalAddress
     *            The external address, how other peers will see us. Use null if
     *            you don't want to use external address
     * @param externalTCPPort
     *            The external port, how other peers will see us, if 0 is
     *            provided, a random port will be used
     * @param externalUDPPort
     *            The external port, how other peers will see us, if 0 is
     *            provided, a random port will be used
     */
    public Bindings(final Protocol protocol, final InetAddress externalAddress, 
            final int externalTCPPort, final int externalUDPPort) {
        if (externalTCPPort < 0 || externalUDPPort < 0) {
            throw new IllegalArgumentException("port needs to be >= 0");
        }
        this.externalAddress = externalAddress;
        this.externalTCPPort = externalTCPPort == 0 ? (RND.nextInt(RANGE) + MIN_DYN_PORT) : externalTCPPort;
        this.externalUDPPort = externalUDPPort == 0 ? (RND.nextInt(RANGE) + MIN_DYN_PORT) : externalUDPPort;
        // set setExternalPortsManually to true if the user specified both ports
        // in advance. This tells us that the user knows about the ports and did
        // a manual port-forwarding.
        this.setExternalPortsManually = externalUDPPort != 0 && externalTCPPort != 0;
        this.listenProtocolHint = protocol;
    }

    /**
     * Adds an address that we want to listen to. If the address is not found,
     * it will be ignored
     * 
     * @param address
     *            The current class
     * @return The bindings (this)
     */
    Bindings addFoundAddress(final InetAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("Cannot add null");
        }
        if (address instanceof Inet4Address) {
            foundAddresses4.add((Inet4Address) address);
        } else if (address instanceof Inet6Address) {
            foundAddresses6.add((Inet6Address) address);
        } else {
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
    public List<InetAddress> getFoundAddresses() {
        // first return ipv4, then ipv6
        List<InetAddress> listenAddresses2 = new ArrayList<InetAddress>();
        listenAddresses2.addAll(foundAddresses4);
        listenAddresses2.addAll(foundAddresses6);
        return listenAddresses2;
    }

    /**
     * Adds an address that we want to listen to. If the address is not found,
     * it will be ignored
     * 
     * @param address
     *            The current class
     * @return this instance
     */
    public Bindings addAddress(final InetAddress address) {
        listenAddresses.add(address);
        return this;
    }

    /**
     * @return A list of the addresses provided by the user
     */
    public List<InetAddress> getAddresses() {
        return listenAddresses;
    }

    /**
     * @return A list of broadcast addresses.
     */
    public List<InetAddress> getBroadcastAddresses() {
        return foundBroadcastAddresses;
    }

    /**
     * Adds an interface that will be searched for. If the interface is not
     * found, it will be ignored
     * 
     * @param interfaceHint
     *            The interface, e.g. eth0
     * @return The same instance
     */
    public Bindings addInterface(final String interfaceHint) {
        if (interfaceHint == null) {
            throw new IllegalArgumentException("Cannot add null");
        }
        listenInterfaceHints.add(interfaceHint);
        return this;
    }

    /**
     * @return A list of interfaces to listen to
     */
    public List<String> getInterfaces() {
        return listenInterfaceHints;
    }

    /**
     * @return The protocol to listen to
     */
    public Protocol getProtocol() {
        return listenProtocolHint;
    }

    /**
     * Clears all lists: listenInterfaceHints, listenAddresses,
     * broadcastAddresses.
     */
    public void clear() {
        listenInterfaceHints.clear();
        listenAddresses.clear();
        foundAddresses4.clear();
        foundAddresses6.clear();
        foundBroadcastAddresses.clear();
    }

    /**
     * @return Checks if the user sets any addresses
     */
    public boolean isAllAddresses() {
        return listenAddresses.size() == 0;
    }

    /**
     * @return Checks if the user sets any interfaces
     */
    public boolean isAllInterfaces() {
        return listenInterfaceHints.size() == 0;
    }

    /**
     * @return Checks if the user sets any protocols
     */
    public boolean isAllProtocols() {
        return listenProtocolHint == Protocol.Any;
    }

    /**
     * @return Checks if the user sets protocol to anything or IPv4
     */
    public boolean isIPv4() {
        return isAllProtocols() || listenProtocolHint == Protocol.IPv4;
    }

    /**
     * @return Checks if the user sets protocol to anything or IPv6
     */
    public boolean isIPv6() {
        return isAllProtocols() || listenProtocolHint == Protocol.IPv6;
    }

    /**
     * @return Checks if the user sets anything at all
     */
    public boolean isListenAll() {
        return isAllProtocols() && isAllInterfaces() && isAllAddresses();
    }

    /**
     * Checks if the user provided an interface hint.
     * 
     * @param name
     *            The name of the interface reported by the system
     * @return True if the user added the interface
     */
    public boolean containsInterface(final String name) {
        return listenInterfaceHints.contains(name);
    }

    /**
     * @return Checks if the user set the external address
     */
    public boolean isExternalAddress() {
        return externalAddress != null && externalTCPPort != 0 && externalUDPPort != 0;
    }

    /**
     * @return Returns the externalAddress, how other peers see us
     */
    public InetAddress getExternalAddress() {
        return externalAddress;
    }

    /**
     * @return Returns the external port, how other peers see us
     */
    public int getOutsideTCPPort() {
        return externalTCPPort;
    }

    /**
     * @return Returns the external port, how other peers see us
     */
    public int getOutsideUDPPort() {
        return externalUDPPort;
    }

    /**
     * Adds the results from an other binding. This is useful because you can
     * add within one Bindings hints only with "and", with add() you have the
     * option "or" as well. E.g., Bindings b1 = new Bindings(IPv4, eth0);
     * Bindings b2 = new Bindings(IPv6, eth1); b2.add(b1) -> this will bind to
     * all IPv4 addresses on eth0 and all IPv6 addresses on eth1
     * 
     * @param other
     *            The other instance to get the results from
     * @return The same instance
     */
    public Bindings add(final Bindings other) {
        this.foundAddresses4.addAll(other.foundAddresses4);
        this.foundAddresses6.addAll(other.foundAddresses6);
        this.foundBroadcastAddresses.addAll(other.foundBroadcastAddresses);
        return this;
    }

    /**
     * @return True if the user specified both ports in advance. This tells us
     *         that the user knows about the ports and did a manual
     *         port-forwarding.
     */
    public boolean isSetExternalPortsManually() {
        return setExternalPortsManually;
    }
}
