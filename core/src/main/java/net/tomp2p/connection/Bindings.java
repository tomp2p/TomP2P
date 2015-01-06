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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Gathers information about interface bindings. Here a user can set the
 * preferences to which addresses to bind the socket. This class contains two
 * types of information: 1.) the interface/address to listen for incoming
 * connections and 2.) how other peers see us. The default is to listen to all
 * interfaces and our outside address is set to the first interface it finds. If
 * more than one search hint is used, then the combination operation will be
 * "and".
 * 
 * @author Thomas Bocek
 */
public class Bindings {

    // This can be set by the user. The discover process will not use this field
    // to store anything
    private final List<InetAddress> addresses = new ArrayList<InetAddress>(1);
    private final List<String> interfaceHints = new ArrayList<String>(1);
    private final List<StandardProtocolFamily> protocolHint = new ArrayList<StandardProtocolFamily>(1);

    // here are the found address stored. This is not set by the user
    private final List<InetAddress> foundBroadcastAddresses = new ArrayList<InetAddress>(1);
    private final List<Inet4Address> foundAddresses4 = new ArrayList<Inet4Address>(1);
    private final List<Inet6Address> foundAddresses6 = new ArrayList<Inet6Address>(1);

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
    
    boolean containsAddress(final InetAddress address) {
        return foundAddresses4.contains(address) || foundAddresses6.contains(address);
    }

    /**
     * Returns a list of InetAddresses to listen to. First Inet4Addresses, then
     * Inet6Addresses are present in the list. This returns the matching
     * addresses from DiscoverNetworks.
     * 
     * @return A list of InetAddresses to listen to
     */
    public List<InetAddress> foundAddresses() {
        // first return ipv4, then ipv6
        final List<InetAddress> listenAddresses = new ArrayList<InetAddress>(foundAddresses4.size() + foundAddresses6.size());
        listenAddresses.addAll(foundAddresses4);
        listenAddresses.addAll(foundAddresses6);
        return listenAddresses;
    }
    
    public InetAddress foundAddress() {
    	List<InetAddress> addresses = foundAddresses();
    	return addresses.isEmpty() ? null : addresses.get(0);
    }
    
    /**
     * @return A list of broadcast addresses.
     */
    public List<InetAddress> broadcastAddresses() {
        return foundBroadcastAddresses;
    }

    /**
     * Adds an address that we want to listen to. If the address is not found,
     * it will be ignored
     * 
     * @param address
     *            
     * @return this instance
     */
    public Bindings addAddress(final InetAddress address) {
        addresses.add(address);
        return this;
    }

    /**
     * @return A list of the addresses provided by the user
     */
    public List<InetAddress> addresses() {
        return addresses;
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
        interfaceHints.add(interfaceHint);
        return this;
    }
    
    public Bindings addProtocol(StandardProtocolFamily protocolFamily) {
        if (protocolFamily == null) {
            throw new IllegalArgumentException("Cannot add null");
        }
        protocolHint.add(protocolFamily);
        return this;
    }

    /**
     * @return A list of interfaces to listen to
     */
    public List<String> interfaceHints() {
        return interfaceHints;
    }

    /**
     * @return The protocol to listen to
     */
    public List<StandardProtocolFamily> protocolHint() {
        return protocolHint;
    }

    /**
     * Clears all lists.
     */
    public void clear() {
        interfaceHints.clear();
        addresses.clear();
        foundAddresses4.clear();
        foundAddresses6.clear();
        foundBroadcastAddresses.clear();
    }

    /**
     * @return Checks if the user sets any addresses
     */
    public boolean anyAddresses() {
        return addresses.size() == 0;
    }

    /**
     * @return Checks if the user sets any interfaces
     */
    public boolean anyInterfaces() {
        return interfaceHints.size() == 0;
    }

    /**
     * @return Checks if the user sets any protocols
     */
    public boolean anyProtocols() {
        return protocolHint.size() == 0;
    }

    /**
     * @return Checks if the user sets protocol to anything or IPv4
     */
    public boolean isIPv4() {
        return anyProtocols() || protocolHint.contains(StandardProtocolFamily.INET);
    }

    /**
     * @return Checks if the user sets protocol to anything or IPv6
     */
    public boolean isIPv6() {
        return anyProtocols() || protocolHint.contains(StandardProtocolFamily.INET6);
    }

    /**
     * @return Checks if the user sets anything at all
     */
    public boolean isListenAll() {
        return anyProtocols() && anyInterfaces() && anyAddresses();
    }

    /**
     * Checks if the user provided an interface hint.
     * 
     * @param name
     *            The name of the interface reported by the system
     * @return True if the user added the interface
     */
    public boolean containsInterface(final String name) {
        return interfaceHints.contains(name);
    }

    /**
     * Adds the results from another binding. This is useful because you can
     * add within one Bindings hints only with "and", with add() you have the
     * option "or" as well. E.g., Bindings b1 = new Bindings(IPv4, eth0);
     * Bindings b2 = new Bindings(IPv6, eth1); b2.add(b1) -> this will bind to
     * all IPv4 addresses on eth0 and all IPv6 addresses on eth1
     * 
     * @param other
     *            The other instance to get the results from
     * @return This class
     */
    public Bindings add(final Bindings other) {
        this.foundAddresses4.addAll(other.foundAddresses4);
        this.foundAddresses6.addAll(other.foundAddresses6);
        this.foundBroadcastAddresses.addAll(other.foundBroadcastAddresses);
        return this;
    }

    public SocketAddress wildCardSocket() {
        if(!isListenAll()) {
            if(foundAddresses4.size()>0) {
                return new InetSocketAddress(foundAddresses4.get(0), 0);
            }
            if(foundAddresses6.size()>0) {
                return new InetSocketAddress(foundAddresses6.get(0), 0);
            }
        }
        return new InetSocketAddress(0);
    }
}
