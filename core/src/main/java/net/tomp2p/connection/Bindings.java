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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

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

    // This can be set by the user. The discover process will not use this field
    // to store anything
    private final List<InetAddress> addresses = new ArrayList<InetAddress>(1);
    private final List<String> interfaceHints = new ArrayList<String>(1);
    private final List<StandardProtocolFamily> protocolHint = new ArrayList<StandardProtocolFamily>(1);

    private boolean listenAny = false;
    
    /**
     * Adds an address that we want to listen to. If the address is not found,
     * it will be ignored
     * 
     * @param address
     *            The current class
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

    /**
     * 
     * @param protocolFamily
     *              The protocol family, e.g. StandardProtocolFamily.INET or StandardProtocolFamily.INET6
     * 
     * @return The same instance
     */
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
     * Clears all lists: listenInterfaceHints, listenAddresses,
     * broadcastAddresses.
     */
    public void clear() {
        interfaceHints.clear();
        addresses.clear();
        protocolHint.clear();
    }

    /**
     * @return Checks if the user has set interfaces to anything (not set to a specific)
     */
    public boolean anyInterfaces() {
        return interfaceHints.isEmpty();
    }

    /**
     * @return Checks if the user has set protocols to anything (not set to a specific)
     */
    public boolean anyProtocols() {
        return protocolHint.isEmpty();
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
     * @return Checks if the user wants to listen to a wildcard address
     */
    public boolean isListenAny() {
        return listenAny;
    }
    
    public Bindings listenAny() {
    	setListenAny(true);
    	return this;
    }
    
    public Bindings setListenAny(boolean listenAny) {
    	this.listenAny = listenAny;
    	return this;
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
}
