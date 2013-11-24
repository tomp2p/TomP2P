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

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * A class to search for addresses to bind the sockets to. The user first
 * creates a {@link Bindings} class and provides all the necesary information,
 * then calls {@link #discoverInterfaces(Bindings)}. The results are stored in
 * {@link Bindings} as well.
 * 
 * @author Thomas Bocek
 */
public final class DiscoverNetworks {
    
    /**
     * Empty constructor.
     */
    private DiscoverNetworks() { }
    /**
     * Search for local interfaces. Hints how to search for those interfaces are
     * provided by the user through the {@link Bindings} class. The results of
     * that search (InetAddress) are stored in {@link Bindings} as well.
     * 
     * @param bindings
     *            The hints for the search and also the results are stored there
     * @return The status of the search
     * @throws IOException
     *             If anything goes wrong, such as reflection.
     */
    public static String discoverInterfaces(final Bindings bindings) throws IOException {
        StringBuilder sb = new StringBuilder("Discover status: ");
        Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface networkInterface = e.nextElement();
            if (bindings.anyInterfaces()) {
                sb.append(" ++").append(networkInterface.getName());
                sb.append(discoverNetwork(networkInterface, bindings)).append(",");
            } else {
                if (bindings.containsInterface(networkInterface.getName())) {
                    sb.append(" +").append(networkInterface.getName());
                    sb.append(discoverNetwork(networkInterface, bindings)).append(",");
                } else {
                    sb.append(" -").append(networkInterface.getName()).append(",");
                }
            }
        }
        // remove the last comma or space
        sb.deleteCharAt(sb.length() - 1);
        return sb.append(".").toString();
    }

    /**
     * Discovers network interfaces and addresses.
     * 
     * @param networkInterface
     *            The networkInterface to search for addresses to listen to
     * @param bindings
     *            The search hints and result storage.
     * @return The status of the discovery
     */
    public static String discoverNetwork(final NetworkInterface networkInterface, final Bindings bindings) {
        StringBuilder sb = new StringBuilder("( ");
        for (InterfaceAddress iface : networkInterface.getInterfaceAddresses()) {
            // reported by Vasiliy:
            // iface == null happens when connecting to the Internet through
            // my mobile operator. In this case additional dial-up connection
            // is created in Network Connections (on Windows)
            if (iface == null) {
                continue;
            }
            InetAddress inet = iface.getAddress();
            if (iface.getBroadcast() != null && !bindings.broadcastAddresses().contains(iface.getBroadcast())) {
                bindings.broadcastAddresses().add(iface.getBroadcast());
            }
            if (bindings.containsAddress(inet)) {
                continue;
            }
            // ignore if a user specifies an address and inet is not part of it
            if (!bindings.anyAddresses()) {
                if (!bindings.addresses().contains(inet)) {
                    continue;
                }
            }

            if (inet instanceof Inet4Address && bindings.isIPv4()) {
                sb.append(inet).append(",");
                bindings.addFoundAddress(inet);
            } else if (inet instanceof Inet6Address && bindings.isIPv6()) {
                sb.append(inet).append(",");
                bindings.addFoundAddress(inet);
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.append(")").toString();
    }
}
