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
public class Ports {
   
    // The number of maximum ports, 2^16.
    public static final int MAX_PORT = 65535;
    //IANA recommends to use ports higher than 49152.
    public static final int MIN_DYN_PORT = 49152;
    // The default port of TomP2P.
    public static final int DEFAULT_PORT = 7700;
    // IANA recommends to use ports higher than 49152
    private static final int RANGE = MAX_PORT - MIN_DYN_PORT;
    private static final Random RND = new Random();

    // provide this information if you know your mapping beforehand, i.e. manual
    // port-forwarding
    private final int externalTCPPort;
    private final int externalUDPPort;
    private final boolean randomPorts;

    /**
     * Creates a binding class that binds to everything.
     */
    public Ports() {
        this(0, 0);
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
    public Ports(final int externalTCPPort, final int externalUDPPort) {
        if (externalTCPPort < 0 || externalUDPPort < 0) {
            throw new IllegalArgumentException("port needs to be >= 0");
        }
        this.externalTCPPort = externalTCPPort == 0 ? (RND.nextInt(RANGE) + MIN_DYN_PORT) : externalTCPPort;
        this.externalUDPPort = externalUDPPort == 0 ? (RND.nextInt(RANGE) + MIN_DYN_PORT) : externalUDPPort;
        this.randomPorts = externalTCPPort == 0 && externalUDPPort == 0;
    }

    /**
     * @return Returns the external port, how other peers see us
     */
    public int externalTCPPort() {
        return externalTCPPort;
    }

    /**
     * @return Returns the external port, how other peers see us
     */
    public int externalUDPPort() {
        return externalUDPPort;
    }

    /**
     * @return True if the user specified both ports in advance. This tells us
     *         that the user knows about the ports and did a manual
     *         port-forwarding.
     */
    public boolean isSetExternalPortsManually() {
        // set setExternalPortsManually to true if the user specified both ports
        // in advance. This tells us that the user knows about the ports and did
        // a manual port-forwarding.
        return !randomPorts;
    }
}
