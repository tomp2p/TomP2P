/*
 * ====================================================================
 * ======== The Apache Software License, Version 1.1
 * ==================
 * ==========================================================
 * Copyright (C) 2002 The Apache Software Foundation. All rights
 * reserved. Redistribution and use in source and binary forms, with
 * or without modifica- tion, are permitted provided that the
 * following conditions are met: 1. Redistributions of source code
 * must retain the above copyright notice, this list of conditions and
 * the following disclaimer. 2. Redistributions in binary form must
 * reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other
 * materials provided with the distribution. 3. The end-user
 * documentation included with the redistribution, if any, must
 * include the following acknowledgment: "This product includes
 * software developed by SuperBonBon Industries
 * (http://www.sbbi.net/)." Alternately, this acknowledgment may
 * appear in the software itself, if and wherever such third-party
 * acknowledgments normally appear. 4. The names "UPNPLib" and
 * "SuperBonBon Industries" must not be used to endorse or promote
 * products derived from this software without prior written
 * permission. For written permission, please contact info@sbbi.net.
 * 5. Products derived from this software may not be called
 * "SuperBonBon Industries", nor may "SBBI" appear in their name,
 * without prior written permission of SuperBonBon Industries. THIS
 * SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR ITS
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLU- DING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE. This software consists of voluntary contributions made
 * by many individuals on behalf of SuperBonBon Industries. For more
 * information on SuperBonBon Industries, please see
 * <http://www.sbbi.net/>.
 */

package net.tomp2p.upnp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This class can be used to access some functionalities on the
 * InternetGatewayDevice on your network without having to know anything about
 * the required input/output parameters. All device functions are not provided.
 * 
 * @author <a href="mailto:superbonbon@sbbi.net">SuperBonBon</a>
 * @version 1.0
 */
public class InternetGatewayDevice {

    private RootDevice igd;

    private UPNPMessageFactory msgFactory;

    private InternetGatewayDevice(RootDevice igd, boolean WANIPConnection, boolean WANPPPConnection)
            throws UnsupportedOperationException {
        this.igd = igd;
        Device myIGDWANConnDevice = igd.getChildDevice("urn:schemas-upnp-org:device:WANConnectionDevice:1");
        if (myIGDWANConnDevice == null) {
            throw new UnsupportedOperationException(
                    "device urn:schemas-upnp-org:device:WANConnectionDevice:1 not supported by IGD device "
                            + igd.modelName);
        }

        Service wanIPSrv = myIGDWANConnDevice.getService("urn:schemas-upnp-org:service:WANIPConnection:1");
        Service wanPPPSrv = myIGDWANConnDevice.getService("urn:schemas-upnp-org:service:WANPPPConnection:1");

        if (WANIPConnection && WANPPPConnection && wanIPSrv == null && wanPPPSrv == null) {
            throw new UnsupportedOperationException(
                    "Unable to find any urn:schemas-upnp-org:service:WANIPConnection:1 or urn:schemas-upnp-org:service:WANPPPConnection:1 service");
        } else if (WANIPConnection && !WANPPPConnection && wanIPSrv == null) {
            throw new UnsupportedOperationException(
                    "Unable to find any urn:schemas-upnp-org:service:WANIPConnection:1 service");
        } else if (!WANIPConnection && WANPPPConnection && wanPPPSrv == null) {
            throw new UnsupportedOperationException(
                    "Unable to find any urn:schemas-upnp-org:service:WANPPPConnection:1 service");
        }

        if (wanIPSrv != null && wanPPPSrv == null) {
            msgFactory = new UPNPMessageFactory(wanIPSrv);
        } else if (wanPPPSrv != null && wanIPSrv == null) {
            msgFactory = new UPNPMessageFactory(wanPPPSrv);
        } else {
            // discover the active WAN interface using the
            // WANCommonInterfaceConfig specs
            Device wanDevice = igd.getChildDevice("urn:schemas-upnp-org:device:WANDevice:1");
            Service configService = wanDevice.getService("urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1");

            if (configService != null) { // retrieve the first active connection
                Action act = configService.getUPNPServiceAction("GetActiveConnection");
                if (act != null) { // this action is optional - definitely not
                                   // supported by
                                   // the NetGear WGR614v9
                    UPNPMessageFactory msg = new UPNPMessageFactory(configService);
                    String deviceContainer = null;
                    String serviceID = null;
                    try { // always lookup for the first index of active
                          // connections.
                        ActionResponse resp = msg.getMessage("GetActiveConnection")
                                .setInputParameter("NewActiveConnectionIndex", 0).service();
                        deviceContainer = resp.getOutActionArgumentValue("NewActiveConnDeviceContainer");
                        serviceID = resp.getOutActionArgumentValue("NewActiveConnectionServiceID");
                    } catch (IOException ex) { // no response returned
                    } catch (UPNPResponseException respEx) { // should never
                                                             // happen unless
                                                             // the damn
                                                             // thing is
                                                             // bugged
                    }
                    if (deviceContainer != null && deviceContainer.trim().length() > 0 && serviceID != null
                            && serviceID.trim().length() > 0) {
                        for (Iterator<Device> i = igd.getChildDevices().iterator(); i.hasNext();) {
                            Device dv = i.next();
                            if (deviceContainer.startsWith(dv.UDN)
                                    && dv.deviceType.indexOf(":WANConnectionDevice:") != -1) {
                                myIGDWANConnDevice = dv;
                                break;
                            }
                        }
                        msgFactory = new UPNPMessageFactory(myIGDWANConnDevice.getServiceByID(serviceID));
                    }
                } else {
                    // Lets look at the required WANAccessType variable
                    StateVariable wat = configService.getUPNPServiceStateVariable("WANAccessType");
                    assert wat != null : "bugged upnp implementation";
                    try {
                        String accessType = wat.getValue();
                        System.out.println(accessType);
                        if (accessType.equals("DSL") || accessType.equals("POTS")) {
                            msgFactory = new UPNPMessageFactory(wanPPPSrv);
                        } else if (accessType.equals("Cable") || accessType.equals("Ethernet")) {
                            msgFactory = new UPNPMessageFactory(wanIPSrv);
                        } else {
                            assert false : "Illegal access type : " + accessType;
                        }
                    } catch (UPNPResponseException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (msgFactory == null) { // last-ditch attempt
                                      // Doing a tricky test with external IP
                                      // address, the
                                      // unactive
                                      // interface should return a null value
                                      // or none
                if (testWANInterface(wanIPSrv)) {
                    msgFactory = new UPNPMessageFactory(wanIPSrv);
                } else if (testWANInterface(wanPPPSrv)) {
                    msgFactory = new UPNPMessageFactory(wanPPPSrv);
                }
            }

            if (msgFactory == null) {
                // Nothing found using WANCommonInterfaceConfig! IP by
                // default
                // log
                // .warn(
                // "Unable to detect active WANIPConnection, dfaulting to urn:schemas-upnp-org:service:WANIPConnection:1"
                // );
                msgFactory = new UPNPMessageFactory(wanIPSrv);
            }
        }
    }

    private boolean testWANInterface(Service srv) {
        UPNPMessageFactory tmp = new UPNPMessageFactory(srv);

        ActionMessage msg = tmp.getMessage("GetExternalIPAddress");
        String ipToParse = null;
        try {
            ipToParse = msg.service().getOutActionArgumentValue("NewExternalIPAddress");
        } catch (UPNPResponseException ex) {
            // ok probably not the IP interface
        } catch (IOException ex) {
            // not really normal
            // log.warn( "IOException occurred during device detection",
            // ex );
        }
        if (ipToParse != null && ipToParse.length() > 0 && !ipToParse.equals("0.0.0.0")) {
            try {
                return InetAddress.getByName(ipToParse) != null;
            } catch (UnknownHostException ex) {
                // ok a crappy IP provided, definitely the wrong
                // interface..
            }
        }
        return false;
    }

    /**
     * Retrieves the IDG UNPNRootDevice object
     * 
     * @return the UNPNRootDevie object bound to this object
     */
    public RootDevice getIGDRootDevice() {
        return igd;
    }

    /**
     * Lookup all the IGD (IP or PPP) devices on the network. If a device
     * implements both IP and PPP, the active service will be used for nat
     * mappings.
     * 
     * @param timeout
     *            the timeout in ms to listen for devices response, -1 for
     *            default value
     * @return an array of devices to play with or null if nothing found.
     * @throws IOException
     *             if some IO Exception occurs during discovery
     */
    public static Collection<InternetGatewayDevice> getDevices(int timeout) throws IOException {
        return lookupDeviceDevices(timeout, Discovery.DEFAULT_TTL, Discovery.DEFAULT_MX, true, true, null);
    }

    /**
     * Lookup all the IGD (IP urn:schemas-upnp-org:service:WANIPConnection:1, or
     * PPP urn:schemas-upnp-org:service:WANPPPConnection:1) devices for a given
     * network interface. If a device implements both IP and PPP, the active
     * service will be used for nat mappings.
     * 
     * @param timeout
     *            the timeout in ms to listen for devices response, -1 for
     *            default value
     * @param ttl
     *            the discovery ttl such as
     *            {@link net.tomp2p.upnp.Discovery#DEFAULT_TTL}
     * @param mx
     *            the discovery mx such as
     *            {@link net.tomp2p.upnp.Discovery#DEFAULT_MX}
     * @param ni
     *            the network interface where to lookup IGD devices
     * @return an array of devices to play with or null if nothing found.
     * @throws IOException
     *             if some IO Exception occurs during discovery
     */
    public static Collection<InternetGatewayDevice> getDevices(int timeout, int ttl, int mx, NetworkInterface ni)
            throws IOException {
        return lookupDeviceDevices(timeout, ttl, mx, true, true, ni);
    }

    private static Collection<InternetGatewayDevice> lookupDeviceDevices(int timeout, int ttl, int mx,
            boolean WANIPConnection, boolean WANPPPConnection, NetworkInterface ni) throws IOException {
        Collection<RootDevice> devices = Discovery.discover(timeout == -1 ? Discovery.DEFAULT_TIMEOUT : timeout, ttl,
                mx, "urn:schemas-upnp-org:device:InternetGatewayDevice:1", ni);

        if (devices == null)
            return null;

        Set<InternetGatewayDevice> valid = new HashSet<InternetGatewayDevice>();
        for (RootDevice device : devices) {
            try {
                valid.add(new InternetGatewayDevice(device, WANIPConnection, WANPPPConnection));
            } catch (UnsupportedOperationException ex) {
                // the device is either not IP or PPP

                // log.debug(
                // "UnsupportedOperationException during discovery " +
                // ex.getMessage() );
            }
        }
        if (valid.size() == 0) {
            return null;
        }
        return valid;
    }

    /**
     * Retrieves the external IP address
     * 
     * @return a String representing the external IP
     * @throws UPNPResponseException
     *             if the devices returns an error code
     * @throws IOException
     *             if some error occurs during communication with the device
     */
    public String getExternalIPAddress() throws UPNPResponseException, IOException {
        ActionMessage msg = msgFactory.getMessage("GetExternalIPAddress");
        return msg.service().getOutActionArgumentValue("NewExternalIPAddress");
    }

    /**
     * Retrieves a generic port mapping entry.
     * 
     * @param newPortMappingIndex
     *            the index to lookup in the nat table of the upnp device
     * @return an action response Object containing the following fields :
     *         NewRemoteHost, NewExternalPort, NewProtocol, NewInternalPort,
     *         NewInternalClient, NewEnabled, NewPortMappingDescription,
     *         NewLeaseDuration or null if the index does not exists
     * @throws IOException
     *             if some error occurs during communication with the device
     * @throws UPNPResponseException
     *             if some unexpected error occurs on the UPNP device
     */
    public ActionResponse getGenericPortMappingEntry(int newPortMappingIndex) throws IOException, UPNPResponseException {

        ActionMessage msg = msgFactory.getMessage("GetGenericPortMappingEntry");
        msg.setInputParameter("NewPortMappingIndex", newPortMappingIndex);

        try {
            return msg.service();
        } catch (UPNPResponseException ex) {
            if (ex.getDetailErrorCode() == 714) {
                return null;
            }
            throw ex;
        }

    }

    /**
     * Retrieves information about a specific port mapping
     * 
     * @param remoteHost
     *            the remote host ip to check, null if wildcard
     * @param externalPort
     *            the port to check
     * @param protocol
     *            the protocol for the mapping, either TCP or UDP
     * @return an action response Object containing the following fields :
     *         NewInternalPort, NewInternalClient, NewEnabled,
     *         NewPortMappingDescription, NewLeaseDuration or null if no such
     *         entry exists in the device NAT table
     * @throws IOException
     *             if some error occurs during communication with the device
     * @throws UPNPResponseException
     *             if some unexpected error occurs on the UPNP device
     */
    public ActionResponse getSpecificPortMappingEntry(String remoteHost, int externalPort, String protocol)
            throws IOException, UPNPResponseException {
        remoteHost = remoteHost == null ? "" : remoteHost;
        checkPortMappingProtocol(protocol);
        checkPortRange(externalPort);

        ActionMessage msg = msgFactory.getMessage("GetSpecificPortMappingEntry");
        msg.setInputParameter("NewRemoteHost", remoteHost).setInputParameter("NewExternalPort", externalPort)
                .setInputParameter("NewProtocol", protocol);

        try {
            return msg.service();
        } catch (UPNPResponseException ex) {
            if (ex.getDetailErrorCode() == 714) {
                return null;
            }
            throw ex;
        }
    }

    public boolean addPortMapping(String description, String protocol, String internalHost, int externalPort,
            int internalPort) throws IOException, UPNPResponseException {
        // we need to be sure that igd.getLocalIP().getHostAddress() is in the
        // network of internalHost
        return addPortMapping(description, protocol, null, externalPort, internalHost, internalPort, 0);
    }

    /**
     * Configures a nat entry on the UPNP device.
     * 
     * @param description
     *            the mapping description, null for no description
     * @param protocol
     *            the protocol, either TCP or UDP
     * @param remoteHost
     *            the remote host ip for this entry, null for a wildcard value
     * @param externalPort
     *            the external port to open on the UPNP device an map on the
     *            internal client, 0 for a wildcard value
     * @param internalClient
     *            the internal client ip where data should be redirected
     * @param internalPort
     *            the internal client port where data should be redirected
     * @param leaseDuration
     *            the lease duration in seconds 0 for an infinite time
     * @return true if the port is mapped false if the mapping is already done
     *         for another internal client
     * @throws IOException
     *             if some error occurs during communication with the device
     * @throws UPNPResponseException
     *             if the device does not accept some settings :<br/>
     *             402 Invalid Args See UPnP Device Architecture section on
     *             Control<br/>
     *             501 Action Failed See UPnP Device Architecture section on
     *             Control<br/>
     *             715 WildCardNotPermittedInSrcIP The source IP address cannot
     *             be wild-carded<br/>
     *             716 WildCardNotPermittedInExtPort The external port cannot be
     *             wild-carded <br/>
     *             724 SamePortValuesRequired Internal and External port values
     *             must be the same<br/>
     *             725 OnlyPermanentLeasesSupported The NAT implementation only
     *             supports permanent lease times on port mappings<br/>
     *             726 RemoteHostOnlySupportsWildcard RemoteHost must be a
     *             wildcard and cannot be a specific IP address or DNS name<br/>
     *             727 ExternalPortOnlySupportsWildcard ExternalPort must be a
     *             wildcard and cannot be a specific port value
     */
    public boolean addPortMapping(String description, String protocol, String remoteHost, int externalPort,
            String internalClient, int internalPort, int leaseDuration) throws IOException, UPNPResponseException {
        remoteHost = remoteHost == null ? "" : remoteHost;
        checkPortMappingProtocol(protocol);
        if (externalPort != 0) {
            checkPortRange(externalPort);
        }
        checkPortRange(internalPort);
        description = description == null ? "" : description;
        if (leaseDuration < 0) {
            throw new IllegalArgumentException("Invalid leaseDuration (" + leaseDuration + ") value");
        }

        ActionMessage msg = msgFactory.getMessage("AddPortMapping");
        msg.setInputParameter("NewRemoteHost", remoteHost).setInputParameter("NewExternalPort", externalPort)
                .setInputParameter("NewProtocol", protocol).setInputParameter("NewInternalPort", internalPort)
                .setInputParameter("NewInternalClient", internalClient).setInputParameter("NewEnabled", true)
                .setInputParameter("NewPortMappingDescription", description)
                .setInputParameter("NewLeaseDuration", leaseDuration);
        try {
            msg.service();
            return true;
        } catch (UPNPResponseException ex) {
            if (ex.getDetailErrorCode() == 718) {
                return false;
            }
            throw ex;
        }
    }

    /**
     * Deletes a port mapping on the IDG device
     * 
     * @param remoteHost
     *            the host ip for which the mapping was done, null value for a
     *            wildcard value
     * @param externalPort
     *            the port to close
     * @param protocol
     *            the protocol for the mapping, TCP or UDP
     * @return true if the port has been unmapped correctly otherwise false (
     *         entry does not exists ).
     * @throws IOException
     *             if some error occurs during communication with the device
     * @throws UPNPResponseException
     *             if the devices returns an error message
     */
    public boolean deletePortMapping(String remoteHost, int externalPort, String protocol) throws IOException,
            UPNPResponseException {

        remoteHost = remoteHost == null ? "" : remoteHost;
        checkPortMappingProtocol(protocol);
        checkPortRange(externalPort);
        ActionMessage msg = msgFactory.getMessage("DeletePortMapping");
        msg.setInputParameter("NewRemoteHost", remoteHost).setInputParameter("NewExternalPort", externalPort)
                .setInputParameter("NewProtocol", protocol);
        try {
            msg.service();
            return true;
        } catch (UPNPResponseException ex) {
            if (ex.getDetailErrorCode() == 714) {
                return false;
            }
            throw ex;
        }
    }

    /**
     * Retrieves the current number of mapping in the NAT table
     * 
     * @return the nat table current number of mappings or null if the device
     *         does not allow to query state variables
     * @throws IOException
     *             if some error occurs during communication with the device
     * @throws UPNPResponseException
     *             if the devices returns an error message with error code other
     *             than 404
     */
    public Integer getNatMappingsCount() throws IOException, UPNPResponseException {

        Integer rtrval = null;
        StateVariableMessage natTableSize = msgFactory.getStateVariableMessage("PortMappingNumberOfEntries");
        try {
            StateVariableResponse resp = natTableSize.service();
            rtrval = new Integer(resp.getStateVariableValue());
        } catch (UPNPResponseException ex) {
            // 404 can happen if device do not implement state variables
            // queries
            if (ex.getDetailErrorCode() != 404) {
                throw ex;
            }
        }
        return rtrval;
    }

    private void checkPortMappingProtocol(String prot) throws IllegalArgumentException {
        if (prot == null || !prot.equals("TCP") && !prot.equals("UDP")) {
            throw new IllegalArgumentException("PortMappingProtocol must be either TCP or UDP");
        }
    }

    private void checkPortRange(int port) throws IllegalArgumentException {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port range must be between 1 and 65535");
        }
    }

}
