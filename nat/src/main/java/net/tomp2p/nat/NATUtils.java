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

package net.tomp2p.nat;

import java.io.IOException;
import java.net.InetAddress;

import javax.xml.parsers.ParserConfigurationException;

import net.tomp2p.natpmp.Gateway;
import net.tomp2p.natpmp.MapRequestMessage;
import net.tomp2p.natpmp.NatPmpDevice;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.natpmp.ResultCode;

import org.bitlet.weupnp.GatewayDevice;
import org.bitlet.weupnp.GatewayDiscover;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * This class is used to do automatic port forwarding. It maps with PMP und UPNP
 * and also unmaps them. It creates a shutdown hook in case the user exits the
 * application without a proper shutdown.
 * 
 * @author Thomas Bocek
 */
public class NATUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(NATUtils.class);

	// UPNP
	private GatewayDevice gatewayDevice;
	private int externalPortUDP = -1;
	// NAT-PMP
	private NatPmpDevice pmpDevice;

	private final Object shutdownLock = new Object();

	/**
	 * Constructor.
	 */
	public NATUtils() {
		shutdownHookEnabled();
	}

	/**
	 * Maps with the PMP protocol
	 * (http://en.wikipedia.org/wiki/NAT_Port_Mapping_Protocol). One of the
	 * drawbacks of this protocol is that it needs to know the IP of the router.
	 * To get the IP of the router in Java, we need to use netstat and parse the
	 * output.
	 * 
	 * @param internalPortUDP
	 *            The UDP internal port
	 * @param internalPortTCP
	 *            The TCP internal port
	 * @param externalPortUDP
	 *            The UDP external port
	 * @param externalPortTCP
	 *            The TCP external port
	 * @return True, if the mapping was successful (i.e. the router supports
	 *         PMP)
	 * @throws NatPmpException
	 *             the router does not supports PMP
	 */
	public boolean mapPMP(final int internalPortUDP, final int externalPortUDP) throws NatPmpException {
		InetAddress gateway = Gateway.getIP();
		pmpDevice = new NatPmpDevice(gateway);
		
		MapRequestMessage mapUDP = new MapRequestMessage(false, internalPortUDP, externalPortUDP, Integer.MAX_VALUE, null);
		pmpDevice.enqueueMessage(mapUDP);
		pmpDevice.waitUntilQueueEmpty();
		// UDP is nice to have, but we need TCP
		return mapUDP.getResultCode() == ResultCode.Success;
	}

	/**
	 * Maps with UPNP protocol
	 * (http://en.wikipedia.org/wiki/Internet_Gateway_Device_Protocol). Since
	 * this uses broadcasting to discover routers, no calling the external
	 * program netstat is necessary.
	 * 
	 * @param internalHost
	 *            The internal host to map the ports to
	 * @param internalPortUDP
	 *            The UDP internal port
	 * @param internalPortTCP
	 *            The TCP internal port
	 * @param externalPortUDP
	 *            The UDP external port
	 * @param externalPortTCP
	 *            The TCP external port
	 * @return True, if at least one mapping was successful (i.e. the router
	 *         supports UPNP)
	 * @throws IOException
	 *             Exception
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 */
	public boolean mapUPNP(final String internalHost, final int internalPortUDP, final int externalPortUDP) 
			throws IOException, SAXException, ParserConfigurationException {
		// -1 sets the default timeout to 1500 ms

		if (gatewayDevice != null) {
			gatewayDevice.deletePortMapping(this.externalPortUDP, "UDP");
		}
		GatewayDiscover discover = new GatewayDiscover();
		discover.discover();
		gatewayDevice = discover.getValidGateway();

		if (gatewayDevice == null) {
			LOGGER.info("no UPNP device found");
			return false;
		}

		this.externalPortUDP = externalPortUDP;

		boolean mapUDP = gatewayDevice.addPortMapping(externalPortUDP, internalPortUDP, internalHost, "UDP",
		        "TomP2P mapping UDP");

		if (mapUDP) {
			return true;
		} else {
			if (!mapUDP) {
				LOGGER.warn("UPNP UDP mapping did failed");
			}
			return false;
		}
	}

	/**
	 * Unmap the device that has been mapped previously. Used during shutdown.
	 * 
	 * @throws SAXException
	 * @throws IOException
	 */
	private void unmapUPNP() throws SAXException, IOException {
		if (gatewayDevice != null) {
			try {
				boolean unmapUDP = gatewayDevice.deletePortMapping(this.externalPortUDP, "UDP");
				if (!unmapUDP) {
					LOGGER.warn("UPNP UDP unmapping did failed");
				}
			} finally {
				gatewayDevice = null;
			}
		}
	}

	/**
	 * Registers a shutdownhook to clean the NAT mapping. If this is not called,
	 * then the mapping may stay until the router is rebooted.
	 */
	private void shutdownHookEnabled() {
		// The shutdown hook simply runs the shutdown method.
		Thread t = new Thread(new Runnable() {
			public void run() {
				shutdown();
			}
		}, "TomP2P:NATUtils:ShutdownHook");
		Runtime.getRuntime().addShutdownHook(t);
	}

	/**
	 * Since shutdown is also called from the shutdown hook, it might get called
	 * twice. Thus, this method deregister NAT mappings only once. If it already
	 * has been called, this method does nothing.
	 */
	public void shutdown() {
		synchronized (shutdownLock) {
			try {
				unmapUPNP();
			} catch (Exception e) {
				LOGGER.error("UPNP UDP unmapping did failed", e);
			}
			if (pmpDevice != null) {
				pmpDevice.shutdown();
				pmpDevice = null;
			}
		}
	}
}
