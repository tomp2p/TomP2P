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
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.natpmp.Gateway;
import net.tomp2p.natpmp.MapRequestMessage;
import net.tomp2p.natpmp.NatPmpDevice;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.natpmp.ResultCode;
import net.tomp2p.upnp.InternetGatewayDevice;
import net.tomp2p.upnp.UPNPResponseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to do automatic port forwarding. It maps with PMP und UPNP and also unmaps them. It creates a
 * shutdown hook in case the user exits the application without a proper shutdown.
 * 
 * @author Thomas Bocek
 */
public class NATUtils
{
    final private static Logger logger = LoggerFactory.getLogger( NATUtils.class );

    // UPNP
    final private Map<InternetGatewayDevice, Integer> internetGatewayDevicesUDP =
        new HashMap<InternetGatewayDevice, Integer>();

    final private Map<InternetGatewayDevice, Integer> internetGatewayDevicesTCP =
        new HashMap<InternetGatewayDevice, Integer>();

    // NAT-PMP
    private NatPmpDevice pmpDevice;

    private final Object shutdownLock = new Object();

    private boolean runOnce = false;

    public NATUtils()
    {
        setShutdownHookEnabled();
    }

    /**
     * Maps with the PMP protocol (http://en.wikipedia.org/wiki/NAT_Port_Mapping_Protocol). One of the drawbacks of this
     * protocol is that it needs to know the IP of the router. To get the IP of the router in Java, we need to use
     * netstat and parse the output.
     * 
     * @param internalPortUDP The UDP internal port
     * @param internalPortTCP The TCP internal port
     * @param externalPortUDP The UDP external port
     * @param externalPortTCP The TCP external port
     * @return True, if the mapping was successful (i.e. the router supports PMP)
     * @throws NatPmpException the router does not supports PMP
     */
    public boolean mapPMP( int internalPortUDP, int internalPortTCP, int externalPortUDP, int externalPortTCP )
        throws NatPmpException
    {
        InetAddress gateway = Gateway.getIP();
        pmpDevice = new NatPmpDevice( gateway );
        MapRequestMessage mapTCP =
            new MapRequestMessage( true, internalPortTCP, externalPortTCP, Integer.MAX_VALUE, null );
        MapRequestMessage mapUDP =
            new MapRequestMessage( false, internalPortUDP, externalPortUDP, Integer.MAX_VALUE, null );
        pmpDevice.enqueueMessage( mapTCP );
        pmpDevice.enqueueMessage( mapUDP );
        pmpDevice.waitUntilQueueEmpty();
        // UDP is nice to have, but we need TCP
        return mapTCP.getResultCode() == ResultCode.Success;
    }

    /**
     * Maps with UPNP protocol (http://en.wikipedia.org/wiki/Internet_Gateway_Device_Protocol). Since this uses
     * broadcasting to discover routern, no calling the external program netstat is necessary.
     * 
     * @param internalHost The internal host to map the ports to
     * @param internalPortUDP The UDP internal port
     * @param internalPortTCP The TCP internal port
     * @param externalPortUDP The UDP external port
     * @param externalPortTCP The TCP external port
     * @return True, if at least one mapping was successful (i.e. the router supports UPNP)
     * @throws IOException
     */
    public boolean mapUPNP( String internalHost, int internalPortUDP, int internalPortTCP, int externalPortUDP,
                            int externalPortTCP )
        throws IOException
    {
        // -1 sets the default timeout to 1500 ms
        Collection<InternetGatewayDevice> IGDs = InternetGatewayDevice.getDevices( -1 );
        if ( IGDs == null )
            return false;
        boolean once = false;
        for ( InternetGatewayDevice igd : IGDs )
        {
            logger.info( "Found device " + igd );
            try
            {
                if ( externalPortUDP != -1 )
                {
                    boolean mappedUDP =
                        igd.addPortMapping( "TomP2P mapping UDP", "UDP", internalHost, externalPortUDP, internalPortUDP );
                    if ( mappedUDP )
                    {
                        internetGatewayDevicesUDP.put( igd, externalPortUDP );
                    }
                }
            }
            catch ( IOException e )
            {
                logger.warn( "error in mapping UPD UPNP " + e );
                continue;

            }
            catch ( UPNPResponseException e )
            {
                logger.warn( "error in mapping UDP UPNP " + e );
                continue;
            }
            try
            {
                if ( externalPortTCP != -1 )
                {
                    boolean mappedTCP =
                        igd.addPortMapping( "TomP2P mapping TCP", "TCP", internalHost, externalPortTCP, internalPortTCP );
                    if ( mappedTCP )
                        internetGatewayDevicesTCP.put( igd, externalPortTCP );
                }
            }
            catch ( IOException e )
            {
                logger.warn( "error in mapping TCP UPNP " + e );
                continue;

            }
            catch ( UPNPResponseException e )
            {
                logger.warn( "error in mapping TCP UPNP " + e );
                continue;
            }
            once = true;
        }
        return once == true;
    }

    /**
     * Unmap the device that has been mapped previously. Used during shutdown.
     */
    private void unmapUPNP()
    {
        for ( Map.Entry<InternetGatewayDevice, Integer> entry : internetGatewayDevicesTCP.entrySet() )
        {
            try
            {
                entry.getKey().deletePortMapping( null, entry.getValue(), "TCP" );
            }
            catch ( IOException e )
            {
                logger.warn( "not removed TCP mapping " + entry.toString() + e );
            }
            catch ( UPNPResponseException e )
            {
                logger.warn( "not removed TCP mapping " + entry.toString() + e );
            }
            logger.info( "removed TCP mapping " + entry.toString() );
        }
        for ( Map.Entry<InternetGatewayDevice, Integer> entry : internetGatewayDevicesUDP.entrySet() )
        {
            try
            {
                entry.getKey().deletePortMapping( null, entry.getValue(), "UDP" );
            }
            catch ( IOException e )
            {
                logger.warn( "not removed UDP mapping " + entry.toString() + e );
            }
            catch ( UPNPResponseException e )
            {
                logger.warn( "not removed UDP mapping " + entry.toString() + e );
            }
            logger.info( "removed UDP mapping " + entry.toString() );
        }
    }

    /**
     * Registers a shutdownhook to clean the NAT mapping. If this is not called, then the mapping may stay until the
     * router is rebooted.
     */
    private void setShutdownHookEnabled()
    {
        synchronized ( shutdownLock )
        {
            // Set to enabled.
            // The shutdown hook simply runs the shutdown method.
            Thread t = new Thread( new Runnable()
            {
                public void run()
                {
                    shutdown();
                }
            }, "TomP2P:NATUtils:ShutdownHook" );
            Runtime.getRuntime().addShutdownHook( t );
        }
    }

    /**
     * Since shutdown is also called from the shutdown hook, it might get called twice. Thus, this method deregister NAT
     * mappings only once. If it already has been called, this method does nothing.
     */
    public void shutdown()
    {
        synchronized ( shutdownLock )
        {
            if ( runOnce )
            {
                return;
            }
            runOnce = true;
            unmapUPNP();
            if ( pmpDevice != null )
            {
                // TODO: check if this removes the entry as well
                pmpDevice.shutdown();
            }
        }
    }
}