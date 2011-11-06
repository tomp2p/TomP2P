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

public class NATUtils 
{
	final private static Logger logger = LoggerFactory.getLogger(NATUtils.class);
	// UPNP
	final private Map<InternetGatewayDevice, Integer> internetGatewayDevicesUDP = new HashMap<InternetGatewayDevice, Integer>();
	final private Map<InternetGatewayDevice, Integer> internetGatewayDevicesTCP = new HashMap<InternetGatewayDevice, Integer>();
	// NAT-PMP
	private NatPmpDevice pmpDevice;
	private Thread shutdownHookThread = null;
	private final Object shutdownLock = new Object();
	
	public NATUtils()
	{
		setShutdownHookEnabled(true);
	}
	
	public boolean mapPMP(int internalPortUDP, int internalPortTCP, int externalPortUDP, int externalPortTCP) throws NatPmpException
	{
		InetAddress gateway = Gateway.getIP();
		pmpDevice = new NatPmpDevice(gateway);
		MapRequestMessage mapTCP = new MapRequestMessage(true, internalPortTCP, externalPortTCP, Integer.MAX_VALUE, null);
		MapRequestMessage mapUDP = new MapRequestMessage(false, internalPortUDP, externalPortUDP, Integer.MAX_VALUE, null);
		pmpDevice.enqueueMessage(mapTCP);
		pmpDevice.enqueueMessage(mapUDP);
		pmpDevice.waitUntilQueueEmpty();
		//UDP is nice to have, but we need TCP
		return mapTCP.getResultCode() == ResultCode.Success;
	}
	
	public void mapUPNP(String internalHost, int internalPortUDP, int internalPortTCP, int externalPortUDP, int externalPortTCP)
			throws IOException
	{
		// -1 sets the default timeout to 1500 ms
		Collection<InternetGatewayDevice> IGDs = InternetGatewayDevice.getDevices(-1);
		if (IGDs == null)
			return;
		for (InternetGatewayDevice igd : IGDs)
		{
			logger.info("Found device " + igd);
			try
			{
				if (externalPortUDP != -1)
				{
					boolean mappedUDP = igd.addPortMapping("TomP2P mapping UDP", "UDP", internalHost, externalPortUDP,
							internalPortUDP);
					if (mappedUDP)
						internetGatewayDevicesUDP.put(igd, externalPortUDP);
				}
			}
			catch (IOException e)
			{
				logger.warn("error in mapping UPD UPNP " + e);

			}
			catch (UPNPResponseException e)
			{
				logger.warn("error in mapping UDP UPNP " + e);
			}
			try
			{
				if (externalPortUDP != -1)
				{
					boolean mappedTCP = igd.addPortMapping("TomP2P mapping TCP", "TCP", internalHost, externalPortTCP,
							internalPortTCP);
					if (mappedTCP)
						internetGatewayDevicesTCP.put(igd, externalPortTCP);
				}
			}
			catch (IOException e)
			{
				logger.warn("error in mapping TCP UPNP " + e);

			}
			catch (UPNPResponseException e)
			{
				logger.warn("error in mapping TCP UPNP " + e);
			}
		}
	}
	
	private void unmapUPNP()
	{
		for (Map.Entry<InternetGatewayDevice, Integer> entry : internetGatewayDevicesTCP.entrySet())
		{
			try
			{
				entry.getKey().deletePortMapping(null, entry.getValue(), "TCP");
			}
			catch (IOException e)
			{
				logger.warn("not removed TCP mapping " + entry.toString() + e);
			}
			catch (UPNPResponseException e)
			{
				logger.warn("not removed TCP mapping " + entry.toString() + e);
			}
			logger.info("removed TCP mapping " + entry.toString());
		}
		for (Map.Entry<InternetGatewayDevice, Integer> entry : internetGatewayDevicesUDP.entrySet())
		{
			try
			{
				entry.getKey().deletePortMapping(null, entry.getValue(), "UDP");
			}
			catch (IOException e)
			{
				logger.warn("not removed UDP mapping " + entry.toString() + e);
			}
			catch (UPNPResponseException e)
			{
				logger.warn("not removed UDP mapping " + entry.toString() + e);
			}
			logger.info("removed UDP mapping " + entry.toString());
		}
	}
	
    public final void setShutdownHookEnabled(boolean enabled) 
    {
        synchronized (shutdownLock) 
        {
            if (isShutdownHookEnabled()) 
            {
                // Currently enabled.
                if (!enabled) 
                {
                    // Set to disabled.
                    Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
                    shutdownHookThread = null;
                }
            } 
            else 
            {
                // Not currently enabled.
                if (enabled) 
                {
                    // Set to enabled.
                    // The shutdown hook simply runs the shutdown method.
                    Thread t = new Thread(new Runnable() 
                    {
                        public void run() 
                        {
                            shutdown();
                        }
                    }, "TomP2P:NATUtils:ShutdownHook");
                    
                    Runtime.getRuntime().addShutdownHook(t);
                    shutdownHookThread = t;
                }
            }
        }
    }
    
    public void shutdown() 
    {
        synchronized (shutdownLock) 
        {
        	setShutdownHookEnabled(false);
        	unmapUPNP();
        	if(pmpDevice!=null)
            {
            	pmpDevice.shutdown();
            }
        }
    }
    
    public final boolean isShutdownHookEnabled() 
    {
        synchronized (shutdownLock) 
        {
            return shutdownHookThread != null;
        }
    }
}