package net.tomp2p.connection;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;

import net.tomp2p.connection.Bindings.Protocol;

public class DiscoverNetwork6 implements DiscoverNetwork
{
	final private Bindings bindings;

	public DiscoverNetwork6(Bindings bindings)
	{
		this.bindings = bindings;
	}

	@Override
	public StringBuilder discoverNetwork(NetworkInterface networkInterface)
	{
		StringBuilder sb = new StringBuilder();
		// works only in 1.6
		for (InterfaceAddress iface : networkInterface.getInterfaceAddresses())
		{
			// works only in 1.6
			InetAddress inet = iface.getAddress();
			if (bindings.getAddresses().contains(inet)) 
				continue;
			// works only in 1.6
			if (iface.getBroadcast() != null) 
				bindings.addBroadcastAddress(iface.getBroadcast());
			if (bindings.useAllProtocols())
			{
				sb.append(",").append(inet);
				bindings.addAddress(inet);
			}
			else
			{
				if (inet instanceof Inet4Address && bindings.getProtocols().contains(Protocol.IPv4))
				{
					sb.append(",").append(inet);
					bindings.addAddress(inet);
				}
				if (inet instanceof Inet6Address && bindings.getProtocols().contains(Protocol.IPv6))
				{
					sb.append(",").append(inet);
					bindings.addAddress(inet);
				}
			}
		}
		return sb.replace(0, 1, "(").append(")");
	}
}
