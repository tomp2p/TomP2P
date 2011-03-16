
package net.tomp2p.connection;

import java.net.NetworkInterface;

public interface DiscoverNetwork
{
	public void init(Bindings bindings);
	public StringBuilder discoverNetwork(NetworkInterface networkInterface);
}
