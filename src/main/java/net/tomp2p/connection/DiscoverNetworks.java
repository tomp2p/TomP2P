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
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * A class to search for addresses to bind the sockets to. The user first
 * creates a {@link Bindings} class and provides all the necesary information, then calls
 * {@link #discoverInterfaces(Bindings)}. The results are stored in {@link Bindings} as
 * well.
 * 
 * @author Thomas Bocek
 */
public class DiscoverNetworks
{
	/**
	 * Serach for local interfaces. Hints how to search for those interfaces are
	 * provided by the user throung the {@link Bindings} class. The results of that
	 * search (InetAddress) are stored in {@link Bindings} as well.
	 * 
	 * @param bindings The hints for the search and also the results are stored
	 *        there
	 * @return The status of the search
	 * @throws Exception If anything goes wrong, such as refelcetion.
	 */
	public static String discoverInterfaces(Bindings bindings) throws Exception
	{
		StringBuilder sb = new StringBuilder("Discover status: ");
		Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
		while (e.hasMoreElements())
		{
			NetworkInterface networkInterface = e.nextElement();
			if (bindings.isAllInterfaces())
			{
				sb.append(" ++").append(networkInterface.getName());
				sb.append(discoverNetwork(networkInterface, bindings)).append(",");
			}
			else
			{
				if (bindings.containsInterface(networkInterface.getName()))
				{
					sb.append(" +").append(networkInterface.getName());
					sb.append(discoverNetwork(networkInterface, bindings)).append(",");
				}
				else
				{
					sb.append(" -").append(networkInterface.getName()).append(",");
				}
			}
		}
		// remove the last comma or space
		sb.deleteCharAt(sb.length() - 1);
		return sb.append(".").toString();
	}

	/**
	 * Calls depending on Java5 or Java6 via reflection the network discovery.
	 * This is the only part in the library that uses reflection.
	 * 
	 * @param networkInterface The network interface to inspect
	 * @param bindings The results will be stored in bindings
	 * @return The status of the discovery
	 * @throws Exception If something goes wrong
	 */
	private static String discoverNetwork(NetworkInterface networkInterface,
			Bindings bindings) throws Exception
	{
		final Class<?> dn;
		// TODO: I don't like this at all, the best thing would be to test if
		// its Java 1.5. Newer Android devices should support Java 1.6
		if (System.getProperty("java.vm.name").equals("Dalvik")
				|| System.getProperty("java.version").startsWith("1.5"))
		{
			// we are Java 1.5 or running on Android, which is comparable to 1.5
			dn = Class.forName("net.tomp2p.connection.DiscoverNetwork5");
		}
		else
		{
			// we are most likely Java 1.6, if not, you'll see an error here
			dn = Class.forName("net.tomp2p.connection.DiscoverNetwork6");
		}
		DiscoverNetwork discoverNetwork = (DiscoverNetwork) dn.newInstance();
		return discoverNetwork.discoverNetwork(networkInterface, bindings);
	}
}
