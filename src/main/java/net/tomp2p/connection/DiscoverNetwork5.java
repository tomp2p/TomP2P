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
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Discovers and searches for interfaces and addresses for users with Java 1.5.
 * The feature to add broadcast addresses for searching peers over layer 2 is
 * not supported here. For such as feature, use Java 1.6
 * 
 * @see DiscoverNetwork6
 * 
 * @author Thomas Bocek
 * 
 */
public class DiscoverNetwork5 implements DiscoverNetwork
{
	@Override
	public String discoverNetwork(NetworkInterface networkInterface, Bindings bindings)
	{
		StringBuilder sb = new StringBuilder("( ");
		// works in 1.5
		for (Enumeration<InetAddress> e = networkInterface.getInetAddresses(); e.hasMoreElements();)
		{
			// works in 1.5
			InetAddress inet = e.nextElement();
			if (bindings.getFoundAddresses().contains(inet))
			{
				continue;
			}
			// ignore if a user specifies an address and inet is not part of it
			if(!bindings.isAllAddresses())
			{
				if(!bindings.getAddresses().contains(inet))
				{
					continue;
				}
			}
			if (inet instanceof Inet4Address && bindings.isIPv4())
			{
				sb.append(inet).append(",");
				bindings.addFoundAddress(inet);
			}
			else if (inet instanceof Inet6Address && bindings.isIPv6())
			{
				sb.append(inet).append(",");
				bindings.addFoundAddress(inet);
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.append(")").toString();
	}
}