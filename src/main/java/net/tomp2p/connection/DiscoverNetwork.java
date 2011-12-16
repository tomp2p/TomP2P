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

public interface DiscoverNetwork
{
	/**
	 * Discovers network interfaces and addresses. Since we need to do it
	 * differently in Java5 and Java6 we need this interface and reflection. As
	 * soon as this library supports Java6+, then these classes will be
	 * simplified.
	 * 
	 * @param networkInterface The networkinterface to search for addresses to listen to
	 * @param bindings The search hints and result storage.
	 * @return 
	 */
	abstract public String discoverNetwork(NetworkInterface networkInterface,
			Bindings bindings);
}
