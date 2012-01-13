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

package net.tomp2p.p2p.config;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;

public class ConfigurationBase
{
	private Number160 contentKey;
	private Number160 domain;
	private RoutingConfiguration routingConfiguration;
	private boolean signMessage;

	public ConfigurationBase setContentKey(Number160 contentKey)
	{
		this.contentKey = contentKey;
		return this;
	}

	public Number160 getContentKey()
	{
		return contentKey;
	}

	public ConfigurationBase setDomain(Number160 domain)
	{
		this.domain = domain;
		return this;
	}

	public Number160 getDomain()
	{
		return domain;
	}

	public ConfigurationBase setRoutingConfiguration(RoutingConfiguration routingConfiguration)
	{
		this.routingConfiguration = routingConfiguration;
		return this;
	}

	public RoutingConfiguration getRoutingConfiguration()
	{
		return routingConfiguration;
	}

	public ConfigurationBase setSignMessage(boolean signMessage)
	{
		this.signMessage = signMessage;
		return this;
	}

	public boolean isSignMessage()
	{
		return signMessage;
	}
}
