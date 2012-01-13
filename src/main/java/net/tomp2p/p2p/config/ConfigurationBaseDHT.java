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
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;

public class ConfigurationBaseDHT extends ConfigurationBase
{
	private RequestP2PConfiguration requestP2PConfiguration;
	private boolean automaticCleanup;

	public ConfigurationBase setRequestP2PConfiguration(
			RequestP2PConfiguration requestP2PConfiguration)
	{
		this.requestP2PConfiguration = requestP2PConfiguration;
		return this;
	}

	public RequestP2PConfiguration getRequestP2PConfiguration()
	{
		return requestP2PConfiguration;
	}

	/**
	 * @return True if automatic cleanup should be enabled. This means that
	 *         after a FutureDHT is complete, the ChannelCreator is
	 *         automatically released. If set to false, the user needs to call
	 *         {@link Peer#release(net.tomp2p.connection.ChannelCreator)}.
	 */
	public boolean isAutomaticCleanup()
	{
		return automaticCleanup;
	}

	/**
	 * Set this flag to true if automatic cleanup should be enabled. This means
	 * that after a FutureDHT is complete, the ChannelCreator is automatically
	 * released. If set to false, the user needs to call
	 * {@link Peer#release(net.tomp2p.connection.ChannelCreator)}.
	 * 
	 * @param automaticCleanup The default in {@link Configurations} is true.
	 */
	public void setAutomaticCleanup(boolean automaticCleanup)
	{
		this.automaticCleanup = automaticCleanup;
	}
}
