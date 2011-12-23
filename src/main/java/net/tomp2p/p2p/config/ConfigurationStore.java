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
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;

public class ConfigurationStore extends ConfigurationBase
{
	private boolean absent;
	private RequestP2PConfiguration requestP2PConfiguration;
	private boolean protectDomain;
	private int refreshSeconds;
	private FutureCreate<FutureDHT> futureCreate;

	public boolean isStoreIfAbsent()
	{
		return absent;
	}

	/**
	 * If set to true, it only stores data on peers that do not have it.
	 * FutureDHT reports back what keys have been stored on which peers (
	 * {@link FutureDHT#getRawKeys()}. The evaluation if it was successful or
	 * not cannot be done by TomP2P, since FutureDHT e.g. may report that it was
	 * stored on peer A, but peer B already has it. Instead a Map<PeerAddress,
	 * Collection<Number160>> is returned, which tells you on what peer the
	 * value has been stored. If you store key1 and key2 and FutureDHT tells you
	 * peerA:key1, then you know, that key1 was absent, while key2 was already
	 * there.
	 * 
	 * @param absent True if the value should only be stored if not already
	 *        present
	 * @return This instance.
	 */
	public ConfigurationStore setStoreIfAbsent(boolean absent)
	{
		this.absent = absent;
		return this;
	}

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

	public ConfigurationBase setProtectDomain(boolean protectDomain)
	{
		this.protectDomain = protectDomain;
		return this;
	}

	public boolean isProtectDomain()
	{
		return protectDomain;
	}

	public ConfigurationStore setRefreshSeconds(int refreshSeconds)
	{
		this.refreshSeconds = refreshSeconds;
		return this;
	}

	public int getRefreshSeconds()
	{
		return refreshSeconds;
	}

	public ConfigurationStore setFutureCreate(FutureCreate<FutureDHT> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}

	public FutureCreate<FutureDHT> getFutureCreate()
	{
		return futureCreate;
	}
}
