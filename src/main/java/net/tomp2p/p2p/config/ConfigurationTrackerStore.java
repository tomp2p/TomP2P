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
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.peers.Number160;

public class ConfigurationTrackerStore extends ConfigurationBase
{
	private byte[] attachement;
	private TrackerConfiguration trackerConfiguration;
	private FutureCreate<BaseFuture> futureCreate;
	// used for peer exchange
	private int waitBeforeNextSendSeconds=0;
	

	public ConfigurationTrackerStore setAttachement(byte[] attachement)
	{
		this.attachement = attachement;
		return this;
	}

	public byte[] getAttachement()
	{
		return attachement;
	}

	public ConfigurationTrackerStore setTrackerConfiguration(
			TrackerConfiguration trackerConfiguration)
	{
		this.trackerConfiguration = trackerConfiguration;
		return this;
	}

	public TrackerConfiguration getTrackerConfiguration()
	{
		return trackerConfiguration;
	}

	@Override
	public ConfigurationBase setContentKey(Number160 contentKey)
	{
		throw new UnsupportedOperationException("the tracker sets its own content key");
	}

	@Override
	public Number160 getContentKey()
	{
		throw new UnsupportedOperationException("the tracker sets its own content key");
	}

	public ConfigurationTrackerStore setFutureCreate(FutureCreate<BaseFuture> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}

	public FutureCreate<BaseFuture> getFutureCreate()
	{
		return futureCreate;
	}

	public void setWaitBeforeNextSendSeconds(int waitBeforeNextSendSeconds)
	{
		this.waitBeforeNextSendSeconds = waitBeforeNextSendSeconds;
	}

	public int getWaitBeforeNextSendSeconds()
	{
		return waitBeforeNextSendSeconds;
	}
}
