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
import net.tomp2p.p2p.EvaluatingSchemeTracker;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.peers.Number160;

public class ConfigurationTrackerGet extends ConfigurationBase
{
	private boolean expectAttachement;
	private boolean useSecondaryTrackers;
	private EvaluatingSchemeTracker evaluationScheme;
	private TrackerConfiguration trackerConfiguration;

	public ConfigurationTrackerGet setExpectAttachement(boolean expectAttachement)
	{
		this.expectAttachement = expectAttachement;
		return this;
	}

	public boolean isExpectAttachement()
	{
		return expectAttachement;
	}

	public ConfigurationTrackerGet setEvaluationScheme(EvaluatingSchemeTracker evaluationScheme)
	{
		this.evaluationScheme = evaluationScheme;
		return this;
	}

	public EvaluatingSchemeTracker getEvaluationScheme()
	{
		return evaluationScheme;
	}

	public ConfigurationTrackerGet setTrackerConfiguration(TrackerConfiguration trackerConfiguration)
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

	public void setUseSecondaryTrackers(boolean useSecondaryTrackers)
	{
		this.useSecondaryTrackers = useSecondaryTrackers;
	}

	public boolean isUseSecondaryTrackers()
	{
		return useSecondaryTrackers;
	}
}
