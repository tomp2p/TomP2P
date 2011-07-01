/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.p2p;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.TrackerData;

public class VotingSchemeTracker implements EvaluatingSchemeTracker
{
	@Override
	public Collection<TrackerData> evaluateSingle(Map<PeerAddress, Collection<TrackerData>> rawData)
	{
		if (rawData == null)
			throw new IllegalArgumentException("cannot evaluate, as no result provided");
		Collection<TrackerData> result = new HashSet<TrackerData>();
		for (Collection<TrackerData> trackerDatas : rawData.values())
			result.addAll(trackerDatas);
		return result;
	}

}
