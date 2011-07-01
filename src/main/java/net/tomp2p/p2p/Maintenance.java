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
package net.tomp2p.p2p;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.TrackerStorage;

public class Maintenance
{

	public void addTrackerMaintenance(PeerAddress peerAddress, PeerAddress referrer, Number160 locationKey,
			Number160 domainKey, TrackerStorage trackerStorage)
	{
		// TODO: do real checks, for now, we trust the peers
		trackerStorage.moveFromSecondaryToMesh(peerAddress, referrer, locationKey, domainKey, null);
	}

}
