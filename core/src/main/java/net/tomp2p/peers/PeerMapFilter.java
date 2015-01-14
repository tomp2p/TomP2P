/*
 * Copyright 2013 Thomas Bocek
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

package net.tomp2p.peers;

import java.util.Collection;

/**
 * A filter that can prevent peers from being stored in the map.
 * Therefore it will be ignored in the routing
 * 
 * @author Thomas Bocek
 * 
 */
public interface PeerMapFilter {

    /**
     * Each peer that is added in the map runs through this filter.
     * 
     * @param peerAddress
     *            The peer address that is going to be added to the map
     * @return True if the peer address should not be added, false otherwise
     */
	boolean reject(PeerAddress peerAddress, Collection<PeerAddress> all, Number160 target);

}
