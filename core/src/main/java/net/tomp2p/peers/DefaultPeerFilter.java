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
 * The default peer filter accepts all peers.
 * 
 * @author Thomas Bocek
 * 
 */
public class DefaultPeerFilter implements PeerMapFilter {

	@Override
    public boolean rejectPeerMap(PeerAddress peerAddress, final PeerMap peerMap) {
		// by default, don't reject anything
	    return false;
    }

	@Override
    public boolean rejectPreRouting(PeerAddress peerAddress, Collection<PeerAddress> all) {
		// by default, don't reject anything
	    return false;
    }

}
