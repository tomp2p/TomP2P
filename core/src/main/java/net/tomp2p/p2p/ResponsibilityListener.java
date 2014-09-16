/*
 * Copyright 2012 Thomas Bocek
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

import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

/**
 * 
 * @author Thomas Bocek
 * 
 */
public interface ResponsibilityListener {

    /**
     * The responsibilty changed to our peer. That means we are now responsible.
     * 
     * @param locationKey
     *            The location key
     */
    void meResponsible(Number160 locationKey);
    
    void meResponsible(Number160 locationKey, PeerAddress newPeer);

    /**
     * Here an other peer is responsible and we need to transfer data.
     * 
     * @param locationKey
     *            The location key
     * @param other
     *            The other peer that is responsible for locationKey
     * @param delayed
     *            Indicates if the other peer should get notified immediately or delayed. The case for delayed is that
     *            multiple non responsible peers may call this and a delayed call in that case may be better.
     * @return 
     */
    FutureDone<?> otherResponsible(Number160 locationKey, PeerAddress other);
}
