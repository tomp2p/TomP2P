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

package net.tomp2p.peers;

public interface PeerStatusListener
{
    /**
     * The reason NOT_REACHABLE means that the peer is offline and cannot be contacted, while REMOVED_FROM_MAP means
     * that this peer has been removed from the neigbhor list, but may still be reachable.
     */
    public enum Reason
    {
        REMOVED_FROM_MAP, NOT_REACHABLE
    }

    /**
     * Called if the peer does not send multiple answer in time. This peer is considered offline
     * 
     * @param peerAddress The address of the peer that went offline
     */
    public void peerOffline( PeerAddress peerAddress, Reason reason );

    /**
     * Called if the peer does not send answer in time. The peer may be busy, so there is a chance of seeing this peer
     * again.
     * 
     * @param peerAddress The address of the peer that failed
     * @param force Set to true if we are sure that the peer died.
     */
    public void peerFail( PeerAddress peerAddress, boolean force );

    /**
     * Called if the peer is online and we verified it. This method may get called many times, for each successful
     * request.
     * 
     * @param peerAddress The address of the peer that is online.
     */
    public void peerOnline( PeerAddress peerAddress );
}
