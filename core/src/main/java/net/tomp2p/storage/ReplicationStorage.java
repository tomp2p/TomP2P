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

package net.tomp2p.storage;

import java.util.Collection;

import net.tomp2p.peers.Number160;

public interface ReplicationStorage {

    public abstract Collection<Number160> findPeerIDsForResponsibleContent(Number160 locationKey);

    public abstract Collection<Number160> findContentForResponsiblePeerID(Number160 peerID);

    public boolean updateResponsibilities(Number160 locationKey, Number160 peerId);

    public void removeResponsibility(Number160 locationKey);

    public void removeResponsibility(Number160 locationKey, Number160 peerId);

}