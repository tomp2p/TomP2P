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
package net.tomp2p.futures;

import java.util.Collection;

import net.tomp2p.peers.PeerAddress;

/**
 * Used for bootstrapping. One important information in bootstrapping is to get
 * the nodes that we bootstrapped to. We may not know this in advance as we
 * might bootstrap via broadcast.
 * 
 * @author Thomas Bocek
 */
public interface FutureBootstrap extends BaseFuture {
    /**
     * Returns the Peers we bootstrapped in no particular order.
     * 
     * @return the peers we bootstrapped to.
     */
     Collection<PeerAddress> getBootstrapTo();
}
