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
 * The bootstrap will be a wrapped future, because we need to ping a server
 * first, and if this ping is successful, we can bootstrap.
 * 
 * @author Thomas Bocek
 * @param <K>
 */
public class FutureWrappedBootstrap<K extends BaseFuture> extends FutureWrapper<K> implements FutureBootstrap {
    private Collection<PeerAddress> bootstrapTo;

    /**
     * The addresses we boostrap to. If we broadcast, we don't know the
     * addresses in advance.
     * 
     * @param bootstrapTo
     *            A list of peers that were involved in the bootstrapping
     */
    public void setBootstrapTo(Collection<PeerAddress> bootstrapTo) {
        synchronized (lock) {
            this.bootstrapTo = bootstrapTo;
        }
    }

    /**
     * Returns a list of of peers that were involved in the bootstrapping
     */
    public Collection<PeerAddress> getBootstrapTo() {
        synchronized (lock) {
            return bootstrapTo;
        }
    }
}
