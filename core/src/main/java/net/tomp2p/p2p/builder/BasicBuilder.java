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

package net.tomp2p.p2p.builder;

import java.util.Collection;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.p2p.PostRoutingFilter;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMapFilter;

/**
 * The basic build methods for the builder classes.
 * 
 * @author Thomas Bocek
 *
 * @param <K>
 */
public interface BasicBuilder<K> extends ConnectionConfiguration, Builder {

    public Number160 locationKey();

    public Number160 domainKey();

    public K domainKey(Number160 domainKey);

    public RoutingConfiguration routingConfiguration();

    public K routingConfiguration(RoutingConfiguration routingConfiguration);

    public RequestP2PConfiguration requestP2PConfiguration();

    public K requestP2PConfiguration(RequestP2PConfiguration requestP2PConfiguration);

    public RoutingBuilder createBuilder(RequestP2PConfiguration requestP2PConfiguration,
            RoutingConfiguration routingConfiguration);

    /**
     * @return A set of filters or null if no filters set
     */
	public Collection<PeerMapFilter> peerMapFilters();
	
	/**
	 * @return A set of filters or null if not filters set
	 */
	public Collection<PostRoutingFilter> postRoutingFilters();

}
