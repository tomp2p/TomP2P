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

package net.tomp2p.connection;

import io.netty.channel.ChannelHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.Map;

import net.tomp2p.utils.Pair;

/**
 * The user may modify the filter by adding, removing or changing the handlers.
 * 
 * @author Thomas Bocek
 * 
 */
public interface PipelineFilter {
    /**
     * Filters the handlers. If no filtering should happen, return the same map.
     * 
     * @param channelHandlers
     *            The handlers created by tomp2p
     * @param tcp
     *            True, if the connection is TCP. False, if UDP
     * @param client
     *            True, if this is the client side. False if server side
     * @return The same, new or changed map of handlers
     */
	Map<String,Pair<EventExecutorGroup,ChannelHandler>> filter(Map<String,Pair<EventExecutorGroup,ChannelHandler>> channelHandlers, boolean tcp, boolean client);
}
