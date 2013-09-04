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

package net.tomp2p.connection2;

import java.util.Map;

import io.netty.channel.ChannelHandler;

/**
 * The user may modify the filter by adding, removing, or changing the handlers.
 * 
 * @author Thomas Bocek
 * 
 */
public interface PipelineFilter {
    /**
     * Filter the handlers. If no filtering should happen, return the same array.
     * 
     * @param handlers
     *            The created handlers by tomp2p
     * @param tcp
     *            True if the connection is TCP, false for UDP
     * @param client
     *            True if this is the client side, false for the server side
     * @return The same, new, or changed array of handlers. It cannot have null elements
     */
    void filter(Map<String,ChannelHandler> channelHandlers, boolean tcp, boolean client);
}
