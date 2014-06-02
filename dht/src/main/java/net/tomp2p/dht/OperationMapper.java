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

package net.tomp2p.dht;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.PeerAddress;

/**
 * The operations that create many RPC.
 * 
 * @author Thomas Bocek
 * 
 * @param <K>
 *            The type of the future that takes care of all the RPC futures
 */
public interface OperationMapper<K extends FutureDHT<?>> {
    /**
     * Creates a single RPC.
     * 
     * @param channelCreator
     *            The channel creator to creade a UDP or TCP channel
     * @param remotePeerAddress
     *            The address of the remote peer
     * @return The future object of this response
     */
    FutureResponse create(ChannelCreator channelCreator, PeerAddress remotePeerAddress);

    /**
     * If the response over all futures arrived (all that were created with {@link #create(ChannelCreator, PeerAddress)}
     * ).
     * 
     * @param future
     *            The overall future, typically FutureDHT or similar
     */
    void response(K future);

    /**
     * Whenever a single future is finished, then this method is called.
     * 
     * @param futureResponse
     *            The future object from an RPC
     */
    void interMediateResponse(FutureResponse futureResponse);
}
