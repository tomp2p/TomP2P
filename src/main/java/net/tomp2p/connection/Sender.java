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

package net.tomp2p.connection;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;

public interface Sender
{
    /**
     * Sent the message via TCP. Keep the future state.
     * 
     * @param handler RequestHandlerTCP
     * @param futureResponse FutureResponse
     * @param message Message
     * @param channelCreator ChannelCreator
     * @param idleTCPMillis Timeout
     */
    public abstract void sendTCP( final RequestHandlerTCP<? extends BaseFuture> handler,
                                  final FutureResponse futureResponse, final Message message,
                                  final ChannelCreator channelCreator, final int idleTCPMillis );

    /**
     * Sent the message via UDP. Keep the future state.
     * 
     * @param handler RequestHandlerUDP
     * @param futureResponse FutureResponse
     * @param message Message
     * @param channelCreator ChannelCreator
     */
    public abstract void sendUDP( final RequestHandlerUDP<? extends BaseFuture> handler,
                                  final FutureResponse futureResponse, final Message message,
                                  final ChannelCreator channelCreator );

    /**
     * Sent the message via UDP broadcast. Keep the future state.
     * 
     * @param handler RequestHandlerUDP
     * @param futureResponse FutureResponse
     * @param message Message
     * @param channelCreator ChannelCreator
     */
    public abstract void sendBroadcastUDP( final RequestHandlerUDP<? extends BaseFuture> handler,
                                           final FutureResponse futureResponse, final Message message,
                                           final ChannelCreator channelCreator );

    /**
     * Shuts down the sender. After this the sender cannot be used again.
     */
    public abstract void shutdown();
}