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

import net.tomp2p.message.Message2;

/**
 * The handler that is called when we receive a broadcast message. One way to implement this would be to send it to
 * random peers.
 * 
 * @author Thomas Bocek
 * 
 */
public interface BroadcastHandler {
    /**
     * This method is called when a peer receives a broadcast message request. Its up to the peer to decide what to do
     * with it.
     * 
     * @param message
     *            The message that was received in the broadcast message
     */
     void receive(Message2 message);
}
