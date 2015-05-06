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

package net.tomp2p.rpc;

import net.tomp2p.peers.PeerAddress;

/**
 * Similar to {@link RawDataReply}, but we convert the raw byte buffer to an object.
 * 
 * @author Thomas Bocek
 * 
 */
public interface ObjectDataReply {
    /**
     * Replies to a direct message from a peer. This reply is based on objects.
     * 
     * @param sender
     *            The sender of this message
     * @param request
     *            The request that the sender sent.
     * @return A new object that is the reply.
     * @throws Exception
     */
    Object reply(PeerAddress sender, Object request) throws Exception;
}
