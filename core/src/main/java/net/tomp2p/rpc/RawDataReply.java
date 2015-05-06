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
package net.tomp2p.rpc;

import net.tomp2p.message.Buffer;
import net.tomp2p.peers.PeerAddress;

/**
 * The interface for receiving raw data and sending raw data back. Raw means that we use a Netty buffer.
 * 
 * @author Thomas Bocek
 * 
 */
public interface RawDataReply {
    /**
     * Replies to a direct message from a peer. This reply is based on ChannelBuffer, which is typically used for those
     * cases where a custom encoder/decoder is necessary.
     * 
     * @param sender
     *            The sender from which the request came
     * @param requestBuffer
     *            The incoming buffer
     * @param complete
     *            Indication if the request buffer is complete
     * @return A ChannelBuffer with the result. If null is returned, then the message will contain NOT_FOUND, if the
     *         same buffer as requestBuffer is sent back, the message will contain OK, otherwise the payload will be
     *         set.
     * @throws Exception
     *             In case of an exception, a stacktrace will be printed to System.err and a log output will be
     *             generated
     */
    Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception;
}
