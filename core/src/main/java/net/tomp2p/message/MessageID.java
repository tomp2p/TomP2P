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
package net.tomp2p.message;

import net.tomp2p.peers.PeerAddress;

/**
 * A message ID consists of the message id, which is created randomly and the nodeaddress. I'm not sure if we need this
 * class with Netty... We'll see
 * 
 * @author Thomas Bocek
 */
public class MessageID implements Comparable<MessageID> {
    // the message id, which is together with the nodeAddress unique. However,
    // we do not check this and collisions will cause a message to fail.
    private final int id;

    // The nodeaddress depends on the message, either its the sender or the
    // receiver
    private final PeerAddress peerAddress;

    /**
     * Creates a message Id. If the message is a request, the peer address is the sender, otherwise its the recipient.
     * This is due to the fact that depending on the direction, peer address may change, but its still considered the
     * same message.
     * 
     * @param message
     *            The message
     */
    public MessageID(final Message message) {
        this(message.getMessageId(), message.isRequest() ? message.getSender() : message.getRecipient());
    }

    /**
     * Creates a message Id. If the message is a request, the peer address is the sender, otherwise its the recipient.
     * This is due to the fact that depending on the direction, peer address may change, but its still considered the
     * same message.
     * 
     * @param id
     *            The message id
     * @param nodeAddress
     *            The node address
     */
    private MessageID(final int id, final PeerAddress nodeAddress) {
        this.id = id;
        this.peerAddress = nodeAddress;
    }

    /**
     * @return the message id
     */
    public int getId() {
        return id;
    }

    /**
     * @return The node address of sender or recipient
     */
    public PeerAddress getNodeAddress() {
        return peerAddress;
    }

    @Override
    public int compareTo(final MessageID o) {
        final int diff = id - o.id;
        if (diff == 0) {
            return peerAddress.compareTo(o.peerAddress);
        }
        return diff;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof MessageID)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        return compareTo((MessageID) obj) == 0;
    }

    @Override
    public int hashCode() {
        return id ^ peerAddress.hashCode();
    }

    @Override
    public String toString() {
        return new StringBuilder("MessageId:").append(id).append("/").append(peerAddress).toString();
    }
}
