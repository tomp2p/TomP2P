/*
 * Copyright 2016 Thomas Bocek
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

import net.tomp2p.peers.PeerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default sending behavior for UDP and TCP messages. Depending whether the recipient is relayed, slow and on
 * the message size, decisions can be made here.
 *
 * @author Nico Rutishauser
 *
 */
public class DefaultSendBehavior implements SendBehavior {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSendBehavior.class);

    @Override
    public SendMethod tcpSendBehavior(Dispatcher dispatcher, PeerAddress sender,
            PeerAddress recipient, boolean isReflected) {
        if (recipient.peerId().equals(sender.peerId())) {
            // shortcut, just send to yourself
            LOG.debug("recipient and sender are the same TCP");
            return SendMethod.SELF;
        }
        //also check if we have multiple peers
        if (dispatcher.responsibleFor(recipient.peerId())) {
            // shortcut, just send to yourself
            LOG.debug("I'm responsible for that peer TCP");
            return SendMethod.SELF;
        }

        if (isReflected) {
            LOG.debug("reflected TCP");
            return SendMethod.DIRECT;
        }

        if (recipient.relaySize() > 0) {
            if (sender.relaySize() > 0) {
                // reverse connection is not possible because both peers are
                // relayed. Thus send the message to
                // one of the receiver's relay peers
                LOG.debug("relay or hole punching TCP");
                if(recipient.portPreserving() && sender.portPreserving()) {
                    return SendMethod.HOLEPUNCHING;
                } else {
                    return SendMethod.RELAY;
                }
                
            } else {
                // Messages with small size can be sent over relay, other messages should be sent directly (more efficient)
                LOG.debug("reverse connection TCP");
                return SendMethod.RCON;
            }
        }
        // send directly
        LOG.debug("go direct TCP");
        return SendMethod.DIRECT;
    }

    @Override
    public SendMethod udpSendBehavior(Dispatcher dispatcher, PeerAddress sender,
            PeerAddress recipient, boolean isReflected) {
        if (recipient.peerId().equals(sender.peerId())) {
            // shortcut, just send to yourself
            LOG.debug("recipient and sender are the same UDP");
            return SendMethod.SELF;
        }
        //also check if we have multiple peers
        if (dispatcher.responsibleFor(recipient.peerId())) {
            // shortcut, just send to yourself
            LOG.debug("reflected UDP");
            return SendMethod.SELF;
        }

        if (isReflected) {
            LOG.debug("reflected UDP");
            return SendMethod.DIRECT;
        }

        if (recipient.relaySize() > 0) {
            LOG.debug("relay, hole punching does not make sense with UDP");
            return SendMethod.RELAY;
        }

        LOG.debug("go direct UDP");
        return SendMethod.DIRECT;
    }
}
