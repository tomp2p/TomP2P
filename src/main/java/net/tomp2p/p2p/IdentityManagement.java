/*
 * Copyright 2011 Thomas Bocek
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

import java.security.PublicKey;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;

public class IdentityManagement implements PeerStatusListener {
    final private ConcurrentHashMap<Number160, PublicKey> peerIdentity = new ConcurrentHashMap<Number160, PublicKey>();

    final private PeerAddress self;

    public IdentityManagement(PeerAddress self) {
        this.self = self;
    }

    public boolean checkIdentity(Number160 peerId, PublicKey publicKey) {
        // check permission
        final PublicKey storedIdentity = peerIdentity.get(peerId);
        if (storedIdentity != null) {
            if (storedIdentity.equals(publicKey)) {
                // the identity stored on first contact matches
                return true;
            } else {
                // the peer is not one we met the first time.
                return false;
            }
        } else if (publicKey != null) {
            // first contact, store this identity
            if (peerIdentity.putIfAbsent(peerId, publicKey) != null) {
                // if someone in the meantime already added something to this
                // map, then go again
                return checkIdentity(peerId, publicKey);
            }
            return true;
        } else {
            // no public key provided, so assuming its ok. For strong
            // security, return false here
            return true;
        }
    }

    @Override
    public void peerOffline(PeerAddress peerAddress, Reason reason) {
        // don't care, peer can come online again

    }

    @Override
    public void peerFail(PeerAddress peerAddress, boolean force) {
        // don't care, peer can come online again

    }

    @Override
    public void peerOnline(PeerAddress peerAddress) {
        // TODO refresh timeout
    }

    public Number160 getSelf() {
        return self.getPeerId();
    }

    public PeerAddress getPeerAddress() {
        return self;
    }
}
