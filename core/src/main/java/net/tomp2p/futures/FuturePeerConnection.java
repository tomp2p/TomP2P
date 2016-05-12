/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.tomp2p.futures;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.peers.PeerAddress;

/**
 *
 * @author Thomas Bocek
 */
public class FuturePeerConnection extends FutureDoneAttachment<PeerConnection, PeerAddress> {

    public FuturePeerConnection(PeerAddress remotePeer) {
        super(remotePeer);
    }
    
    public PeerConnection peerConnection() {
        return object();
    }
    
    public PeerAddress remotePeer() {
        return attachment();
    }    
}
