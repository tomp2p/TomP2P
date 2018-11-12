package net.tomp2p.p2p;

import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Pair;

public interface PeerAddressManager {
    Pair<PeerAddress, byte[]> getPeerAddressFromShortId(int recipientShortId);
    Pair<PeerAddress, byte[]> getPeerAddressFromId(Number256 peerId);
}
