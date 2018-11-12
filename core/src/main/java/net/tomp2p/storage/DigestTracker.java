package net.tomp2p.storage;

import net.tomp2p.peers.Number256;
import net.tomp2p.rpc.DigestInfo;

public interface DigestTracker {

	DigestInfo digest(Number256 locationKey, Number256 domainKey, Number256 contentKey);

}