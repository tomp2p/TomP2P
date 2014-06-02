package net.tomp2p.storage;

import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.DigestInfo;

public interface DigestTracker {

	DigestInfo digest(Number160 locationKey, Number160 domainKey, Object object);

	

}