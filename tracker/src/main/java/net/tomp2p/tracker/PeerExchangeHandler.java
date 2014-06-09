package net.tomp2p.tracker;

import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;

public interface PeerExchangeHandler {

	boolean put(Number320 key, TrackerData trackerData, PeerAddress referrer);

	TrackerTriple get();

}
