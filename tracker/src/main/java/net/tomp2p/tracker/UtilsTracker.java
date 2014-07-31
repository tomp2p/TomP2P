package net.tomp2p.tracker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

public class UtilsTracker {
	public static TrackerData limit(TrackerData peers, int size) {
		Map<PeerStatatistic, Data> map = new HashMap<PeerStatatistic, Data>(peers.peerAddresses());
		Map<PeerStatatistic, Data> retVal = new HashMap<PeerStatatistic, Data>(size);
		int i = 0;
		for (Iterator<Map.Entry<PeerStatatistic, Data>> it = map.entrySet().iterator(); it.hasNext() && i++ < size;) {
			Map.Entry<PeerStatatistic, Data> entry = it.next();
			retVal.put(entry.getKey(), entry.getValue());
		}
		TrackerData data = new TrackerData(retVal);
		return data;
	}

	public static TrackerData disjunction(TrackerData meshPeers, SimpleBloomFilter<Number160> knownPeers) {
		TrackerData trackerData = new TrackerData(new HashMap<PeerStatatistic, Data>());
		for (Map.Entry<PeerStatatistic, Data> entry : meshPeers.peerAddresses().entrySet()) {
			if (!knownPeers.contains(entry.getKey().peerAddress().peerId())) {
				trackerData.put(entry.getKey(), entry.getValue());
			}
		}
		return trackerData;
	}
}
