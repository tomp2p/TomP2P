package net.tomp2p.tracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

public class UtilsTracker {
	public static TrackerData limit(TrackerData peers, int size) {
		Map<PeerStatistic, Data> retVal = new HashMap<PeerStatistic, Data>(size);
		
		Random random = new Random();
		List<PeerStatistic> keys = new ArrayList<PeerStatistic>(peers.peerAddresses().keySet());
		
		for(int i=0; i<size && !keys.isEmpty(); i++) {
			PeerStatistic key = keys.get( random.nextInt(keys.size()) );
			Data value = peers.peerAddresses().get(key);
			if(value != null) {
				retVal.put(key, value);
			} else {
				//not there anymore
				i--;
			}
		}
		
		TrackerData data = new TrackerData(retVal, peers.peerAddresses().size() > size);
		return data;
	}

	public static TrackerData disjunction(TrackerData meshPeers, SimpleBloomFilter<Number160> knownPeers) {
		TrackerData trackerData = new TrackerData(new HashMap<PeerStatistic, Data>());
		for (Map.Entry<PeerStatistic, Data> entry : meshPeers.peerAddresses().entrySet()) {
			if (!knownPeers.contains(entry.getKey().peerAddress().peerId())) {
				trackerData.put(entry.getKey(), entry.getValue());
			}
		}
		return trackerData;
	}
}
