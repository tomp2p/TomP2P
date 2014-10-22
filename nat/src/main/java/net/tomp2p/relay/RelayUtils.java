package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatatistic;

public class RelayUtils {

	private RelayUtils() {
		// only static methods
	}

	public static List<Map<Number160, PeerStatatistic>> unflatten(Collection<PeerAddress> map, PeerAddress sender) {
		PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(sender.peerId());
		PeerMap peerMap = new PeerMap(peerMapConfiguration);
		for (PeerAddress peerAddress : map) {
			peerMap.peerFound(peerAddress, null, null);
		}
		return peerMap.peerMapVerified();
	}

	public static Collection<PeerAddress> flatten(List<Map<Number160, PeerStatatistic>> maps) {
		Collection<PeerAddress> result = new ArrayList<PeerAddress>();
		for (Map<Number160, PeerStatatistic> map : maps) {
			for (PeerStatatistic peerStatatistic : map.values()) {
				result.add(peerStatatistic.peerAddress());
			}
		}
		return result;
	}
}
