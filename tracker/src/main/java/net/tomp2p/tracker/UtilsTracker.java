package net.tomp2p.tracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

public class UtilsTracker {
	public static TrackerData limit(TrackerData peers, int size) {
		Map<PeerAddress, Data> retVal = new HashMap<PeerAddress, Data>(size);
		
		Random random = new Random();
		List<PeerAddress> keys = new ArrayList<PeerAddress>(peers.peerAddresses().keySet());
		
		for(int i=0; i<size && !keys.isEmpty(); i++) {
			PeerAddress key = keys.get( random.nextInt(keys.size()) );
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
		TrackerData trackerData = new TrackerData(new HashMap<PeerAddress, Data>());
		for (Map.Entry<PeerAddress, Data> entry : meshPeers.peerAddresses().entrySet()) {
			if (!knownPeers.contains(entry.getKey().peerId())) {
				trackerData.put(entry.getKey(), entry.getValue());
			}
		}
		return trackerData;
	}
	
	/**
     * Adds a listener to the response future and releases all acquired channels in channel creator.
     * 
     * @param channelCreator
     *            The channel creator that will be shutdown and all connections will be closed
     * @param baseFutures
     *            The futures to listen to. If all the futures finished, then the channel creator is shutdown. If null
     *            provided, the channel creator is shutdown immediately.
     */
	public static void addReleaseListener(final ChannelCreator channelCreator,
			final FutureTracker futureTracker) {
		if (futureTracker == null) {
			channelCreator.shutdown();
			return;
		}

		futureTracker.addListener(new BaseFutureAdapter<FutureTracker>() {
			@Override
			public void operationComplete(final FutureTracker future)
					throws Exception {
				final FutureDone<Void> futuresCompleted = futureTracker
						.futuresCompleted();
				if (futuresCompleted != null) {
					futureTracker.futuresCompleted().addListener(
							new BaseFutureAdapter<FutureDone<Void>>() {
								@Override
								public void operationComplete(
										final FutureDone<Void> future)
										throws Exception {
									channelCreator.shutdown();
								}
							});
				} else {
					channelCreator.shutdown();
				}
			}
		});

	}
}
