package net.tomp2p.p2p;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.message.Message;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.ConcurrentCacheMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * As seen in http://www.hiradastechnika.hu/data/upload/file/2009/2009%20I/
 * Pages_from_HT0901a_2.pdf
 * 
 * @author Thomas Bocek
 *
 */
public class StructuredBroadcastHandler implements BroadcastHandler {

	private static final Logger LOG = LoggerFactory
			.getLogger(StructuredBroadcastHandler.class);
	// redundancy, if set to 1, we should get exactly the number of messages as the number of peers
	private static final int FROM_EACH_BAG = 3;
	
	private static final AtomicInteger broadcastCounter = new AtomicInteger(0);
	private static final AtomicInteger messageCounter = new AtomicInteger(0);
	private final ConcurrentCacheMap<Number160, Boolean> cache = new ConcurrentCacheMap<Number160, Boolean>();
	private volatile Peer peer;

	public StructuredBroadcastHandler init(final Peer peer) {
		this.peer = peer;
		return this;
	}

	/**
	 * Used in JUnit tests only.
	 * 
	 * @return Return the number of peer in the debug set
	 */
	public int broadcastCounter() {
		return broadcastCounter.get();
	}
	
	public int messageCounter() {
		return messageCounter.get();
	}

	@Override
	public StructuredBroadcastHandler receive(final Message message) {
		if (peer == null) {
			throw new RuntimeException(
					"Init never called. This should be done by the PeerBuilder");
		}
		final Number160 messageKey = message.key(0);
		final NavigableMap<Number640, Data> dataMap;
		if (message.dataMap(0) != null) {
			dataMap = message.dataMap(0).dataMap();
		} else {
			dataMap = null;
		}
		final int hopCount = message.intAt(0);
		final int bucketNr = message.intAt(1);
		LOG.debug("I {} received a message", peer.peerID());
		if (twiceSeen(messageKey)) {
			LOG.debug("already forwarded this message in {}", peer.peerID());
			return this;
		}
		LOG.debug("got broadcast map {} from {}", dataMap, peer.peerID());

		broadcastCounter.incrementAndGet();
		if (hopCount < peer.peerBean().peerMap().nrFilledBags()) {
			if (hopCount == 0) {
				LOG.debug("zero hop");
				firstPeer(messageKey, dataMap, hopCount);
			} else {
				LOG.debug("more hop");
				otherPeer(message.sender().peerId(), messageKey, dataMap,
						hopCount, bucketNr);
			}
		} else {
			LOG.debug("max hop reached in {}", peer.peerID());
		}
		LOG.debug("done");
		return this;
	}

	/**
	 * If a message is seen for the second time, then we don't want to send this
	 * message again. The cache has a size of 1024 entries and the objects have
	 * a default lifetime of 60s.
	 * 
	 * @param messageKey
	 *            The key of the message
	 * @return True if this message was send withing the last 60 seconds.
	 */
	private boolean twiceSeen(final Number160 messageKey) {
		Boolean isInCache = cache.putIfAbsent(messageKey, Boolean.TRUE);
		if (isInCache != null) {
			// ttl refresh
			cache.put(messageKey, Boolean.TRUE);
			return true;
		}
		return false;
	}

	/**
	 * The first peer is the initiator. This peer that wants to start the
	 * broadcast will send it to all its neighbors. Since this peer has an
	 * interest in sending, it should also work more than the other peers.
	 * 
	 * @param messageKey
	 *            The key of the message
	 * @param dataMap
	 *            The data map to send around
	 * @param hopCounter
	 *            The number of hops
	 */
	private void firstPeer(final Number160 messageKey,
			final NavigableMap<Number640, Data> dataMap, final int hopCounter) {
		final List<PeerAddress> list = peer.peerBean().peerMap()
				.fromEachBag(FROM_EACH_BAG, Number160.BITS);
		for (final PeerAddress peerAddress : list) {
			final int bucketNr = PeerMap.classMember(peerAddress.peerId(),
					peer.peerID());
			doSend(messageKey, dataMap, hopCounter, peerAddress,
					bucketNr);
		}
	}

	/**
	 * This method is called on relaying peers. We select a random set and we
	 * send the message to those random peers.
	 * 
	 * @param messageKey
	 *            The key of the message
	 * @param dataMap
	 *            The data map to send around
	 * @param hopCounter
	 *            The number of hops
	 */
	private void otherPeer(Number160 sender, Number160 messageKey,
			NavigableMap<Number640, Data> dataMap, int hopCounter,
			final int bucketNr) {
		final List<PeerAddress> list = peer.peerBean().peerMap()
				.fromEachBag(FROM_EACH_BAG, bucketNr);
		for (final PeerAddress peerAddress : list) {
			final int bucketNr2 = PeerMap.classMember(peerAddress.peerId(),
					peer.peerID());
			doSend(messageKey, dataMap, hopCounter, peerAddress,
					bucketNr2);
		}
	}

	private void doSend(final Number160 messageKey,
			final NavigableMap<Number640, Data> dataMap, final int hopCounter,
			final PeerAddress peerAddress,
			final int bucketNr) {

		
					BroadcastBuilder broadcastBuilder = new BroadcastBuilder(
							peer, messageKey);
					broadcastBuilder.dataMap(dataMap);
					broadcastBuilder.hopCounter(hopCounter + 1);
					/*Triple<FutureDone<Message>, FutureDone<SctpChannelFacade>, FutureDone<Void>> p = peer.broadcastRPC()
							.send(peerAddress, broadcastBuilder,
									future.channelCreator(), broadcastBuilder,
									bucketNr);*/
					//TODO: make this work again
					LOG.debug("send to {}", peerAddress);
					messageCounter.incrementAndGet();
					
	}
}
