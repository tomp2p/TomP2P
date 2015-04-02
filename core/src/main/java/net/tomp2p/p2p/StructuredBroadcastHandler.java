package net.tomp2p.p2p;

import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: check message numbers!!
public class StructuredBroadcastHandler extends DefaultBroadcastHandler {
	
	private static final Logger LOG = LoggerFactory.getLogger(StructuredBroadcastHandler.class);
	
	private static final int FROM_EACH_BAG = 2;
	
	public StructuredBroadcastHandler(final Random rnd) {
		super(rnd);		
	}

	@Override
	public void firstPeer(final Number160 messageKey, final NavigableMap<Number640, Data> dataMap, final int hopCounter, final boolean isUDP) {
	    System.err.println("first: "+peer().peerID());
		
		final List<PeerAddress> list = peer().peerBean().peerMap().fromEachBag(FROM_EACH_BAG, Number160.BITS);
		for (final PeerAddress peerAddress : list) {
			final int bucketNr = PeerMap.classMember(peerAddress.peerId(), peer().peerID());
			doSend(messageKey, dataMap, hopCounter, isUDP, peerAddress, bucketNr);
		}
		System.err.println("sent to "+list.size());
		
	}
	
	@Override
	public void otherPeer(Number160 sender, Number160 messageKey, NavigableMap<Number640, Data> dataMap, 
			int hopCounter, boolean isUDP, final int bucketNr) {
		System.err.println("second");
		
		final List<PeerAddress> list = peer().peerBean().peerMap().fromEachBag(FROM_EACH_BAG, bucketNr);
		for (final PeerAddress peerAddress : list) {
			final int bucketNr2 = PeerMap.classMember(peerAddress.peerId(), peer().peerID());
			doSend(messageKey, dataMap, hopCounter, isUDP, peerAddress, bucketNr2);
		}
		System.err.println("sent to "+list.size());
	}

	private void doSend(final Number160 messageKey, final NavigableMap<Number640, Data> dataMap, final int hopCounter,
            final boolean isUDP, final PeerAddress peerAddress, final int bucketNr) {
		System.err.println("send to "+peerAddress);
	    FutureChannelCreator frr = peer().connectionBean().reservation().create(isUDP ? 1 : 0, isUDP ? 0 : 1);
	    frr.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
	    	@Override
	    	public void operationComplete(final FutureChannelCreator future) throws Exception {
	    		if (future.isSuccess()) {
	    			BroadcastBuilder broadcastBuilder = new BroadcastBuilder(peer(), messageKey);
	    			broadcastBuilder.dataMap(dataMap);
	    			broadcastBuilder.hopCounter(hopCounter + 1);
	    			broadcastBuilder.udp(isUDP);
	    			FutureResponse futureResponse = peer().broadcastRPC().send(peerAddress, broadcastBuilder,
	    			        future.channelCreator(), broadcastBuilder, bucketNr);
	    			LOG.debug("send to {}", peerAddress);
	    			Utils.addReleaseListener(future.channelCreator(), futureResponse);
	    		} else {
	    			Utils.addReleaseListener(future.channelCreator());
	    		}
	    	}
	    });
    }
	
	

	

}
