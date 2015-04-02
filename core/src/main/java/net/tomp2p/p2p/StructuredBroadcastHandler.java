package net.tomp2p.p2p;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.peers.PeerMap.Direction;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class StructuredBroadcastHandler extends DefaultBroadcastHandler {
	
	private static final Logger LOG = LoggerFactory.getLogger(StructuredBroadcastHandler.class);
	
	
	
	private static final int FROM_EACH_BAG = 1;
	
	public StructuredBroadcastHandler(final Random rnd) {
		super(rnd);		
	}

	@Override
	public void firstPeer(final Number160 messageKey, final NavigableMap<Number640, Data> dataMap, final int hopCounter, final boolean isUDP) {
	    System.err.println("first: "+peer().peerID());
		//short jumps
	    final List<PeerAddress> sent = new ArrayList<PeerAddress>(2);
	    NavigableSet<PeerStatistic> closestLeft = peer().peerBean().peerMap().closePeers(1, PeerMap.Direction.LEFT);
		if(closestLeft.size() > 0) {
			PeerAddress sendTo = closestLeft.iterator().next().peerAddress();
			doSend(messageKey, dataMap, hopCounter, isUDP, sendTo, true);
			sent.add(sendTo);
		}
		NavigableSet<PeerStatistic> closestRight = peer().peerBean().peerMap().closePeers(1, PeerMap.Direction.RIGHT);
		if(closestRight.size() > 0) {
			PeerAddress sendTo = closestRight.iterator().next().peerAddress();
			doSend(messageKey, dataMap, hopCounter, isUDP, sendTo, true);
			sent.add(sendTo);
		}
		System.err.println("long");
		//long jumps
		final List<PeerAddress> list = peer().peerBean().peerMap().fromEachBag(FROM_EACH_BAG);
		list.removeAll(sent);
		for (final PeerAddress peerAddress : list) {
			doSend(messageKey, dataMap, hopCounter, isUDP, peerAddress, false);
		}
		System.err.println("sent to "+list.size());
		
	}

	private void doSend(final Number160 messageKey, final NavigableMap<Number640, Data> dataMap, final int hopCounter,
            final boolean isUDP, final PeerAddress peerAddress, final boolean shortJump) {
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
	    			        future.channelCreator(), broadcastBuilder, shortJump);
	    			LOG.debug("send to {}", peerAddress);
	    			Utils.addReleaseListener(future.channelCreator(), futureResponse);
	    		} else {
	    			Utils.addReleaseListener(future.channelCreator());
	    		}
	    	}
	    });
    }
	
	

	@Override
	public void otherPeer(Number160 sender, Number160 messageKey, NavigableMap<Number640, Data> dataMap, 
			int hopCounter, boolean isUDP, boolean isShortJump) {
		System.err.println("second");
		
		final List<PeerAddress> sent = new ArrayList<PeerAddress>(2);
	    if(isShortJump) {
	    	System.err.println("short jump");
	    	final Direction direction;
	    	//figure out which direction
	    	if(sender.compareTo(peer().peerID()) < 0) {
	    		direction=Direction.LEFT;
	    	} else {
	    		direction=Direction.RIGHT;
	    	}
	    	NavigableSet<PeerStatistic> closest = peer().peerBean().peerMap().closePeers(1, direction);
	    	if(closest.size() > 0) {
	    		PeerAddress sendTo = closest.iterator().next().peerAddress();
	    		doSend(messageKey, dataMap, hopCounter, isUDP, sendTo, true);
	    		sent.add(sendTo);
	    	}
	    } else {
	    	NavigableSet<PeerStatistic> closestLeft = peer().peerBean().peerMap().closePeers(1, PeerMap.Direction.LEFT);
			if(closestLeft.size() > 0) {
				PeerAddress sendTo = closestLeft.iterator().next().peerAddress();
				doSend(messageKey, dataMap, hopCounter, isUDP, sendTo, true);
				sent.add(sendTo);
			}
			NavigableSet<PeerStatistic> closestRight = peer().peerBean().peerMap().closePeers(1, PeerMap.Direction.RIGHT);
			if(closestRight.size() > 0) {
				PeerAddress sendTo = closestRight.iterator().next().peerAddress();
				doSend(messageKey, dataMap, hopCounter, isUDP, sendTo, true);
				sent.add(sendTo);
			}
	    }
	    
	    System.err.println("long jump");
    	//pick far away peer
    	NavigableSet<PeerStatistic> farPeers = peer().peerBean().peerMap().closePeers(peer().peerID().xor(Number160.MAX_VALUE), 1);
    	if(farPeers.size() > 0) {
    		doSend(messageKey, dataMap, hopCounter, isUDP, farPeers.iterator().next().peerAddress(), false);
    	}
    	/*final List<PeerAddress> list = peer().peerBean().peerMap().fromEachBag(1);
    	if(list.size() > 0) {
    		for(int i=1;i<10;i++) {
    			doSend(messageKey, dataMap, hopCounter, isUDP, list.get(list.size()-i), false);
    		}
    	}*/
	    
	}

}
