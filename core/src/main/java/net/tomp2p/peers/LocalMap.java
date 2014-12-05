package net.tomp2p.peers;

import java.util.Collection;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.PeerException;
import net.tomp2p.utils.ConcurrentCacheMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalMap implements Maintainable, PeerStatusListener {
	
	private static final Logger LOG = LoggerFactory.getLogger(LocalMap.class);
	
    // the storage for the peers that are verified
    private final ConcurrentCacheMap<Number160, PeerStatistic> localMap;
    private final ConcurrentCacheMap<Number160, PeerAddress> localMapRev;
    private final ConcurrentCacheMap<Number160, Boolean> offlineMap;
    private final int[] intervalSeconds;
    
    public LocalMap() {
    	this(new LocalMapConf());
    }
    
    public LocalMap(LocalMapConf localMapConf) {
    	localMap = new ConcurrentCacheMap<Number160, PeerStatistic>(localMapConf.localMapTimout(), localMapConf.localMapSize());
    	localMapRev = new ConcurrentCacheMap<Number160, PeerAddress>(localMapConf.localMapRevTimeout(), localMapConf.localMapRevSize());
    	offlineMap = new ConcurrentCacheMap<Number160, Boolean>(localMapConf.offlineMapTimout(), localMapConf.offlineMapSize());
    	intervalSeconds = localMapConf.intervalSeconds();
    }

	@Override
    public PeerStatistic nextForMaintenance(Collection<PeerAddress> notInterestedAddresses) {
	    for(PeerStatistic peerStatistic:localMap.values()) {
	    	if(DefaultMaintenance.needMaintenance(peerStatistic, intervalSeconds)) {
	    		return peerStatistic;
	    	}
	    }
	    return null;
    }

	@Override
    public boolean peerFailed(PeerAddress remotePeer, PeerException exception) {
		PeerStatistic ps = localMap.remove(remotePeer.peerId());
		localMapRev.remove(remotePeer.peerId());
		offlineMap.put(remotePeer.peerId(), Boolean.TRUE);
		return ps != null;
    }

	@Override
    public boolean peerFound(PeerAddress remotePeer, PeerAddress referrer, PeerConnection peerConnection) {
		//do nothing, here we get to know all the peers, we are interested only in those we have stored
        return false;
    }
	
    public boolean peerFound(PeerAddress remotePeer, PeerAddress referrer) {
    	//this is called from the RPC method, here we only get the local peers
		LOG.debug("local peer {} is online reporter was {}", remotePeer, referrer);
        boolean firstHand = referrer == null;
        //if we got contacted by this peer, but we did not initiate the connection
        boolean secondHand = remotePeer.equals(referrer);
        
        if (firstHand || secondHand) {
            offlineMap.remove(remotePeer.peerId());
            PeerStatistic peerStatatistic = PeerMap.updateExistingVerifiedPeerAddress(localMap, remotePeer, firstHand);
            if(peerStatatistic == null) {
            	peerStatatistic = new PeerStatistic(remotePeer);
            	localMap.put(remotePeer.peerId(), peerStatatistic);
            }
            peerStatatistic.successfullyChecked();
            peerStatatistic.local();
            return true;
        }
        return false;
    }
	
	public PeerStatistic translate(PeerAddress remotePeer) {
		PeerStatistic peerStatistic = localMap.get(remotePeer.peerId());
		if(peerStatistic != null) {
			localMapRev.put(remotePeer.peerId(), remotePeer);
		}
		return peerStatistic;
	}
	
	public PeerAddress translateReverse(PeerAddress remotePeer) {
		PeerAddress peerAddress = localMapRev.get(remotePeer.peerId());
		return peerAddress;
	}
	
	public int size() {
		return localMap.size();
	}
}
