package net.tomp2p.p2p;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class Statistics {
    private final static double MAX = Math.pow(2, Number160.BITS);
    private double estimatedNumberOfPeers = 1;
    private double avgGap = MAX / 2;
    //
    private final List<Map<Number160, PeerAddress>> peerMap;
    private final Number160 remotePeer;
    private final int maxSize;
    private final int bagSize;

    //
    private int currentSize;

    public Statistics(List<Map<Number160, PeerAddress>> peerMap,
	    Number160 remotePeer, int maxSize, int bagSize) {
	this.peerMap = peerMap;
	this.remotePeer = remotePeer;
	this.maxSize = maxSize;
	this.bagSize = bagSize;
    }

    // TODO: with increasing number of peers, the diff gets lower and lower
    public void triggerStatUpdate(boolean insert, int currentSize) {
	this.currentSize = currentSize;
    }

    public double getEstimatedNumberOfNodes() {
	// we know it exactly
	if (currentSize < maxSize) {
	    estimatedNumberOfPeers = currentSize;
	    return estimatedNumberOfPeers;
	}
	// otherwise we know we are full!
	double gap = 0D;
	int gapCount = 0;
	int oldNumPeers = 0;
	for (int i = Number160.BITS - 1; i >= 0; i--) {
	    Map<Number160, PeerAddress> peers = peerMap.get(i);
	    int numPeers = peers.size();
	    // System.out.print(numPeers);
	    // System.out.print(",");
	    if (numPeers > 0 && (numPeers < bagSize || numPeers < oldNumPeers)) {
		double currentGap = Math.pow(2, i) / numPeers;
		// System.out.print("gap("+i+"):"+currentGap+";");
		gap += currentGap * numPeers;
		gapCount += numPeers;
	    } else {
		// System.out.print("ignoring "+i+";");
	    }
	    oldNumPeers = numPeers;
	}
	// System.out.print("\n");
	avgGap = gap / gapCount;
	estimatedNumberOfPeers = (MAX / avgGap);
	return estimatedNumberOfPeers;
    }

    public double getAvgGap() {
	return avgGap;
    }
    
    public static void tooClose(Collection<PeerAddress> collection)
    {
	
    }
}
