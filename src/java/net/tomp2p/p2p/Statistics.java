package net.tomp2p.p2p;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class Statistics
{
	private final static double MAX = Math.pow(2, Number160.BITS);
	private double estimatedNumberOfPeers = 1;
	private double avgGap = MAX/2;

	// TODO: with increaing number of peers, the diff gets lower and lower
	public void triggerStatUpdate(List<Map<Number160, PeerAddress>> peerMap,
			PeerAddress remotePeer, boolean insert, int currentSize, int maxSize, int bagSize)
	{
		// we know it exactly
		if (currentSize < maxSize)
		{
			estimatedNumberOfPeers = currentSize + 1;
			return;
		}
		// otherwise we know we are full!
		double gap = 0D;
		int gapCount = 0;
		for (int i = 0; i < Number160.BITS; i++)
		{
			Map<Number160, PeerAddress> peers = peerMap.get(i);
			synchronized (peers)
			{
				int numPeers = peers.size();
				if (numPeers > 0 && numPeers < bagSize)
				{
					gap += Math.pow(2, i) / numPeers;
					gapCount++;
				}
			}
		}
		avgGap = gap / gapCount;
		estimatedNumberOfPeers = (MAX / avgGap);
	}

	public double getEstimatedNumberOfNodes()
	{
		return estimatedNumberOfPeers;
	}

	public double getAvgGap()
	{
		return avgGap;
	}
}
