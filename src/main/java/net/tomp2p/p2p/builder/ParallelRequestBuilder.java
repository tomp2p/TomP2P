package net.tomp2p.p2p.builder;

import java.util.NavigableSet;
import java.util.TreeSet;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.DistributedHashTable.Operation;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class ParallelRequestBuilder extends DHTBuilder<ParallelRequestBuilder>
{
	private NavigableSet<PeerAddress> queue;
	private Operation operation;
	private boolean cancelOnFinish = false;
	public ParallelRequestBuilder(Peer peer, Number160 locationKey)
	{
		super(peer, locationKey);
		self(this);
	}
	
	public NavigableSet<PeerAddress> getQueue()
	{
		return queue;
	}

	public ParallelRequestBuilder setQueue(NavigableSet<PeerAddress> queue)
	{
		this.queue = queue;
		return this;
	}
	
	public ParallelRequestBuilder add(PeerAddress peerAddress)
	{
		if(queue == null)
		{
			queue = new TreeSet<PeerAddress>(peer.getPeerBean().getPeerMap().createPeerComparator());
		}
		queue.add(peerAddress);
		return this;
	}

	public Operation getOperation()
	{
		return operation;
	}

	public ParallelRequestBuilder setOperation(Operation operation)
	{
		this.operation = operation;
		return this;
	}

	public boolean isCancelOnFinish()
	{
		return cancelOnFinish;
	}
	
	public ParallelRequestBuilder setCancelOnFinish()
	{
		this.cancelOnFinish = true;
		return this;
	}

	public ParallelRequestBuilder setCancelOnFinish(boolean cancelOnFinish)
	{
		this.cancelOnFinish = cancelOnFinish;
		return this;
	}
	
	@Override
	public FutureDHT build()
	{
		preBuild("parallel-builder");
		if(queue == null || queue.size() == 0)
		{
			throw new IllegalArgumentException("queue cannot be empty");
		}
		return peer.getDistributedHashMap().parallelRequests(requestP2PConfiguration, 
				queue, cancelOnFinish, futureChannelCreator, peer.getConnectionBean().getConnectionReservation(), 
				manualCleanup, operation);
	}
}
