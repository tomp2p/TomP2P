package net.tomp2p.mapreduce;


import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class MapReducePeer extends Peer
{

	public MapReducePeer(Number160 nodeId)
	{
		super(nodeId);
	}

	public FutureMapReduce map(Number160 createHash, Data data, Mapper map)
	{
		return null;
		
	}

	public FutureReduce reduce(Number160 createHash, Data data, Reducer reducer)
	{
		return null;
		
	}
}
