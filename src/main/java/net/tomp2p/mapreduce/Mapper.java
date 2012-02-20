package net.tomp2p.mapreduce;


import java.io.Serializable;

import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Storage;



public interface Mapper extends Serializable
{
	public abstract void map(FutureMap futureMap, MapReducePeer remotePeer, Number480 key, Storage storage) throws Exception;
}
