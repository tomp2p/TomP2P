package net.tomp2p.mapreduce;

import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Storage;

public interface Reducer
{
	public abstract void reduce(Number480 key, Storage storage) throws Exception;
}
