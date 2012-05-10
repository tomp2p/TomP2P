package net.tomp2p.task;
import java.io.Serializable;
import java.util.Map;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public interface Worker extends Serializable
{
	public abstract Map<Number160, Data> execute(Peer peer, Map<Number160, Data> inputData) throws Exception;
}
