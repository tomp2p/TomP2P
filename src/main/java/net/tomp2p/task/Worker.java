package net.tomp2p.task;
import java.io.Serializable;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage;

public interface Worker extends Serializable
{
	public abstract Map<Number160, Data> execute(Map<Number160, Data> inputData, Storage storage) throws Exception;
}
