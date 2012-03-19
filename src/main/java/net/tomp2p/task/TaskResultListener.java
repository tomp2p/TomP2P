package net.tomp2p.task;

import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public interface TaskResultListener
{
	public abstract void taskReceived(Number160 taskId, Map<Number160, Data> dataMap);
	
	public abstract void taskFailed(Number160 taskId);
}
