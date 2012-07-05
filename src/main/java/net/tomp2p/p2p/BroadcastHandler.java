package net.tomp2p.p2p;

import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public interface BroadcastHandler
{
	public abstract void receive(Number160 messageKey, Map<Number160, Data> dataMap, int hopCounter, boolean isUDP);
}
