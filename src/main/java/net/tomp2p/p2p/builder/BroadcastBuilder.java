package net.tomp2p.p2p.builder;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class BroadcastBuilder
{
	final private Peer peer;
	final private Number160 messageKey;
	private Map<Number160, Data> dataMap;
	private Boolean isUDP;
	
	public BroadcastBuilder(Peer peer, Number160 messageKey)
	{
		this.peer = peer;
		this.messageKey = messageKey;
	}
	
	public void start()
	{
		if(isUDP == null)
		{
			//not set, decide based on the data
			if(getDataMap() == null)
			{
				setIsUDP(true);
			}
			else
			{
				setIsUDP(false);
			}
		}
		if(dataMap == null)
		{
			setDataMap(new HashMap<Number160, Data>());
		}
		peer.getBroadcastRPC().getBroadcastHandler().receive(messageKey, dataMap, 0, isUDP);
	}

	public Map<Number160, Data> getDataMap()
	{
		return dataMap;
	}

	public BroadcastBuilder setDataMap(Map<Number160, Data> dataMap)
	{
		this.dataMap = dataMap;
		return this;
	}

	public boolean isUDP()
	{
		if(isUDP == null)
		{
			return false;
		}
		return isUDP;
	}

	public BroadcastBuilder setIsUDP(boolean isUDP)
	{
		this.isUDP = isUDP;
		return this;
	}
}
