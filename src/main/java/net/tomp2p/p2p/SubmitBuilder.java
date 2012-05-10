package net.tomp2p.p2p;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureTask;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.task.Worker;

public class SubmitBuilder
{
	private final static Map<Number160, Data> EMPTY_MAP = new HashMap<Number160, Data>();
	private final Number160 locationKey;
	private final Worker worker;
	private final DistributedTask distributedTask;
	private final Peer peer;
	//
	private Map<Number160, Data> dataMap;
	private RoutingConfiguration routingConfiguration;
	private RequestP2PConfiguration requestP2PConfiguration;
	private FutureChannelCreator futureChannelCreator;
	private boolean signMessage = false;
	private boolean isAutomaticCleanup = true;
	//
	public SubmitBuilder(Peer peer, DistributedTask distributedTask, Number160 locationKey, Worker worker)
	{
		this.peer = peer;
		this.distributedTask = distributedTask;
		this.locationKey = locationKey;
		this.worker = worker;
	}

	public Map<Number160, Data> getDataMap()
	{
		return dataMap;
	}

	public SubmitBuilder setDataMap(Map<Number160, Data> dataMap)
	{
		this.dataMap = dataMap;
		return this;
	}
	
	public RoutingConfiguration getRoutingConfiguration()
	{
		return routingConfiguration;
	}

	public SubmitBuilder setRoutingConfiguration(RoutingConfiguration routingConfiguration)
	{
		this.routingConfiguration = routingConfiguration;
		return this;
	}
	
	public RequestP2PConfiguration getRequestP2PConfiguration()
	{
		return requestP2PConfiguration;
	}

	public SubmitBuilder setRequestP2PConfiguration(RequestP2PConfiguration requestP2PConfiguration)
	{
		this.requestP2PConfiguration = requestP2PConfiguration;
		return this;
	}

	public FutureChannelCreator getFutureChannelCreator()
	{
		return futureChannelCreator;
	}

	public SubmitBuilder setFutureChannelCreator(FutureChannelCreator futureChannelCreator)
	{
		this.futureChannelCreator = futureChannelCreator;
		return this;
	}

	public boolean isSignMessage()
	{
		return signMessage;
	}

	public SubmitBuilder setSignMessage(boolean signMessage)
	{
		this.signMessage = signMessage;
		return this;
	}

	public boolean isAutomaticCleanup()
	{
		return isAutomaticCleanup;
	}

	public SubmitBuilder setAutomaticCleanup(boolean isAutomaticCleanup)
	{
		this.isAutomaticCleanup = isAutomaticCleanup;
		return this;
	}
	
	public FutureTask submit()
	{
		if(dataMap == null)
		{
			dataMap = EMPTY_MAP;
		}
		if(routingConfiguration == null)
		{
			routingConfiguration = new RoutingConfiguration(3, 5, 10, 2);
		}
		if(requestP2PConfiguration == null)
		{
			requestP2PConfiguration = new RequestP2PConfiguration(1, 0, 1);
		}
		if(futureChannelCreator == null)
		{
			futureChannelCreator = peer.reserve(routingConfiguration, requestP2PConfiguration, "submit-builder");
		}	
		return distributedTask.submit(locationKey, dataMap, worker, routingConfiguration, requestP2PConfiguration, 
				futureChannelCreator, signMessage, isAutomaticCleanup, peer.getConnectionBean().getConnectionReservation());
	}	
}