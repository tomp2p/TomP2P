package net.tomp2p.p2p;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class AddBuilder
{
	private final Peer peer;
	private final DistributedHashTable distributedHashMap;
	private final Number160 locationKey;
	//
	private Number160 domainKey;
	private Collection<Data> dataSet;
	private Data data;
	private RoutingConfiguration routingConfiguration;
	private RequestP2PConfiguration requestP2PConfiguration;
	private FutureCreate<FutureDHT> futureCreate;
	private FutureChannelCreator futureChannelCreator;
	//
	private boolean protectDomain = false;
	private boolean signMessage = false;
	private boolean isAutomaticCleanup = true;
	private boolean list = false;
	
	public AddBuilder(Peer peer, DistributedHashTable distributedHashMap, Number160 locationKey)
	{
		this.peer = peer;
		this.distributedHashMap = distributedHashMap;
		this.locationKey = locationKey;
	}
	
	public Number160 getDomainKey()
	{
		return domainKey;
	}

	public AddBuilder setDomainKey(Number160 domainKey)
	{
		this.domainKey = domainKey;
		return this;
	}
	
	public Collection<Data> getDataSet()
	{
		return dataSet;
	}

	public AddBuilder setDataSet(Collection<Data> dataSet)
	{
		this.dataSet = dataSet;
		return this;
	}
	
	public Data getData()
	{
		return data;
	}

	public AddBuilder setData(Data data)
	{
		this.data = data;
		return this;
	}
	
	public RoutingConfiguration getRoutingConfiguration()
	{
		return routingConfiguration;
	}

	public AddBuilder setRoutingConfiguration(RoutingConfiguration routingConfiguration)
	{
		this.routingConfiguration = routingConfiguration;
		return this;
	}

	public RequestP2PConfiguration getRequestP2PConfiguration()
	{
		return requestP2PConfiguration;
	}

	public AddBuilder setRequestP2PConfiguration(RequestP2PConfiguration requestP2PConfiguration)
	{
		this.requestP2PConfiguration = requestP2PConfiguration;
		return this;
	}
	
	public FutureCreate<FutureDHT> getFutureCreate()
	{
		return futureCreate;
	}

	public AddBuilder setFutureCreate(FutureCreate<FutureDHT> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}
	
	public FutureChannelCreator getFutureChannelCreator()
	{
		return futureChannelCreator;
	}

	public AddBuilder setFutureChannelCreator(FutureChannelCreator futureChannelCreator)
	{
		this.futureChannelCreator = futureChannelCreator;
		return this;
	}
	
	public boolean isProtectDomain()
	{
		return protectDomain;
	}

	public AddBuilder setProtectDomain(boolean protectDomain)
	{
		this.protectDomain = protectDomain;
		return this;
	}

	public boolean isSignMessage()
	{
		return signMessage;
	}

	public AddBuilder setSignMessage(boolean signMessage)
	{
		this.signMessage = signMessage;
		return this;
	}

	public boolean isAutomaticCleanup()
	{
		return isAutomaticCleanup;
	}

	public AddBuilder setAutomaticCleanup(boolean isAutomaticCleanup)
	{
		this.isAutomaticCleanup = isAutomaticCleanup;
		return this;
	}
	
	public boolean isList()
	{
		return list;
	}

	public AddBuilder setList(boolean list)
	{
		this.list = list;
		return this;
	}
	
	public FutureDHT add()
	{
		if(domainKey == null)
		{
			domainKey = Configurations.DEFAULT_DOMAIN;
		}
		if(dataSet == null)
		{
			dataSet = new ArrayList<Data>(1);
		}
		if(data != null)
		{
			dataSet.add(data);
		}
		if(dataSet.size()==0)
		{
			throw new IllegalArgumentException("You must either set data via setDataMap() or setData(). Cannot add nothing.");
		}
		if(routingConfiguration == null)
		{
			routingConfiguration = new RoutingConfiguration(5, 10, 2);
		}
		if(requestP2PConfiguration == null)
		{
			requestP2PConfiguration = new RequestP2PConfiguration(3, 5, 3);
		}
		if(futureChannelCreator == null)
		{
			futureChannelCreator = peer.reserve(routingConfiguration, requestP2PConfiguration, "submit-builder");
		}
		return distributedHashMap.add(locationKey, domainKey, dataSet, routingConfiguration, requestP2PConfiguration, 
				protectDomain, signMessage, isAutomaticCleanup, list, futureCreate, futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
	}

	
}