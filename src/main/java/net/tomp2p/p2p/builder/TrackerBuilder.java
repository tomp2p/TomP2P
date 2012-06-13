package net.tomp2p.p2p.builder;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.peers.Number160;

public abstract class TrackerBuilder<K extends TrackerBuilder<K>>
{
	public final static Number160 DEFAULT_DOMAIN = Number160.createHash("default-tracker");	
	protected final Peer peer;
	protected final Number160 locationKey;
	//
	protected Number160 domainKey;
	protected RoutingConfiguration routingConfiguration;
	protected TrackerConfiguration trackerConfiguration;
	protected FutureChannelCreator futureChannelCreator;
	
	private K self;
	
	public TrackerBuilder(Peer peer, Number160 locationKey)
	{
		this.peer = peer;
		this.locationKey = locationKey;
	}
	
	public void self(K self)
	{
		this.self = self;
	}
	
	public Number160 getDomainKey()
	{
		return domainKey;
	}

	public K setDomainKey(Number160 domainKey)
	{
		this.domainKey = domainKey;
		return self;
	}

	public RoutingConfiguration getRoutingConfiguration()
	{
		return routingConfiguration;
	}

	public K setRoutingConfiguration(RoutingConfiguration routingConfiguration)
	{
		this.routingConfiguration = routingConfiguration;
		return self;
	}

	public TrackerConfiguration getTrackerConfiguration()
	{
		return trackerConfiguration;
	}

	public K setTrackerConfiguration(TrackerConfiguration trackerConfiguration)
	{
		this.trackerConfiguration = trackerConfiguration;
		return self;
	}

	public FutureChannelCreator getFutureChannelCreator()
	{
		return futureChannelCreator;
	}

	public K setFutureChannelCreator(FutureChannelCreator futureChannelCreator)
	{
		this.futureChannelCreator = futureChannelCreator;
		return self;
	}
	
	public void preBuild(String name)
	{
		if(domainKey == null)
		{
			domainKey = DEFAULT_DOMAIN;
		}
		if(routingConfiguration == null)
		{
			routingConfiguration = new RoutingConfiguration(5, 10, 2);
		}
		if(trackerConfiguration == null)
		{
			int size = peer.getPeerBean().getPeerMap().size() + 1;
			trackerConfiguration = new TrackerConfiguration(Math.min(size, 3), 5, 3, 30);
		}
		if(futureChannelCreator == null)
		{
			int conn = Math.max(routingConfiguration.getParallel(), trackerConfiguration.getParallel());
			futureChannelCreator = peer.getConnectionBean().getConnectionReservation().reserve(conn);
		}
	}
	
	public abstract FutureTracker build();
}
