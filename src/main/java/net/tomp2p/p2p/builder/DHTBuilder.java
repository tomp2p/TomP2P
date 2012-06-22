package net.tomp2p.p2p.builder;

import java.util.concurrent.ScheduledFuture;

import net.tomp2p.futures.Cancellable;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCleanup;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;

public abstract class DHTBuilder<K extends DHTBuilder<K>>
{
	public final static Number160 DEFAULT_DOMAIN = Number160.createHash("default-dht");
	protected final Peer peer;
	protected final Number160 locationKey;
	//
	protected Number160 domainKey;
	protected RoutingConfiguration routingConfiguration;
	protected RequestP2PConfiguration requestP2PConfiguration;
	protected FutureCreate<FutureDHT> futureCreate;
	protected FutureChannelCreator futureChannelCreator;
	protected FutureCreator<FutureDHT> defaultDirectReplication;
	//
	protected int refreshSeconds = 30;
	protected boolean protectDomain = false;
	protected boolean signMessage = false;
	protected boolean manualCleanup = false;
	protected boolean directReplication = false;
	
	private K self;
	
	public DHTBuilder(Peer peer, Number160 locationKey)
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

	public RequestP2PConfiguration getRequestP2PConfiguration()
	{
		return requestP2PConfiguration;
	}

	public K setRequestP2PConfiguration(RequestP2PConfiguration requestP2PConfiguration)
	{
		this.requestP2PConfiguration = requestP2PConfiguration;
		return self;
	}

	public FutureCreate<FutureDHT> getFutureCreate()
	{
		return futureCreate;
	}

	public K setFutureCreate(FutureCreate<FutureDHT> futureCreate)
	{
		this.futureCreate = futureCreate;
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

	public int getRefreshSeconds()
	{
		return refreshSeconds;
	}

	public K setRefreshSeconds(int refreshSeconds)
	{
		this.refreshSeconds = refreshSeconds;
		return self;
	}

	public FutureCreator<FutureDHT> getDefaultDirectReplication()
	{
		return defaultDirectReplication;
	}

	public K setDefaultDirectReplication(FutureCreator<FutureDHT> defaultDirectReplication)
	{
		this.defaultDirectReplication = defaultDirectReplication;
		return self;
	}

	public boolean isProtectDomain()
	{
		return protectDomain;
	}

	public K setProtectDomain(boolean protectDomain)
	{
		this.protectDomain = protectDomain;
		return self;
	}
	
	public K setProtectDomain()
	{
		this.protectDomain = true;
		return self;
	}

	public boolean isSignMessage()
	{
		return signMessage;
	}

	public K setSignMessage(boolean signMessage)
	{
		this.signMessage = signMessage;
		return self;
	}
	
	public K setSignMessage()
	{
		this.signMessage = true;
		return self;
	}

	public boolean isManualCleanup()
	{
		return manualCleanup;
	}

	public K setManualCleanup(boolean isManualCleanup)
	{
		this.manualCleanup = isManualCleanup;
		return self;
	}
	
	public K setManualCleanup()
	{
		this.manualCleanup = true;
		return self;
	}

	public boolean isDirectReplication()
	{
		return directReplication;
	}

	public K setDirectReplication(boolean directReplication)
	{
		this.directReplication = directReplication;
		return self;
	}
	
	public K setDirectReplication()
	{
		this.directReplication = true;
		return self;
	}
	
	protected void setupCancel(final FutureCleanup futureCleanup, final ScheduledFuture<?> future)
	{
		peer.getScheduledFutures().add(future);
		futureCleanup.addCleanup(new Cancellable()
		{
			@Override
			public void cancel()
			{
				future.cancel(true);
				peer.getScheduledFutures().remove(future);
			}
		});
	}
	
	protected void preBuild(String name)
	{
		if(domainKey == null)
		{
			domainKey = DEFAULT_DOMAIN;
		}
		if(routingConfiguration == null)
		{
			routingConfiguration = new RoutingConfiguration(5, 10, 2);
		}
		if(requestP2PConfiguration == null)
		{
			int size = peer.getPeerBean().getPeerMap().size() + 1;
			requestP2PConfiguration = new RequestP2PConfiguration(Math.min(size, 3), 5, 3);
		}
		if(futureChannelCreator == null)
		{
			futureChannelCreator = peer.reserve(routingConfiguration, requestP2PConfiguration, name);
		}
		if(refreshSeconds > 0)
		{
			directReplication = true;
		}
	}
	
	public abstract FutureDHT build();
}