package net.tomp2p.p2p.builder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureWrappedBootstrap;
import net.tomp2p.futures.FutureWrapper;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;


/**
 * Boostraps to a known peer. First channels are reserved, then
 * #discover(PeerAddress) is called to verify this Internet connection
 * settings using the argument peerAddress. Then the routing is initiated to
 * the peers specified in bootstrapTo. Please be aware that in order to
 * boostrap you need to know the peer ID of all peers in the collection
 * bootstrapTo. Passing Number160.ZERO does *not* work.
 * 
 * @param discoveryPeerAddress The peer address to use for discovery
 * @param bootstrapTo The peers used to bootstrap
 * @param config The configuration
 * @return The future bootstrap
 */

public class BootstrapBuilder 
{
	final private Peer peer;
	private Collection<PeerAddress> bootstrapTo;
	private PeerAddress peerAddress;
	private InetAddress inetAddress;
	private int portUDP = Bindings.DEFAULT_PORT;
	private int portTCP = Bindings.DEFAULT_PORT;
	private RoutingConfiguration routingConfiguration;
	private RequestP2PConfiguration requestP2PConfiguration;
	private boolean forceRoutingOnlyToSelf = false;
	private boolean broadcast = false;
	
	public BootstrapBuilder(Peer peer)
	{
		this.peer = peer;
	}
	
	public Collection<PeerAddress> getBootstrapTo()
	{
		return bootstrapTo;
	}

	public BootstrapBuilder setBootstrapTo(Collection<PeerAddress> bootstrapTo)
	{
		this.bootstrapTo = bootstrapTo;
		return this;
	}

	public PeerAddress getPeerAddress()
	{
		return peerAddress;
	}

	public BootstrapBuilder setPeerAddress(PeerAddress peerAddress)
	{
		this.peerAddress = peerAddress;
		return this;
	}

	public InetAddress getInetAddress()
	{
		return inetAddress;
	}

	public BootstrapBuilder setInetAddress(InetAddress inetAddress)
	{
		this.inetAddress = inetAddress;
		return this;
	}

	public int getPortUDP()
	{
		return portUDP;
	}

	public BootstrapBuilder setPortUDP(int portUDP)
	{
		this.portUDP = portUDP;
		return this;
	}

	public int getPortTCP()
	{
		return portTCP;
	}

	public BootstrapBuilder setPortTCP(int portTCP)
	{
		this.portTCP = portTCP;
		return this;
	}
	
	public BootstrapBuilder setPorts(int port)
	{
		this.portTCP = port;
		this.portUDP = port;
		return this;
	}

	public RoutingConfiguration getRoutingConfiguration()
	{
		return routingConfiguration;
	}

	public BootstrapBuilder setRoutingConfiguration(RoutingConfiguration routingConfiguration)
	{
		this.routingConfiguration = routingConfiguration;
		return this;
	}

	public RequestP2PConfiguration getRequestP2PConfiguration()
	{
		return requestP2PConfiguration;
	}

	public BootstrapBuilder setRequestP2PConfiguration(RequestP2PConfiguration requestP2PConfiguration)
	{
		this.requestP2PConfiguration = requestP2PConfiguration;
		return this;
	}

	public boolean isForceRoutingOnlyToSelf()
	{
		return forceRoutingOnlyToSelf;
	}
	
	public BootstrapBuilder setForceRoutingOnlyToSelf()
	{
		this.forceRoutingOnlyToSelf = true;
		return this;
	}

	public BootstrapBuilder setForceRoutingOnlyToSelf(boolean forceRoutingOnlyToSelf)
	{
		this.forceRoutingOnlyToSelf = forceRoutingOnlyToSelf;
		return this;
	}
	
	public boolean isBroadcast()
	{
		return broadcast;
	}
	
	public BootstrapBuilder setBroadcast()
	{
		this.broadcast = true;
		return this;
	}

	public BootstrapBuilder setBroadcast(boolean broadcast)
	{
		this.broadcast = broadcast;
		return this;
	}
	
	public FutureBootstrap build()
	{
		if(routingConfiguration == null)
		{
			routingConfiguration = new RoutingConfiguration(5, 10, 2);
		}
		if(requestP2PConfiguration == null)
		{
			int size = peer.getPeerBean().getPeerMap().size() + 1;
			requestP2PConfiguration = new RequestP2PConfiguration(Math.min(size, 3), 5, 3);
		}
		//
		if(broadcast)
		{
			return broadcast();
		}
		if(peerAddress == null && inetAddress != null && bootstrapTo == null)
		{
			peerAddress = new PeerAddress(Number160.ZERO, inetAddress, portTCP, portUDP);
			return bootstrapPing(peerAddress);
			
		}
		else if(peerAddress != null && bootstrapTo == null)
		{
			bootstrapTo = new ArrayList<PeerAddress>(1);
			bootstrapTo.add(peerAddress);
			return bootstrap();
		}
		else if(bootstrapTo !=null)
		{
			return bootstrap();
		}
		else
		{
			throw new IllegalArgumentException("need to set an address to bootstrap to");
		}
	}
	
	private FutureBootstrap bootstrap()
	{
		final FutureWrappedBootstrap<FutureWrapper<FutureRouting>> result = new FutureWrappedBootstrap<FutureWrapper<FutureRouting>>();
		result.setBootstrapTo(bootstrapTo);
		int conn = Math.max(routingConfiguration.getParallel(), requestP2PConfiguration.getParallel());
		peer.getConnectionBean().getConnectionReservation().reserve(conn).addListener(new BaseFutureAdapter<FutureChannelCreator>()
		{
			@Override
			public void operationComplete(final FutureChannelCreator futureChannelCreator) throws Exception
			{
				if(futureChannelCreator.isSuccess())
				{
					FutureWrapper<FutureRouting> futureBootstrap = peer.getDistributedRouting().bootstrap(bootstrapTo, 
							routingConfiguration.getMaxNoNewInfo(requestP2PConfiguration.getMinimumResults()),
							routingConfiguration.getMaxFailures(), routingConfiguration.getMaxSuccess(),
							routingConfiguration.getParallel(), false, 
							forceRoutingOnlyToSelf, futureChannelCreator.getChannelCreator());
					Utils.addReleaseListenerAll(futureBootstrap, peer.getConnectionBean().getConnectionReservation(), 
							futureChannelCreator.getChannelCreator());
					result.waitFor(futureBootstrap);
				}
				else
				{
					result.setFailed(futureChannelCreator);
				}
			}
		});
		return result;
	}
	
	private FutureWrappedBootstrap<FutureBootstrap> bootstrapPing(PeerAddress address)
	{
		final FutureWrappedBootstrap<FutureBootstrap> result = new FutureWrappedBootstrap<FutureBootstrap>();
		final FutureResponse tmp = (FutureResponse) peer.ping().setPeerAddress(address).setTcpPing().build();
		tmp.addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(final FutureResponse future) throws Exception
			{
				if (future.isSuccess())
				{
					peerAddress = future.getResponse().getSender();
					bootstrapTo = new ArrayList<PeerAddress>(1);
					bootstrapTo.add(peerAddress);
					result.setBootstrapTo(bootstrapTo);
					result.waitFor(bootstrap());
				}
				else
				{
					result.setFailed("could not reach anyone with bootstrap");
				}
			}
		});
		return result;
	}
	
	private FutureWrappedBootstrap<FutureBootstrap> broadcast()
	{
		final FutureWrappedBootstrap<FutureBootstrap> result = new FutureWrappedBootstrap<FutureBootstrap>();
		// limit after
		@SuppressWarnings("unchecked")
		final FutureLateJoin<FutureResponse> tmp = (FutureLateJoin<FutureResponse>) peer.ping().setBroadcast().setPort(portUDP).build();
		tmp.addListener(new BaseFutureAdapter<FutureLateJoin<FutureResponse>>()
		{
			@Override
			public void operationComplete(final FutureLateJoin<FutureResponse> future)
					throws Exception
			{
				if (future.isSuccess())
				{
					FutureResponse futureResponse = future.getLastSuceessFuture();
					if(futureResponse==null)
					{
						result.setFailed("no futures found", future);
						return;
					}
					peerAddress = futureResponse.getResponse().getSender();
					Collection<PeerAddress> bootstrapTo = new ArrayList<PeerAddress>(1);
					bootstrapTo.add(peerAddress);
					result.setBootstrapTo(bootstrapTo);
					result.waitFor(bootstrap());
				}
				else
				{
					result.setFailed("could not reach anyone with the broadcast", future);
				}
			}
		});
		return result;
	}	
}
