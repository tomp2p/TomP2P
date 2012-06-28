package net.tomp2p.p2p.builder;

import java.net.InetAddress;
import java.util.Collection;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerListener;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoverBuilder
{
	final private static Logger logger = LoggerFactory.getLogger(DiscoverBuilder.class);
	final private static FutureDiscover FUTURE_DISCOVER_SHUTDOWN = new FutureDiscover().setFailed("Peer is shutting down");
	final private Peer peer;
	private InetAddress inetAddress;
	private int portUDP = Bindings.DEFAULT_PORT;
	private int portTCP = Bindings.DEFAULT_PORT;
	private PeerAddress peerAddress;
	private int discoverTimeoutSec = 5;
	public DiscoverBuilder(Peer peer)
	{
		this.peer = peer;
	}
	
	public InetAddress getInetAddress()
	{
		return inetAddress;
	}

	public DiscoverBuilder setInetAddress(InetAddress inetAddress)
	{
		this.inetAddress = inetAddress;
		return this;
	}

	public int getPortUDP()
	{
		return portUDP;
	}

	public DiscoverBuilder setPortUDP(int portUDP)
	{
		this.portUDP = portUDP;
		return this;
	}

	public int getPortTCP()
	{
		return portTCP;
	}

	public DiscoverBuilder setPortTCP(int portTCP)
	{
		this.portTCP = portTCP;
		return this;
	}
	
	public DiscoverBuilder setPorts(int port)
	{
		this.portTCP = port;
		this.portUDP = port;
		return this;
	}

	public PeerAddress getPeerAddress()
	{
		return peerAddress;
	}

	public DiscoverBuilder setPeerAddress(PeerAddress peerAddress)
	{
		this.peerAddress = peerAddress;
		return this;
	}

	public int getDiscoverTimeoutSec()
	{
		return discoverTimeoutSec;
	}

	public DiscoverBuilder setDiscoverTimeoutSec(int discoverTimeoutSec)
	{
		this.discoverTimeoutSec = discoverTimeoutSec;
		return this;
	}
	
	public FutureDiscover start()
	{
		if(peer.isShutdown())
		{
			return FUTURE_DISCOVER_SHUTDOWN;
		}
		
		if(peerAddress == null && inetAddress!=null)
		{
			peerAddress = new PeerAddress(Number160.ZERO, inetAddress, portTCP, portUDP);
		}
		if(peerAddress == null)
		{
			throw new IllegalArgumentException("need peeraddress or inetaddress");
		}
		return discover(peerAddress);
	}
	
	/**
	 * Discover attempts to find the external IP address of this peer. This is
	 * done by first trying to set UPNP with port forwarding (gives us the
	 * external address), query UPNP for the external address, and pinging a
	 * well known peer. The fallback is NAT-PMP.
	 * 
	 * @param peerAddress The peer address. Since pings are used the peer ID can
	 *        be Number160.ZERO
	 * @return The future discover. This future holds also the real ID of the
	 *         peer we send the discover request
	 */
	private FutureDiscover discover(final PeerAddress peerAddress)
	{
		final FutureDiscover futureDiscover = new FutureDiscover();
		peer.getConnectionBean().getConnectionReservation().reserve(3).addListener(new BaseFutureAdapter<FutureChannelCreator>()
		{
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception
			{
				if(future.isSuccess())
				{
					discover(futureDiscover, peerAddress, future.getChannelCreator());
				}
				else
				{
					futureDiscover.setFailed(future);
				}
			}
		});
		return futureDiscover;
	}
	
	/**
	 * Needs 3 connections. Cleans up ChannelCreator, which means they will be released.
	 * 
	 * @param peerAddress
	 * @param cc
	 * @return
	 */
	private void discover(final FutureDiscover futureDiscover, final PeerAddress peerAddress, final ChannelCreator cc)
	{
		final FutureResponse futureResponseTCP = peer.getHandshakeRPC().pingTCPDiscover(peerAddress, cc);
		Utils.addReleaseListener(futureResponseTCP, peer.getConnectionBean().getConnectionReservation(), cc, 1);
		peer.addPeerListener(new PeerListener()
		{
			private boolean changedUDP = false;
			private boolean changedTCP = false;

			@Override
			public void serverAddressChanged(PeerAddress peerAddress, PeerAddress reporter, boolean tcp)
			{
				if (tcp)
				{
					changedTCP = true;
					futureDiscover.setDiscoveredTCP();
				}
				else
				{
					changedUDP = true;
					futureDiscover.setDiscoveredUDP();
				}
				if (changedTCP && changedUDP)
				{
					futureDiscover.done(peerAddress, reporter);
				}
			}

			@Override
			public void notifyOnStart()
			{}

			@Override
			public void notifyOnShutdown()
			{}
		});
		futureResponseTCP.addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(FutureResponse future) throws Exception
			{
				PeerAddress serverAddress = peer.getPeerBean().getServerPeerAddress();
				if (futureResponseTCP.isSuccess())
				{
					Collection<PeerAddress> tmp = futureResponseTCP.getResponse().getNeighbors();
					if (tmp.size() == 1)
					{
						PeerAddress seenAs = tmp.iterator().next();
						logger.info("I'm seen as " + seenAs + " by peer " + peerAddress+" I see myself as "+peer.getPeerAddress().getInetAddress());
						if (!peer.getPeerAddress().getInetAddress().equals(seenAs.getInetAddress()))
						{
							// check if we have this interface in that we can listen to
							Bindings bindings2 = new Bindings(seenAs.getInetAddress());
							String status = DiscoverNetworks.discoverInterfaces(bindings2);
							logger.info("2nd interface discovery: "+status);
							if (bindings2.getFoundAddresses().size() > 0 &&  bindings2.getFoundAddresses().contains(seenAs.getInetAddress()))
							{
								serverAddress = serverAddress.changeAddress(seenAs.getInetAddress());
								peer.getPeerBean().setServerPeerAddress(serverAddress);
							}
							else
							{
								// now we know our internal IP, where we receive packets
								if (peer.getBindings().isSetExternalPortsManually() || 
										peer.setupPortForwanding(futureResponseTCP.getResponse().getRecipient().getInetAddress().getHostAddress()))
								{
									serverAddress = serverAddress.changePorts(peer.getBindings().getOutsideUDPPort(),
											peer.getBindings().getOutsideTCPPort());
									serverAddress = serverAddress.changeAddress(seenAs.getInetAddress());
									peer.getPeerBean().setServerPeerAddress(serverAddress);
								}
							}
						}
						// else -> we announce exactly how the other peer sees us
						FutureResponse fr1 = peer.getHandshakeRPC().pingTCPProbe(peerAddress, cc);
						FutureResponse fr2 = peer.getHandshakeRPC().pingUDPProbe(peerAddress, cc);
						Utils.addReleaseListener(fr1, peer.getConnectionBean().getConnectionReservation(), cc, 1);
						Utils.addReleaseListener(fr2, peer.getConnectionBean().getConnectionReservation(), cc, 1);
						// from here we probe, set the timeout here
						futureDiscover.setTimeout(peer.getTimer(), discoverTimeoutSec);
						return;
					}
					else
					{
						// important to release connection if not needed
						peer.getConnectionBean().getConnectionReservation().release(cc, 2);
						//System.err.println("release 2"+cc);
						futureDiscover.setFailed("Peer " + peerAddress + " did not report our IP address");
						return;
					}
				}
				else
				{
					// important to release connection if not needed
					peer.getConnectionBean().getConnectionReservation().release(cc, 2);
					//System.err.println("release 2"+cc);
					futureDiscover.setFailed("FutureDiscover: We need at least the TCP connection", futureResponseTCP);
					return;
				}
			}
		});
	}
}
