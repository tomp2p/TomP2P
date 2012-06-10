package net.tomp2p.p2p.builder;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.rpc.RequestHandlerUDP;
import net.tomp2p.utils.Utils;

public class PingBuilder
{
	final private Peer peer;
	private PeerAddress peerAddress;
	private InetAddress inetAddress;
	private int port = Bindings.DEFAULT_PORT;
	private boolean broadcast = false;
	private boolean tcpPing = false;
	public PingBuilder(Peer peer)
	{
		this.peer = peer;
	}
	
	public PeerAddress getPeerAddress()
	{
		return peerAddress;
	}

	public PingBuilder setPeerAddress(PeerAddress peerAddress)
	{
		this.peerAddress = peerAddress;
		return this;
	}

	public InetAddress getInetAddress()
	{
		return inetAddress;
	}

	public PingBuilder setInetAddress(InetAddress inetAddress)
	{
		this.inetAddress = inetAddress;
		return this;
	}

	public int getPort()
	{
		return port;
	}

	public PingBuilder setPort(int port)
	{
		this.port = port;
		return this;
	}

	public boolean isBroadcast()
	{
		return broadcast;
	}
	
	public PingBuilder setBroadcast()
	{
		this.broadcast = true;
		return this;
	}

	public PingBuilder setBroadcast(boolean broadcast)
	{
		this.broadcast = broadcast;
		return this;
	}

	public boolean isTcpPing()
	{
		return tcpPing;
	}
	
	public PingBuilder setTcpPing()
	{
		this.tcpPing = true;
		return this;
	}

	public PingBuilder setTcpPing(boolean tcpPing)
	{
		this.tcpPing = tcpPing;
		return this;
	}
	
	public BaseFuture build()
	{
		if(broadcast)
		{
			return pingBroadcast(port);
		}
		else
		{
			if(peerAddress != null)
			{
				if(tcpPing)
				{
					return ping(peerAddress.createSocketTCP(), true);
				}
				else
				{
					return ping(peerAddress.createSocketUDP(), false);
				}
			}
			else if(inetAddress!=null)
			{
				if(tcpPing)
				{
					return ping(new InetSocketAddress(inetAddress, port), true);
				}
				else
				{
					return ping(new InetSocketAddress(inetAddress, port), false);
				}
			}
			else
			{
				throw new IllegalArgumentException("cannot ping, need to know peer address or inet address");
			}
		}
	}
	
	FutureLateJoin<FutureResponse> pingBroadcast(final int port)
	{
		final int size = peer.getBindings().getBroadcastAddresses().size();
		final FutureLateJoin<FutureResponse> futureLateJoin = new FutureLateJoin<FutureResponse>(size, 1);
		if (size > 0)
		{
			peer.getConnectionBean().getConnectionReservation().reserve(size).addListener(new BaseFutureAdapter<FutureChannelCreator>()
			{
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception
				{
					if(future.isSuccess())
					{
						for (int i = 0; i < size ; i++)
						{
							final InetAddress broadcastAddress = peer.getBindings().getBroadcastAddresses().get(i);
							final PeerAddress peerAddress = new PeerAddress(Number160.ZERO, broadcastAddress,
									port, port);
							FutureResponse validBroadcast = peer.getHandshakeRPC().pingBroadcastUDP(peerAddress, future.getChannelCreator());
							Utils.addReleaseListener(validBroadcast, peer.getConnectionBean().getConnectionReservation(), future.getChannelCreator(), 1);
							if(!futureLateJoin.add(validBroadcast))
							{
								//the late join future is fininshed if the add returns false
								break;
							}
						}
					}
					else
					{
						futureLateJoin.setFailed(future);
					}				
				}
			});
		}
		else
		{
			futureLateJoin.setFailed("No broadcast address found. Cannot ping nothing");
		}
		return futureLateJoin;
	}
	
	/**
	 * Pings a peer. Default is to use UDP
	 * 
	 * @param address The address of the remote peer.
	 * @return The future response
	 */
	public FutureResponse ping(final InetSocketAddress address)
	{
		return ping(address, true);
	}

	/**
	 * Pings a peer.
	 * 
	 * @param address The address of the remote peer.
	 * @param isUDP Set to true if UDP should be used, false for TCP.
	 * @return The future response
	 */
	public FutureResponse ping(final InetSocketAddress address, boolean isUDP)
	{
		if (isUDP)
		{
			final RequestHandlerUDP<FutureResponse> request = peer.getHandshakeRPC().pingUDP(new PeerAddress(Number160.ZERO, address));
			peer.getConnectionBean().getConnectionReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
			{
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception
				{
					if (future.isSuccess())
					{
						FutureResponse futureResponse = request.sendUDP(future.getChannelCreator());
						Utils.addReleaseListener(futureResponse, peer.getConnectionBean().getConnectionReservation(), future.getChannelCreator(), 1);
					}
					else
					{
						request.getFutureResponse().setFailed(future);
					}
				}
			});
			return request.getFutureResponse();
		}
		else
		{
			final RequestHandlerTCP<FutureResponse> request = peer.getHandshakeRPC().pingTCP(new PeerAddress(Number160.ZERO, address));
			peer.getConnectionBean().getConnectionReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
			{
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception
				{
					if (future.isSuccess())
					{
						FutureResponse futureResponse = request.sendTCP(future.getChannelCreator());
						Utils.addReleaseListener(futureResponse, peer.getConnectionBean().getConnectionReservation(), future.getChannelCreator(), 1);
					}
					else
					{
						request.getFutureResponse().setFailed(future);
					}
				}
			});
			return request.getFutureResponse();
		}
	}	
}
