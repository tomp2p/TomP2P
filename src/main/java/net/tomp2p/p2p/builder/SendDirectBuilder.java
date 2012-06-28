package net.tomp2p.p2p.builder;

import java.io.IOException;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RequestHandlerTCP;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class SendDirectBuilder
{
	final private static FutureResponse FUTURE_REQUEST_SHUTDOWN = new FutureResponse(null).setFailed("Peer is shutting down");
	final private Peer peer;
	private PeerAddress peerAddress;
	private ChannelBuffer buffer;
	private PeerConnection connection;
	private Object object;
	private FutureChannelCreator futureChannelCreator;
	public SendDirectBuilder(Peer peer)
	{
		this.peer = peer;
	}
	
	public PeerAddress getPeerAddress()
	{
		return peerAddress;
	}

	public SendDirectBuilder setPeerAddress(PeerAddress peerAddress)
	{
		this.peerAddress = peerAddress;
		return this;
	}

	public ChannelBuffer getBuffer()
	{
		return buffer;
	}

	public SendDirectBuilder setBuffer(ChannelBuffer buffer)
	{
		this.buffer = buffer;
		return this;
	}

	public PeerConnection getConnection()
	{
		return connection;
	}

	public SendDirectBuilder setConnection(PeerConnection connection)
	{
		this.connection = connection;
		return this;
	}

	public Object getObject()
	{
		return object;
	}

	public SendDirectBuilder setObject(Object object)
	{
		this.object = object;
		return this;
	}
	
	public FutureChannelCreator getFutureChannelCreator()
	{
		return futureChannelCreator;
	}

	public SendDirectBuilder setFutureChannelCreator(FutureChannelCreator futureChannelCreator)
	{
		this.futureChannelCreator = futureChannelCreator;
		return this;
	}
	
	public FutureResponse start()
	{
		if(peer.isShutdown())
		{
			return FUTURE_REQUEST_SHUTDOWN;
		}
		
		final boolean keepAlive;
		if(peerAddress != null && connection == null)
		{
			keepAlive = false;
		}
		else if(peerAddress == null && connection != null)
		{
			keepAlive = true;
		}
		else
		{
			throw new IllegalArgumentException("either remotePeer or connection has to be set");
		}
		final boolean raw;
		if(object != null && buffer == null)
		{
			byte[] me;
			try
			{
				me = Utils.encodeJavaObject(object);
			}
			catch (IOException e)
			{
				FutureResponse futureResponse = new FutureResponse(null);
				return futureResponse.setFailed("cannot serialize object: "+e);
			}
			buffer = ChannelBuffers.wrappedBuffer(me);
			raw = false;
		}
		else
		{
			raw = true;
		}
		if(buffer != null)
		{
			if(keepAlive)
			{
				return sendDirectAlive(raw);
			}
			else
			{
				if(futureChannelCreator == null)
				{
					futureChannelCreator = peer.getConnectionBean().getConnectionReservation().reserve(1, "send-direct-builder");
				}
				return sendDirectClose(raw);
			}
		}
		else
		{
			throw new IllegalArgumentException("either object or requestBuffer has to be set");
		}
	}

	private FutureResponse sendDirectAlive(boolean raw)
	{
		RequestHandlerTCP<FutureResponse> request = peer.getDirectDataRPC().prepareSend(connection.getDestination(),
				buffer.slice(), raw);
		request.setKeepAlive(true);
		// since we keep one connection open, we need to make sure that we do
		// not send anything in parallel.
		try
		{
			connection.aquireSingleConnection();
		}
		catch (InterruptedException e)
		{
			request.getFutureResponse().setFailed("Interupted " + e);
		}
		request.sendTCP(connection.getChannelCreator(), connection.getIdleTCPMillis());
		request.getFutureResponse().addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(FutureResponse future) throws Exception
			{
				connection.releaseSingleConnection();
			}
		});
		return request.getFutureResponse();
	}
	
	private FutureResponse sendDirectClose(final boolean raw)
	{
		final RequestHandlerTCP<FutureResponse> request = peer.getDirectDataRPC().prepareSend(peerAddress, buffer.slice(),
				raw);
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>()
		{
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception
			{
				if(future.isSuccess())
				{
					FutureResponse futureResponse = request.sendTCP(future.getChannelCreator());
					Utils.addReleaseListenerAll(futureResponse, peer.getConnectionBean().getConnectionReservation(), future.getChannelCreator());
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