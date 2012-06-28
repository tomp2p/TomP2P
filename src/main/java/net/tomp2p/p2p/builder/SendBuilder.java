package net.tomp2p.p2p.builder;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class SendBuilder  extends DHTBuilder<SendBuilder>
{
	private ChannelBuffer buffer;
	private Object object;
	//
	private boolean cancelOnFinish = false;
	private boolean raw = false;
	private int repetitions = 5;
	public SendBuilder(Peer peer, Number160 locationKey)
	{
		super(peer, locationKey);
		self(this);
	}
	
	public ChannelBuffer getBuffer()
	{
		return buffer;
	}

	public SendBuilder setBuffer(ChannelBuffer buffer)
	{
		this.buffer = buffer;
		return this;
	}

	public Object getObject()
	{
		return object;
	}

	public SendBuilder setObject(Object object)
	{
		this.object = object;
		return this;
	}
	
	public int getRepetitions()
	{
		return repetitions;
	}

	public SendBuilder setRepetitions(int repetitions)
	{
		this.repetitions = repetitions;
		return this;
	}
	
	public boolean isCancelOnFinish()
	{
		return cancelOnFinish;
	}

	public SendBuilder setCancelOnFinish(boolean cancelOnFinish)
	{
		this.cancelOnFinish = cancelOnFinish;
		return this;
	}
	
	public SendBuilder setCancelOnFinish()
	{
		this.cancelOnFinish = true;
		return this;
	}
	
	@Override
	public FutureDHT start()
	{
		if(peer.isShutdown())
		{
			return FUTURE_DHT_SHUTDOWN;
		}
		preBuild("send-builder");
		if(buffer == null && object != null)
		{
			raw = false;
			byte[] me;
			try
			{
				me = Utils.encodeJavaObject(object);
			}
			catch (IOException e)
			{
				FutureDHT futureDHT = new FutureDHT();
				return futureDHT.setFailed("problems with encoding the object "+ e);
			}
			buffer = ChannelBuffers.wrappedBuffer(me);
		}
		else if(buffer !=null && object == null)
		{
			raw = true;
		}
		else
		{
			throw new IllegalArgumentException("either buffer has to be set or object.");
		}
		final FutureDHT futureDHT = peer.getDistributedHashMap().direct(locationKey, buffer, raw,
				routingConfiguration, requestP2PConfiguration, futureCreate,
				isCancelOnFinish(), manualCleanup, futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
		if(directReplication)
		{
			if(defaultDirectReplication == null)
			{
				defaultDirectReplication = new DefaultDirectReplication();
			}
			Runnable runner = new Runnable()
			{
				private int counter = 0;
				@Override
				public void run()
				{
					if(counter < repetitions)
					{
						FutureDHT futureDHTReplication = defaultDirectReplication.create();
						futureDHT.repeated(futureDHTReplication);
						counter++;
						ScheduledFuture<?> tmp = peer.getConnectionBean().getScheduler().getScheduledExecutorServiceReplication().schedule(
								this, refreshSeconds, TimeUnit.SECONDS);
						setupCancel(futureDHT, tmp);
					}
				}
			};
			ScheduledFuture<?> tmp = peer.getConnectionBean().getScheduler().getScheduledExecutorServiceReplication().schedule(
					runner, refreshSeconds, TimeUnit.SECONDS);
			setupCancel(futureDHT, tmp);
		}
		return futureDHT;
	}
	
	private class DefaultDirectReplication implements FutureCreator<FutureDHT>
	{
		@Override
		public FutureDHT create()
		{
			final FutureChannelCreator futureChannelCreator = peer.reserve(routingConfiguration, requestP2PConfiguration, "send-builder-direct-replication");
			final FutureDHT futureDHT = peer.getDistributedHashMap().direct(locationKey, buffer, raw,
					routingConfiguration, requestP2PConfiguration, futureCreate,
					isCancelOnFinish(), manualCleanup, futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
			return futureDHT;
			}	
		}
}
