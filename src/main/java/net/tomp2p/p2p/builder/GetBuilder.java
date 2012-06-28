package net.tomp2p.p2p.builder;
import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.connection.ConnectionHandler;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.message.MessageCodec;
import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;

public class GetBuilder extends DHTBuilder<GetBuilder>
{
	// if we don't provide any content key, the default is Number160.ZERO
	private final static Collection<Number160> NUMBER_ZERO_CONTENT_KEYS = new ArrayList<Number160>(1);
	private final static FutureDHT FUTURE_DHT_MESSAGE_TOO_LARGE = new FutureDHT().setFailed("Message size exceeds UDP transfer size, please consider using TCP");
	private Collection<Number160> contentKeys;
	private Number160 contentKey;
	private SimpleBloomFilter<Number160> keyBloomFilter;
	private SimpleBloomFilter<Number160> valueBloomFilter;
	private EvaluatingSchemeDHT evaluationScheme;
	//
	private boolean all = false;
	private boolean digest = false;
	private boolean returnBloomFilter = false;
	private boolean range = false;
	static
	{
		NUMBER_ZERO_CONTENT_KEYS.add(Number160.ZERO);
	}

	public GetBuilder(Peer peer, Number160 locationKey)
	{
		super(peer, locationKey);
		self(this);
	}

	public Collection<Number160> getContentKeys()
	{
		return contentKeys;
	}

	/**
	 * Set the content keys that should be found. Please note that if the
	 * content keys are too large, you may need to switch to TCP during routing.
	 * The default routing is UDP. Currently, the header is 59bytes, and the
	 * length of the content keys is as follows: 4 bytes for the length, 20bytes
	 * per content key. The user is warned if it will exceed the UDP size of
	 * 1400 (ConnectionHandler.UDP_LIMIT)
	 * 
	 * @param contentKeys
	 * @return
	 */
	public GetBuilder setContentKeys(Collection<Number160> contentKeys)
	{
		this.contentKeys = contentKeys;
		return this;
	}

	public Number160 getContentKey()
	{
		return contentKey;
	}

	public GetBuilder setContentKey(Number160 contentKey)
	{
		this.contentKey = contentKey;
		return this;
	}

	public SimpleBloomFilter<Number160> getKeyBloomFilter()
	{
		return keyBloomFilter;
	}

	public GetBuilder setKeyBloomFilter(SimpleBloomFilter<Number160> keyBloomFilter)
	{
		this.keyBloomFilter = keyBloomFilter;
		return this;
	}

	public SimpleBloomFilter<Number160> getValueBloomFilter()
	{
		return valueBloomFilter;
	}

	public GetBuilder setValueBloomFilter(SimpleBloomFilter<Number160> valueBloomFilter)
	{
		this.valueBloomFilter = valueBloomFilter;
		return this;
	}

	public EvaluatingSchemeDHT getEvaluationScheme()
	{
		return evaluationScheme;
	}

	public GetBuilder setEvaluationScheme(EvaluatingSchemeDHT evaluationScheme)
	{
		this.evaluationScheme = evaluationScheme;
		return this;
	}

	public boolean isAll()
	{
		return all;
	}

	public GetBuilder setAll(boolean all)
	{
		this.all = all;
		return this;
	}

	public GetBuilder setAll()
	{
		this.all = true;
		return this;
	}

	public boolean isDigest()
	{
		return digest;
	}

	public GetBuilder setDigest(boolean digest)
	{
		this.digest = digest;
		return this;
	}

	public GetBuilder setDigest()
	{
		this.digest = true;
		return this;
	}

	public boolean isReturnBloomFilter()
	{
		return returnBloomFilter;
	}

	public GetBuilder setReturnBloomFilter(boolean returnBloomFilter)
	{
		this.returnBloomFilter = returnBloomFilter;
		return this;
	}

	public GetBuilder setReturnBloomFilter()
	{
		this.returnBloomFilter = true;
		return this;
	}

	public boolean isRange()
	{
		return range;
	}

	public GetBuilder setRange(boolean range)
	{
		this.range = range;
		return this;
	}

	public GetBuilder setRange()
	{
		this.range = true;
		return this;
	}

	@Override
	public GetBuilder setRefreshSeconds(int refreshSeconds)
	{
		// this is not supported for get(), only for put(), add(), and remove()
		throw new UnsupportedOperationException("The get() does not have a refresh");
	}

	@Override
	public FutureDHT start()
	{
		if(peer.isShutdown())
		{
			return FUTURE_DHT_SHUTDOWN;
		}
		preBuild("get-builder");
		if (all)
		{
			contentKeys = null;
		}
		else if (contentKeys == null && !all)
		{
			// default is Number160.ZERO
			if (contentKey == null)
			{
				contentKeys = NUMBER_ZERO_CONTENT_KEYS;
			}
			else
			{
				contentKeys = new ArrayList<Number160>(1);
				contentKeys.add(contentKey);
			}
		}
		else
		{
			// check length of call, maybe we need to use TCP
			if (isMessageTooLargeForUDP() && !routingConfiguration.isForceTCP())
			{
				return FUTURE_DHT_MESSAGE_TOO_LARGE;
			}
		}
		if (evaluationScheme == null)
		{
			evaluationScheme = new VotingSchemeDHT();
		}
		return peer.getDistributedHashMap().get(locationKey, domainKey, contentKeys,
				keyBloomFilter, valueBloomFilter, routingConfiguration, requestP2PConfiguration,
				evaluationScheme, signMessage, digest, returnBloomFilter, range, manualCleanup,
				futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
	}

	/**
	 * Check if the message size exceeds the UDP size. In case it does, the
	 * future will fail and the user is notified. In this case, the user has to
	 * use this method to check if {@link RoutingConfiguration} needs to have
	 * the forceTCP flag.
	 * 
	 * @return True if the size is too large for UDP
	 */
	public boolean isMessageTooLargeForUDP()
	{
		return MessageCodec.HEADER_SIZE + 4 + (20 * contentKeys.size()) > ConnectionHandler.UDP_LIMIT;
	}
}
