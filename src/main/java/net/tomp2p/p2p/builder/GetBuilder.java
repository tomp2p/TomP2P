package net.tomp2p.p2p.builder;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;

public class GetBuilder extends DHTBuilder<GetBuilder>
{
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
	
	public GetBuilder(Peer peer, Number160 locationKey)
	{
		super(peer, locationKey);
		self(this);
	}
	
	public Collection<Number160> getContentKeys()
	{
		return contentKeys;
	}

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
		//this is not supported for get(), only for put(), add(), and remove()
		throw new UnsupportedOperationException("The get() does not have a refresh");
	}
	
	@Override
	public FutureDHT build()
	{
		preBuild("get-builder");
		if(all)
		{
			contentKeys = null;
		}
		else if(contentKeys == null && !all)
		{
			contentKeys = new ArrayList<Number160>(1);
			if(contentKey == null)
			{
				contentKey = Number160.ZERO;
			}
			contentKeys.add(contentKey);
		}
		return peer.getDistributedHashMap().get(locationKey, domainKey, 
				contentKeys, keyBloomFilter, valueBloomFilter, routingConfiguration, requestP2PConfiguration, 
				evaluationScheme, signMessage, digest, returnBloomFilter, range, manualCleanup, futureChannelCreator, 
				peer.getConnectionBean().getConnectionReservation());
	}
}
