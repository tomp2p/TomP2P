package net.tomp2p.p2p.config;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.peers.Number160;

public class ConfigurationDirect extends ConfigurationBase
{
	private RequestP2PConfiguration requestP2PConfiguration;
	private int refreshSeconds;
	private FutureCreate<FutureDHT> futureCreate;
	private boolean cancelOnFinish;
	private int repetitions;

	public ConfigurationBase setRequestP2PConfiguration(
			RequestP2PConfiguration requestP2PConfiguration)
	{
		this.requestP2PConfiguration = requestP2PConfiguration;
		return this;
	}

	public RequestP2PConfiguration getRequestP2PConfiguration()
	{
		return requestP2PConfiguration;
	}

	public ConfigurationDirect setRefreshSeconds(int refreshSeconds)
	{
		this.refreshSeconds = refreshSeconds;
		return this;
	}

	public int getRefreshSeconds()
	{
		return refreshSeconds;
	}

	public ConfigurationDirect setFutureCreate(FutureCreate<FutureDHT> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}

	public FutureCreate<FutureDHT> getFutureCreate()
	{
		return futureCreate;
	}

	@Override
	public ConfigurationBase setContentKey(Number160 contentKey)
	{
		throw new UnsupportedOperationException("the direct command cannot set its own content key");
	}

	@Override
	public Number160 getContentKey()
	{
		throw new UnsupportedOperationException("the direct command cannot set its own content key");
	}

	@Override
	public ConfigurationBase setDomain(Number160 domain)
	{
		throw new UnsupportedOperationException("the  direct command cannot set its own domain key");
	}

	@Override
	public Number160 getDomain()
	{
		throw new UnsupportedOperationException("the  direct command cannot set its own domain key");
	}

	public ConfigurationDirect setRepetitions(int repetitions)
	{
		if (repetitions < 0)
			throw new RuntimeException("repetitions cannot be negative");
		this.repetitions = repetitions;
		return this;
	}

	public int getRepetitions()
	{
		return repetitions;
	}

	public ConfigurationDirect setCancelOnFinish(boolean cancelOnFinish)
	{
		this.cancelOnFinish = cancelOnFinish;
		return this;
	}

	public boolean isCancelOnFinish()
	{
		return cancelOnFinish;
	}
}
