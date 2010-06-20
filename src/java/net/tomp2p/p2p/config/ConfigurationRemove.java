package net.tomp2p.p2p.config;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;

public class ConfigurationRemove extends ConfigurationBase
{
	private boolean returnResults;
	private RequestP2PConfiguration requestP2PConfiguration;
	private int repetitions;
	private int refreshSeconds;
	private FutureCreate<FutureDHT> futureCreate;

	public ConfigurationRemove setReturnResults(boolean returnResults)
	{
		this.returnResults = returnResults;
		return this;
	}

	public boolean isReturnResults()
	{
		return returnResults;
	}

	public ConfigurationRemove setRequestP2PConfiguration(
			RequestP2PConfiguration requestP2PConfiguration)
	{
		this.requestP2PConfiguration = requestP2PConfiguration;
		return this;
	}

	public RequestP2PConfiguration getRequestP2PConfiguration()
	{
		return requestP2PConfiguration;
	}

	public ConfigurationRemove setRepetitions(int repetitions)
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

	public ConfigurationRemove setRefreshSeconds(int refreshSeconds)
	{
		this.refreshSeconds = refreshSeconds;
		return this;
	}

	public int getRefreshSeconds()
	{
		return refreshSeconds;
	}

	public ConfigurationRemove setFutureCreate(FutureCreate<FutureDHT> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}

	public FutureCreate<FutureDHT> getFutureCreate()
	{
		return futureCreate;
	}
}
