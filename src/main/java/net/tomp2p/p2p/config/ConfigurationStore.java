package net.tomp2p.p2p.config;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;

public class ConfigurationStore extends ConfigurationBase
{
	private boolean absent;
	private RequestP2PConfiguration requestP2PConfiguration;
	private boolean protectDomain;
	private int refreshSeconds;
	private FutureCreate<FutureDHT> futureCreate;

	public boolean isStoreIfAbsent()
	{
		return absent;
	}

	public ConfigurationStore setStoreIfAbsent(boolean absent)
	{
		this.absent = absent;
		return this;
	}

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

	public ConfigurationBase setProtectDomain(boolean protectDomain)
	{
		this.protectDomain = protectDomain;
		return this;
	}

	public boolean isProtectDomain()
	{
		return protectDomain;
	}

	public ConfigurationStore setRefreshSeconds(int refreshSeconds)
	{
		this.refreshSeconds = refreshSeconds;
		return this;
	}

	public int getRefreshSeconds()
	{
		return refreshSeconds;
	}

	public ConfigurationStore setFutureCreate(FutureCreate<FutureDHT> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}

	public FutureCreate<FutureDHT> getFutureCreate()
	{
		return futureCreate;
	}
}
