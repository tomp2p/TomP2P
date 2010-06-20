package net.tomp2p.p2p.config;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;

public class ConfigurationBase
{
	private Number160 contentKey;
	private Number160 domain;
	private RoutingConfiguration routingConfiguration;
	private boolean signMessage;

	public ConfigurationBase setContentKey(Number160 contentKey)
	{
		this.contentKey = contentKey;
		return this;
	}

	public Number160 getContentKey()
	{
		return contentKey;
	}

	public ConfigurationBase setDomain(Number160 domain)
	{
		this.domain = domain;
		return this;
	}

	public Number160 getDomain()
	{
		return domain;
	}

	public ConfigurationBase setRoutingConfiguration(RoutingConfiguration routingConfiguration)
	{
		this.routingConfiguration = routingConfiguration;
		return this;
	}

	public RoutingConfiguration getRoutingConfiguration()
	{
		return routingConfiguration;
	}

	public ConfigurationBase setSignMessage(boolean signMessage)
	{
		this.signMessage = signMessage;
		return this;
	}

	public boolean isSignMessage()
	{
		return signMessage;
	}
}
