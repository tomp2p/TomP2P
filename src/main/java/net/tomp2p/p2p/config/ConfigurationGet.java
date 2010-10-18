package net.tomp2p.p2p.config;
import java.security.PublicKey;

import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.p2p.RequestP2PConfiguration;


public class ConfigurationGet extends ConfigurationBase
{
	private EvaluatingSchemeDHT evaluationScheme;
	private RequestP2PConfiguration requestP2PConfiguration;
	private PublicKey publicKey;

	public ConfigurationGet setEvaluationScheme(EvaluatingSchemeDHT evaluationScheme)
	{
		this.evaluationScheme = evaluationScheme;
		return this;
	}

	public EvaluatingSchemeDHT getEvaluationScheme()
	{
		return evaluationScheme;
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

	public ConfigurationGet setPublicKey(PublicKey publicKey)
	{
		this.publicKey = publicKey;
		return this;
	}

	public PublicKey getPublicKey()
	{
		return publicKey;
	}
}
