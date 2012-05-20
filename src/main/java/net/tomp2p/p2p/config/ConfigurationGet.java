/*
 * Copyright 2011 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.p2p.config;
import java.security.PublicKey;

import net.tomp2p.p2p.EvaluatingSchemeDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;


public class ConfigurationGet extends ConfigurationBaseDHT
{
	private EvaluatingSchemeDHT evaluationScheme;
	private PublicKey publicKey;
	private SimpleBloomFilter<Number160> keyBloomFilter;
	private SimpleBloomFilter<Number160> contentBloomFilter;
	private boolean returnBloomFliter;
	private boolean range;

	public ConfigurationGet setEvaluationScheme(EvaluatingSchemeDHT evaluationScheme)
	{
		this.evaluationScheme = evaluationScheme;
		return this;
	}

	public EvaluatingSchemeDHT getEvaluationScheme()
	{
		return evaluationScheme;
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
	
	public ConfigurationGet setKeyBloomFilter(SimpleBloomFilter<Number160> keyBloomFilter)
	{
		this.keyBloomFilter = keyBloomFilter;
		return this;
	}
	
	public SimpleBloomFilter<Number160> getKeyBloomFilter()
	{
		return keyBloomFilter;
	}
	
	public ConfigurationGet setContentBloomFilter(SimpleBloomFilter<Number160> contentBloomFilter)
	{
		this.contentBloomFilter = contentBloomFilter;
		return this;
	}
	
	public SimpleBloomFilter<Number160> getContentBloomFilter()
	{
		return contentBloomFilter;
	}
	
	public ConfigurationGet setReturnBloomFliter(boolean returnBloomFliter)
	{
		this.returnBloomFliter = returnBloomFliter;
		return this;
	}

	public boolean isReturnBloomFliter()
	{
		return returnBloomFliter;
	}
	
	public ConfigurationGet setRange(boolean range)
	{
		this.range = range;
		return this;
	}

	public boolean isRange()
	{
		return range;
	}
}
