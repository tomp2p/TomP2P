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
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;

public class ConfigurationRemove extends ConfigurationBaseDHT
{
	private boolean returnResults;
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
