/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.p2p;
public class RoutingConfiguration
{
	final private int directHits;
	final private int maxNoNewInfoDiff;
	final private int maxFailures;
	final private int maxSuccess;
	final private int parallel;
	
	public RoutingConfiguration(int maxNoNewInfoDiff, int maxFailures, int parallel)
	{
		this(Integer.MAX_VALUE, maxNoNewInfoDiff, maxFailures, 20, parallel);
	}

	public RoutingConfiguration(int maxNoNewInfoDiff, int maxFailures, int maxSuccess, int parallel)
	{
		this(Integer.MAX_VALUE, maxNoNewInfoDiff, maxFailures, maxSuccess, parallel);
	}

	public RoutingConfiguration(int directHits, int maxNoNewInfoDiff, int maxFailures, int maxSuccess, int parallel)
	{
		if (directHits < 0 || maxNoNewInfoDiff < 0 || maxFailures < 0 || parallel < 0)
			throw new IllegalArgumentException("need to be larger or equals zero");
		this.directHits = directHits;
		this.maxNoNewInfoDiff = maxNoNewInfoDiff;
		this.maxFailures = maxFailures;
		this.maxSuccess = maxSuccess;
		this.parallel = parallel;
	}

	public int getDirectHits()
	{
		return directHits;
	}

	/**
	 * This returns the difference to the min value of P2P configuration. We
	 * need to have a difference, because we need to search at least for min
	 * peers in the routing, as otherwise if we find the closest node by chance,
	 * then we don't reach min.
	 * 
	 * @return
	 */
	public int getMaxNoNewInfoDiff()
	{
		return maxNoNewInfoDiff;
	}

	public int getMaxNoNewInfo(int minimumResults)
	{
		return maxNoNewInfoDiff + minimumResults;
	}

	public int getMaxFailures()
	{
		return maxFailures;
	}
	
	public int getMaxSuccess()
	{
		return maxSuccess;
	}

	public int getParallel()
	{
		return parallel;
	}
}
