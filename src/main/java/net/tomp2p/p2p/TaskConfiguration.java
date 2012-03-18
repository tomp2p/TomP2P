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
/**
 * This name was chosen over P2PConfiguration, as it already exists
 * @author draft
 *
 */
public class TaskConfiguration
{
	final private int minimumResults;
	final private int maxFailure;
	final private int parallelDiff;
	//set to force either UDP or TCP
	final private boolean forceUPD;
	final private boolean forceTCP;
	final private boolean sign;
	
	public TaskConfiguration(int minimumResults, int maxFailure, int parallelDiff)
	{
		this(minimumResults, maxFailure, parallelDiff, false, false, false);
	}

	public TaskConfiguration(int minimumResults, int maxFailure, int parallelDiff, boolean forceUPD, boolean forceTCP, boolean sign)
	{
		if (minimumResults < 0 || maxFailure < 0 || parallelDiff < 0)
			throw new IllegalArgumentException("need to be larger or equals zero");
		this.minimumResults = minimumResults;
		this.maxFailure = maxFailure;
		this.parallelDiff = parallelDiff;
		this.forceUPD = forceUPD;
		this.forceTCP = forceTCP;
		this.sign = sign;
	}

	public int getMinimumResults()
	{
		return minimumResults;
	}

	public int getMaxFailure()
	{
		return maxFailure;
	}

	public int getParallelDiff()
	{
		return parallelDiff;
	}
	
	public int getParallel()
	{
		return minimumResults+parallelDiff;
	}

	public boolean isForceUPD()
	{
		return forceUPD;
	}

	public boolean isForceTCP()
	{
		return forceTCP;
	}

	public boolean isSign()
	{
		return sign;
	}
}