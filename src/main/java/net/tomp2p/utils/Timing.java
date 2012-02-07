/*
 * Copyright 2012 Thomas Bocek
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
package net.tomp2p.utils;
/**
 * The timing class for TomP2P. This class was introduced since SimGrid uses
 * different timings. While the normal operation uses System.currentTimeMillis
 * and Thread.sleep, the SimGrid integration uses other implementations.
 * 
 * @author Thomas Bocek
 * 
 */
public interface Timing
{
	/**
	 * @return the time in millis for this system
	 */
	public abstract long currentTimeMillis();

	/**
	 * Sleeps with throwing an InterruptedException
	 * 
	 * @param millis The time to sleep in milliseconds
	 * @throws InterruptedException If interrputed is called
	 */
	public abstract void sleep(int millis) throws InterruptedException;

	/**
	 * Sleeps without throwing an InterruptedException
	 * 
	 * @param millis The time to sleep in milliseconds
	 */
	public abstract void sleepUninterruptibly(int millis);
}