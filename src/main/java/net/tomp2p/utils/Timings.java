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
 * Static interface for the Timing framework
 * 
 * @author Thomas Bocek
 *
 */
public class Timings
{
	private static Timing impl = new TimingImpl();
	public static void setImpl(Timing impl2)
	{
		//TODO: use better way of integration, e.g. with -Dtiming=net.tomp2p.bla
		impl = impl2;
	}
	public static long currentTimeMillis()
	{
		return impl.currentTimeMillis();
	}
	public static void sleep(int millis) throws InterruptedException
	{
		impl.sleep(millis);
	}
	public static void sleepUninterruptibly(int millis)
	{
		impl.sleepUninterruptibly(millis);
	}
}
