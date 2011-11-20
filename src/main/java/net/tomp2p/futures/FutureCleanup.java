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
package net.tomp2p.futures;

/**
 * Some futures have a longer life span than others. Typically, a future sets
 * completed to true and thats it, but FutureTracker and FutureDHT may keep
 * track of futures created based on an initial future. To shutdown / cleanup
 * those up, we use this interface.
 * 
 * @author Thomas Bocek
 * 
 */
public interface FutureCleanup
{
	/**
	 * Call to add cleanup classes for the future e.g. to stop creating more
	 * scheduled futures.
	 * 
	 * @param cancellable The cleanup classes
	 */
	public abstract void addCleanup(Cancellable cancellable);
}
