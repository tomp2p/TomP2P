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
package net.tomp2p.connection;

import java.util.concurrent.Semaphore;

import net.tomp2p.futures.FutureRunnable;

public interface Reservation
{
    public abstract void shutdown();

    public abstract boolean acquire( Semaphore semaphore, int permits );

    public abstract void runDeadLockProof( Scheduler scheduler, FutureRunnable futureRunnable );

    public abstract void prepareDeadLockCheck();

    public abstract void removeDeadLockCheck( long creatorThread );
}