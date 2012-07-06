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

package net.tomp2p.futures;

import org.jboss.netty.channel.Channel;

/**
 * Keeps track of the creation of a UDP or TCP channel. Since this may take a while, we return a future, and the user
 * gets notified when its finished.
 * 
 * @author Thomas Bocek
 */
public class FutureChannel
    extends BaseFutureImpl<FutureChannel>
{
    private Channel channel;

    private boolean semaphoreAcquired;

    public FutureChannel()
    {
        self( this );
    }

    /**
     * Finishes this future.
     * 
     * @param channel The created UDP or TCP Netty channel
     */
    public void setChannel( Channel channel )
    {
        synchronized ( lock )
        {
            if ( !setCompletedAndNotify() )
            {
                return;
            }
            this.channel = channel;
            this.type = FutureType.OK;
        }
        notifyListerenrs();
    }

    /**
     * @return The created UDP or TCP Netty channel
     */
    public Channel getChannel()
    {
        synchronized ( lock )
        {
            return channel;
        }
    }

    /**
     * Set a flag if a semaphore has been acquired for this channel. If it is not set, and we cannot create new channel,
     * the creation of this channel will be put in a queue and executed later.
     * 
     * @param semaphoreAcquired True if we already could acquire a semaphore
     */
    public void setAcquired( boolean semaphoreAcquired )
    {
        synchronized ( lock )
        {
            this.semaphoreAcquired = semaphoreAcquired;
        }
    }

    /**
     * @return The state if the channel has acquired a semaphore
     */
    public boolean isAcquired()
    {
        synchronized ( lock )
        {
            return semaphoreAcquired;
        }
    }
}
