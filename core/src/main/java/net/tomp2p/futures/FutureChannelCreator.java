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

import net.tomp2p.connection2.ChannelCreator;

/**
 * This future is used for the connection reservation. If notifies the user when
 * a connection could have been reserved.
 * 
 * @author Thomas Bocek
 */
public class FutureChannelCreator extends BaseFutureImpl<FutureChannelCreator> {
    private ChannelCreator channelCreator;

    /**
     * Creates a new future for the shutdown operation.
     */
    public FutureChannelCreator() {
        self(this);
    }

    /**
     * Called if a channel creator could be created. With this channel creator
     * connections can be created.
     * 
     * @param channelCreator
     *            The newly created ChannelCreator
     */
    public void reserved(final ChannelCreator channelCreator) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.type = FutureType.OK;
            this.channelCreator = channelCreator;
        }
        notifyListeners();
    }

    /**
     * @return The ChannelCreator
     */
    public ChannelCreator getChannelCreator() {
        synchronized (lock) {
            return channelCreator;
        }
    }
}
