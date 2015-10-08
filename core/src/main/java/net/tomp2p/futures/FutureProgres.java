/*
 * Copyright 2013 Thomas Bocek
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
 * A generic future that can be used to set a future to complete with an attachment.
 * 
 * @author Thomas Bocek
 * 
 * @param <K>
 */
public class FutureProgres<K> extends BaseFutureImpl<FutureProgres<K>> {

    private K object;
    private FutureProgres<K> next;

    /**
     * Creates a new future for the shutdown operation.
     */
    public FutureProgres() {
        self(this);
    }

    /**
     * This will finish this future, but it will create a new future that will be returned and that can be obtained by
     * {@link #next()}.
     * 
     * @param object
     *            The object to set
     * @param last
     *            If the there will be no more data, set flag to true. Same as {@link #done(Object)}.
     * @return A new future object or null if this was the last data piece
     */
    public FutureProgres<K> progres(final K object, final boolean last) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return null;
            }
            if (!last) {
                this.next = new FutureProgres<K>();
            }
            this.object = object;
            this.type = BaseFuture.FutureType.OK;
        }
        notifyListeners();
        return last ? null : next;
    }

    /**
     * Set future as finished and notify listeners.
     * 
     * @param object
     *            An object that can be attached.
     * @return This class
     */
    public FutureProgres<K> done(final K object) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return this;
            }
            this.object = object;
            this.type = BaseFuture.FutureType.OK;
        }
        notifyListeners();
        return this;
    }

    /**
     * @return The attached object
     */
    public K object() {
        synchronized (lock) {
            return object;
        }
    }

    /**
     * @return The next future if {@link #setProgres(Object)} is calledU
     */
    public FutureProgres<K> next() {
        synchronized (lock) {
            return next;
        }
    }
}
