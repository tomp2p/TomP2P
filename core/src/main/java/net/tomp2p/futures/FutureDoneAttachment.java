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
 * @param <K, T>
 */
public class FutureDoneAttachment<K, T> extends BaseFutureImpl<FutureDoneAttachment<K, T>> {
	
    public static FutureDoneAttachment<Void, Void> SUCCESS = new FutureDoneAttachment<Void, Void>().done();

    private K object;
    private T attachment;
    
    public FutureDoneAttachment(T attachment) {
        self(this);
        attachment(attachment);
    }

    /**
     * Creates a new future for the shutdown operation.
     */
    public FutureDoneAttachment() {
        self(this);
    }

    /**
     * Set future as finished and notify listeners.
     * 
     * @return This class
     */
    public FutureDoneAttachment<K, T> done() {
        done(null);
        return this;
    }

    /**
     * Set future as finished and notify listeners.
     * 
     * @param object
     *            An object that can be attached.
     * @return This class
     */
    public FutureDoneAttachment<K, T> done(final K object) {
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
    
    public T attachment() {
        synchronized (lock) {
            return attachment;
        }
    }
    
    public FutureDoneAttachment<K, T> attachment(T attachment) {
        synchronized (lock) {
            this.attachment = attachment;
        }
        return this;
    }
}
