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
 * Wraps a future into an other future. This is useful for futures that are
 * created later on. You can create a wrapper, return it to the user, create an
 * other future, wrap this created future and the wrapper will tell the user if
 * the newly created future has finished.
 * 
 * @author Thomas Bocek
 * @param <K>
 */
public class FutureWrapper<K extends BaseFuture> extends BaseFutureImpl<K> {
    private K wrappedFuture;

    /**
     * Wait for the future, which will cause this future to complete if the
     * wrapped future completes.
     * 
     * @param future
     *            The future to wrap
     */
    public void waitFor(final K future) {
        self(future);
        future.addListener(new BaseFutureAdapter<K>() {
            @Override
            public void operationComplete(final K future) throws Exception {
                synchronized (lock) {
                    if (!setCompletedAndNotify())
                        return;
                    type = future.getType();
                    reason = future.toString();
                    wrappedFuture = future;
                }
                notifyListerenrs();
            }
        });
    }

    /**
     * @return The wrapped (original) future.
     */
    public K getWrappedFuture() {
        synchronized (lock) {
            return wrappedFuture;
        }
    }
}
