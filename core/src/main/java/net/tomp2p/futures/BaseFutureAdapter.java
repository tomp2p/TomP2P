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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BaseFuture} always completes either successfully or failed. In
 * either case a {@link BaseFutureListener} is called. If this future fails,
 * then exception is caught. Since this should never happen in this framework,
 * we use an adapter where we only have to define
 * BaseFutureListener#operationComplete(BaseFuture). Since the future is exposed
 * to the application, that may have exceptions in the listener, developers
 * should rather use BaseFutureListener#operationComplete(BaseFuture).
 * 
 * @author Thomas Bocek
 * @param <F>
 */
public abstract class BaseFutureAdapter<F extends BaseFuture> implements BaseFutureListener<F> {
    final private static Logger logger = LoggerFactory.getLogger(BaseFutureAdapter.class);

    /**
     * Prints out the error using the logger.
     */
    @Override
    public void exceptionCaught(final Throwable t) throws Exception {
        if (logger.isErrorEnabled()) {
            logger.error("BaseFutureAdapter exceptionCaught()", t);
        }
        t.printStackTrace();
    }
}
