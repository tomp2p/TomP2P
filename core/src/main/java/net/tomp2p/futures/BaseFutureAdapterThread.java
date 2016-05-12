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

import java.util.concurrent.ExecutorService;
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
public abstract class BaseFutureAdapterThread<F extends BaseFuture> implements BaseFutureListener<F> {
    final private static Logger LOG = LoggerFactory.getLogger(BaseFutureAdapterThread.class);

    final private ExecutorService executorService;
    public BaseFutureAdapterThread(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public final void operationComplete(final F future) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    operationCompleteThread(future);
                } catch (Throwable t) {
                    exceptionCaught(t);
                }
            
            }
        });
    }
    
    public abstract void operationCompleteThread(F future) throws Exception;
    
    /**
     * Prints out the error using the logger and System.err.
     */
    @Override
    public void exceptionCaught(final Throwable t) {
        LOG.error("BaseFutureAdapter exceptionCaught()", t);
        t.printStackTrace();
    }
}