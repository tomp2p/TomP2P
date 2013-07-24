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
 * A future runnable is used for running futures in the background (in a
 * different thread)
 * 
 * @author Thomas Bocek
 */
public interface FutureRunnable extends Runnable {

    /**
     * A run() method that fails is very silent, thus we provide failed, which
     * can be used to set a future to fail.
     * 
     * @param reason
     *            The reason why something failed.
     */
    public abstract void failed(String reason);

}
