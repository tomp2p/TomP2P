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

/**
 * For direct replication, the DHT needs to be created repeatedly. In order for the user to be able to customize the
 * creation, this interface exists. The user can create the future for example with data reading from a file or database
 * instead of keeping everything in memory.
 * 
 * @author Thomas Bocek
 * @param <K>
 */
public interface FutureCreator<K extends BaseFuture>
{
    /**
     * @return A newly created future
     */
    public K create();
}
