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

package net.tomp2p.rpc;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

/**
 * A bean that stores a hash and data.
 * 
 * @author Thomas Bocek
 */
public class HashData {
    final private Number160 hash;

    final private Data data;

    public HashData(Number160 hash, Data data) {
        this.hash = hash;
        this.data = data;
    }

    public Number160 getHash() {
        return hash;
    }

    public Data getData() {
        return data;
    }
}
