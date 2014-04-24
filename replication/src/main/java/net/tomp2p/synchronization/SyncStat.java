/*
 * Copyright 2013 Thomas Bocek, Maxat Pernebayev
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

package net.tomp2p.synchronization;

import net.tomp2p.peers.Number160;

public class SyncStat {
    
    final private int dataCopy;
    final private int dataOrig;
    final private Number160 fromPeer;
    final private Number160 toPeer;

    public SyncStat(Number160 fromPeer, Number160 toPeer, int dataCopy, int dataOrig) {
	    this.dataCopy = dataCopy;
	    this.dataOrig = dataOrig;
	    this.fromPeer = fromPeer;
	    this.toPeer = toPeer;
    }
    
    public int dataCopy() {
        return dataCopy;
    }
    
    public int dataOrig() {
        return dataOrig;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("sync stats from [");
        sb.append(fromPeer).append("] to [").append(toPeer).append(":");
        sb.append("send=").append(dataCopy).append("(orig=").append(dataOrig).append(")");
        sb.append(",ratio: ").append(dataOrig/(double)dataCopy);
        return sb.toString();
    }
}
