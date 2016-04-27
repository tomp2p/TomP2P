/*
 * Copyright 2016 Thomas Bocek
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
package net.tomp2p.examples;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 *
 * @author Thomas Bocek
 */
public class ExampleBroadcast {
    public static void main(final String[] args) throws Exception {
        
        Peer[] peers = null;
        try {
            peers = ExampleUtils.createAndAttachPeersBroadcast(100, 4001);
            ExampleUtils.bootstrap(peers);
            exampleBroadcast(peers);
            Thread.sleep(3000);
        } finally {
            // 0 is the master
            if (peers != null && peers[0] != null) {
                peers[0].shutdown();
            }
        }
    }

    private static void exampleBroadcast(Peer[] peers) throws IOException {
        NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
        dataMap.put(Number640.ZERO, new Data("testme"));
        //take any peer and send broadcast
        peers[42].broadcast(Number160.createHash("blub")).dataMap(dataMap).start();
    }
}
