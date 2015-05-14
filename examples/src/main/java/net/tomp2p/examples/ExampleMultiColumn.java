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

package net.tomp2p.examples;

import java.io.IOException;
import java.util.Map;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 * Example of storing data in a column.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExampleMultiColumn {

    /**
     * Empty constructor.
     */
    private ExampleMultiColumn() {
    }

    /**
     * Start the examples.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
    	PeerDHT master = null;
        try {
            final int peerNr = 100;
            final int port = 4001;
            PeerDHT[] peers = ExampleUtils.createAndAttachPeersDHT(peerNr, port);
            master = peers[0];
            ExampleUtils.bootstrap(peers);
            exampleMultiColumn(peers);
        } finally {
            if (master != null) {
                master.shutdown();
            }
        }
    }

    /**
     * Show the example of multiple columns. At the moment only contentKey splitting is supported.
     * 
     * @param peers
     *            All the peers.
     * @throws IOException .
     * @throws ClassNotFoundException .
     */
    private static void exampleMultiColumn(final PeerDHT[] peers) throws IOException, ClassNotFoundException {
        // currently only contentKey splitting supported
        String rowKey1 = "tbocek";
        String rowKey2 = "fhecht";
        String rowKey3 = "gmachado";
        String rowKey4 = "tsiaras";
        String rowKey5 = "bstiller";

        String col1 = "first name";
        String col2 = "last name";
        String col3 = "loaction";
        String col4 = "nr";

        Number160 locationKey = Number160.createHash("users");
        System.out.println("storing data in table (identified by locationKey) " + locationKey);
        
        multiAdd(peers[11], rowKey1, col1, "thomas");
        multiAdd(peers[11], rowKey1, col2, "bocek");
        multiAdd(peers[11], rowKey1, col3, "zuerich");
        multiAdd(peers[11], rowKey1, col4, "77");
        //
        multiAdd(peers[11], rowKey2, col1, "fabio");
        multiAdd(peers[11], rowKey2, col2, "hecht");
        multiAdd(peers[11], rowKey2, col3, "zuerich");
        //
        multiAdd(peers[11], rowKey3, col1, "guilherme");
        multiAdd(peers[11], rowKey3, col2, "machado");
        multiAdd(peers[11], rowKey3, col3, "zuerich");
        //
        multiAdd(peers[11], rowKey4, col1, "christos");
        multiAdd(peers[11], rowKey4, col2, "tsiaras");
        multiAdd(peers[11], rowKey4, col3, "zuerich");
        //
        multiAdd(peers[11], rowKey5, col1, "burkhard");
        multiAdd(peers[11], rowKey5, col2, "stiller");
        multiAdd(peers[11], rowKey5, col3, "zuerich");
        //
        // search

        Number160 contentKey = combine(rowKey1, col1);
        // get entry
        final int peerGet = 22;
        FutureGet futureGet = peers[peerGet].get(locationKey).contentKey(contentKey).start();
        futureGet.awaitUninterruptibly();
        System.out
                .println("single fetch for (" + rowKey1 + "," + col1 + "): [" + futureGet.data().object() + "]");
        // get list
        
        Number640 from = new Number640(locationKey, Number160.ZERO, createNr(rowKey1, 0), Number160.ZERO);
        Number640 to = new Number640(locationKey, Number160.ZERO, createNr(rowKey1, -1), Number160.MAX_VALUE);
        
        // get can also be used with ranges for content keys
        FutureGet futureGet2 = peers[peerGet].get(locationKey).from(from).to(to).start();
        futureGet2.awaitUninterruptibly();
        System.out.println("row fetch [" + rowKey1 + "]");
        for (Map.Entry<Number640, Data> entry : futureGet2.dataMap().entrySet()) {
            System.out.println("multi fetch: " + entry.getValue().object());
        }
        // column get
        String search = col1;
        
        from = new Number640(locationKey, Number160.ZERO, createNr(search, 0), Number160.ZERO);
        to = new Number640(locationKey, Number160.ZERO, createNr(search, -1), Number160.MAX_VALUE);
        
        FutureGet futureGet3 = peers[peerGet].get(locationKey).from(from).to(to).start();
        futureGet3.awaitUninterruptibly();
        System.out.println("column fetch [" + search + "]");
        for (Map.Entry<Number640, Data> entry : futureGet3.dataMap().entrySet()) {
            System.out.println("multi fetch: " + entry.getValue().object());
        }
    }

    /**
     * Add it twice, once to search for the column, once for the row.
     * 
     * @param peer
     *            The peer that stores the data
     * @param rowKey
     *            The row key
     * @param col
     *            The col key
     * @param string
     *            The data to store
     * @throws IOException .
     */
    private static void multiAdd(final PeerDHT peer, final String rowKey, final String col, final String string)
            throws IOException {
        Number160 locationKey = Number160.createHash("users");
        Number160 contentKey = combine(rowKey, col);
        peer.put(locationKey).data(contentKey, new Data(string)).start().awaitUninterruptibly();
        contentKey = combine(col, rowKey);
        peer.put(locationKey).data(contentKey, new Data(string)).start().awaitUninterruptibly();
    }

    /**
     * Create a partial key with the first 64bit and fills the last 96bit with nr.
     * 
     * @param key1
     *            The key to fill the first 64bits
     * @param nr
     *            The number to fill the last 96bits
     * @return The complete key
     */
    //CHECKSTYLE:OFF
    private static Number160 createNr(final String key1, final int nr) {
        Number160 firstKey = Number160.createHash(key1);
        int[] val = firstKey.toIntArray();
        val[0] = val[0] ^ val[4] ^ val[3] ^ val[2];
        val[1] = val[1] ^ val[4] ^ val[3] ^ val[2];
        val[2] = nr;
        val[3] = nr;
        val[4] = nr;
        return new Number160(val);
    }

    //CHECKSTYLE:ON

    /**
     * Combines two keys into one.
     * 
     * @param key1
     *            The first key
     * @param key2
     *            The second key
     * @return the hash of the combined keys
     */
    private static Number160 combine(final String key1, final String key2) {
        Number160 firstKey = Number160.createHash(key1);
        Number160 secondKey = Number160.createHash(key2);
        return combine(firstKey, secondKey);
    }

    /**
     * Combines two keys into one.
     * 
     * @param firstKey
     *            The first key
     * @param secondKey
     *            The second key
     * @return the hash of the combined keys
     */
    //CHECKSTYLE:OFF
    private static Number160 combine(final Number160 firstKey, final Number160 secondKey) {
        int[] val1 = firstKey.toIntArray();

        val1[0] = val1[0] ^ val1[4] ^ val1[3] ^ val1[2];
        val1[1] = val1[1] ^ val1[4] ^ val1[3] ^ val1[2];
        int[] val2 = secondKey.toIntArray();

        val2[2] = val2[2] ^ val2[0] ^ val2[1];
        val2[3] = val2[3] ^ val2[0] ^ val2[1];
        val2[4] = val2[4] ^ val2[0] ^ val2[1];

        val2[0] = val1[0];
        val2[1] = val1[1];

        return new Number160(val2);
    }
    //CHECKSTYLE:ON
}
