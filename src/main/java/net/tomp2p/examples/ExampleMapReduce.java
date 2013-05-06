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

package net.tomp2p.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureTask;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.builder.DHTBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.task.Worker;

/**
 * Example of simple map reduce operations.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExampleMapReduce {

    public static final String TEXT_1 = "to be or not to be";

    public static final String TEXT_2 = "to do is to be";

    public static final String TEXT_3 = "to be is to do";

    /**
     * Empty constructor.
     */
    private ExampleMapReduce() {
    }

    /**
     * Create 100 peers and start the map reduce example.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
        Peer master = null;
        try {
            final int peerNr = 100;
            final int port = 4001;
            Peer[] peers = ExampleUtils.createAndAttachNodes(peerNr, port);
            master = peers[0];
            ExampleUtils.bootstrap(peers);
            exampleMapReduce(peers);
        } finally {
            if (master != null) {
                master.shutdown();
            }
        }
    }

    /**
     * Start the map reduce example.
     * 
     * @param peers
     *            All the peers
     * @throws IOException .
     * @throws ClassNotFoundException .
     */
    private static void exampleMapReduce(final Peer[] peers) throws IOException, ClassNotFoundException {
        final Number160 result = Number160.createHash(0);
        final Number160 nr1 = Number160.createHash("1");
        final Number160 nr2 = Number160.createHash("2");
        final Number160 nr3 = Number160.createHash("3");
        Worker map = new Worker() {
            private static final long serialVersionUID = 276677516112036039L;

            @Override
            public Map<Number160, Data> execute(final Peer peer, final Number160 taskId,
                    final Map<Number160, Data> inputData) throws Exception {
                List<FutureDHT> futures = new ArrayList<FutureDHT>();
                Map<Number160, Data> retVal = new HashMap<Number160, Data>();
                for (Map.Entry<Number160, Data> entry : inputData.entrySet()) {
                    String text = (String) entry.getValue().getObject();
                    StringTokenizer st = new StringTokenizer(text, " ");
                    while (st.hasMoreTokens()) {
                        String word = st.nextToken();
                        Number160 key = Number160.createHash(word);
                        FutureDHT futureDHT = peer.add(key).setData(new Data(1)).setList(true).start();
                        System.out.println("map DHT call for word [" + word + "], key=" + key + " on peer "
                                + peer.getPeerID());
                        futures.add(futureDHT);
                        retVal.put(key, new Data(taskId));
                    }
                }
                for (FutureDHT futureDHT : futures) {
                    futureDHT.awaitUninterruptibly();
                }
                return retVal;
            }
        };
        Worker reduce = new Worker() {
            private static final long serialVersionUID = 87921790732341L;

            @Override
            public Map<Number160, Data> execute(final Peer peer, final Number160 taskId,
                    final Map<Number160, Data> inputData) throws Exception {
                Map<Number160, FutureDHT> futures = new HashMap<Number160, FutureDHT>();
                for (Map.Entry<Number160, Data> entry : inputData.entrySet()) {
                    Number160 key = entry.getKey();
                    int size = peer.getPeerBean().getStorage()
                            .get(key, DHTBuilder.DEFAULT_DOMAIN, Number160.ZERO, Number160.MAX_VALUE).size();
                    System.out.println("reduce DHT call " + key + " found " + size + " on peer " + peer.getPeerID());
                    Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
                    dataMap.put(peer.getPeerAddress().getID(), new Data(size));
                    futures.put(key, peer.put(result).setDataMap(dataMap).start());
                }
                Map<Number160, Data> retVal = new HashMap<Number160, Data>();
                for (Map.Entry<Number160, FutureDHT> entry : futures.entrySet()) {
                    entry.getValue().awaitUninterruptibly();
                    retVal.put(entry.getKey(), new Data(entry.getValue().getType()));
                }
                return retVal;
            }
        };
        final int peerSubmit1 = 33;
        final int peerSubmit2 = 34;
        final int peerSubmit3 = 35;
        FutureTask ft1 = peers[peerSubmit1].submit(nr1, map)
                .setRequestP2PConfiguration(new RequestP2PConfiguration(2, 0, 0)).setDataMap(createData(TEXT_1))
                .start();
        FutureTask ft2 = peers[peerSubmit2].submit(nr2, map)
                .setRequestP2PConfiguration(new RequestP2PConfiguration(2, 0, 0)).setDataMap(createData(TEXT_2))
                .start();
        FutureTask ft3 = peers[peerSubmit3].submit(nr3, map)
                .setRequestP2PConfiguration(new RequestP2PConfiguration(2, 0, 0)).setDataMap(createData(TEXT_3))
                .start();
        ft1.awaitUninterruptibly();
        ft2.awaitUninterruptibly();
        ft3.awaitUninterruptibly();
        Set<Number160> intermediate = new HashSet<Number160>();

        for (Map<Number160, Data> map1 : ft1.getRawDataMap().values()) {
            intermediate.addAll(map1.keySet());
        }
        for (Map<Number160, Data> map1 : ft2.getRawDataMap().values()) {
            intermediate.addAll(map1.keySet());
        }
        for (Map<Number160, Data> map1 : ft3.getRawDataMap().values()) {
            intermediate.addAll(map1.keySet());
        }
        int i = 0;
        List<FutureTask> resultList = new ArrayList<FutureTask>();
        System.out.println("we got " + intermediate.size() + " unique words");
        final int peerAdd = 40;
        for (Number160 location : intermediate) {
            resultList.add(peers[peerAdd + i].submit(location, reduce)
                    .setRequestP2PConfiguration(new RequestP2PConfiguration(1, 0, 0)).setDataMap(createData(location))
                    .start());
            i++;
        }
        // now we wait for the completion
        for (FutureTask futureTask : resultList) {
            futureTask.awaitUninterruptibly();
        }
        final int peerGet = 20;
        FutureDHT futureDHT = peers[peerGet].get(result).setAll().start();
        futureDHT.awaitUninterruptibly();
        int counter = 0;
        for (Map.Entry<Number160, Data> entry : futureDHT.getDataMap().entrySet()) {
            Data data = entry.getValue();
            Object obj = data.getObject();
            counter += (Integer) obj;
        }
        System.out.println("final result DHT call, count returns " + counter);
    }

    /**
     * @param location
     *            The location to encapsulate
     * @return The location encapsulated in a data object in a map
     * @throws IOException .
     */
    private static Map<Number160, Data> createData(final Number160 location) throws IOException {
        Map<Number160, Data> retVal = new HashMap<Number160, Data>();
        Data data = new Data(location);
        retVal.put(location, data);
        return retVal;
    }

    /**
     * @param text
     *            The text to encapsulate
     * @return The text encapsulated in a data object in a map
     * @throws IOException .
     */
    private static Map<Number160, Data> createData(final String text) throws IOException {
        Map<Number160, Data> retVal = new HashMap<Number160, Data>();
        Data data = new Data(text);
        retVal.put(data.getHash(), data);
        return retVal;
    }
}
