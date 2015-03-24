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
import java.util.HashSet;
import java.util.Set;

import net.tomp2p.dht.FutureDigest;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

/**
 * Example how to search.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExampleSearch {
    private static final String TERM = "Communication Systems Group";

    /**
     * Empty constructor.
     */
    private ExampleSearch() {
    }

    /**
     * Start the examples.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
        final int peerNr = 100;
        final int port = 4001;
        PeerDHT[] peers = null;
        try {
            peers = ExampleUtils.createAndAttachPeersDHT(peerNr, port);
            ExampleUtils.bootstrap(peers);
            exampleSearch(peers);
            exampleKeywordSearch(peers);
            exampleBloomfilerSearch(peers);
            exampleXORSearch(peers);
        } finally {
            // 0 is the master
            if (peers != null && peers[0] != null) {
                peers[0].shutdown();
            }
        }
    }

    /**
     * Search for term. This also stores the term under its hash.
     * 
     * @param peers
     *            All the peers
     * @throws IOException .
     * @throws ClassNotFoundException .
     */
    private static void exampleSearch(final PeerDHT[] peers) throws IOException, ClassNotFoundException {
        final int peer30 = 30;
        final int peer60 = 60;

        Number160 key = Number160.createHash(TERM);

        FuturePut futurePut = peers[peer60].put(key).object(TERM).start();
        futurePut.awaitUninterruptibly();

        FutureGet futureGet = peers[peer30].get(key).start();
        futureGet.awaitUninterruptibly();

        System.out.println("got: " + key + " = " + futureGet.data().object());

    }

    /**
     * Search for a keyword and find a term.
     * 
     * @param peers
     *            All the peers.
     * @throws IOException .
     * @throws ClassNotFoundException .
     */
    private static void exampleKeywordSearch(final PeerDHT[] peers) throws IOException, ClassNotFoundException {
        final int peer10 = 10;
        final int peer20 = 20;

        Number160 keyTerm = Number160.createHash(TERM);
        String[] keywords = TERM.split(" ");

        // store a keyword
        for (String keyword : keywords) {
            Number160 keyKeyword = Number160.createHash(keyword);
            peers[peer10].put(keyKeyword).object(keyTerm).start().awaitUninterruptibly();
        }

        // search for a keyword
        Number160 termKey = findReference(peers[peer20], "Communication");
        if(termKey == null) {
        	System.out.println("nothing found");
        	return;
        }
        // this will return a reference to the term stored in the method exampleSearch(), next, we have to search for
        // that.
        FutureGet futureGet = peers[peer10].get(termKey).start();
        futureGet.awaitUninterruptibly();
        System.out.println("searched (keyword) for [Communication], found " + futureGet.data().object());
    }
    
    private static void exampleXORSearch(final PeerDHT[] peers) throws IOException, ClassNotFoundException {
    	Number160 keyTerm = Number160.createHash(TERM);
        String[] keywords = TERM.split(" ");
        Set<Number160> xoredKeywords = new HashSet<Number160>();
        for (String keyword1 : keywords) {
        	for (String keyword2 : keywords) {
        		if(!keyword1.equals(keyword2)) {
        			Number160 key1Keyword = Number160.createHash(keyword1);
        			Number160 key2Keyword = Number160.createHash(keyword2);
        			xoredKeywords.add(key1Keyword.xor(key2Keyword));
        		} else {
        			Number160 key1Keyword = Number160.createHash(keyword1);
        			xoredKeywords.add(key1Keyword);
        		}
        	}
        }
        System.out.println("we store "+xoredKeywords.size() + " keywords");
        for(Number160 key: xoredKeywords) {
        	peers[10].put(key).object(keyTerm).start().awaitUninterruptibly();
        }
        // search for a keyword
        Number160 key1Keyword = Number160.createHash("Communication");
		Number160 key2Keyword = Number160.createHash("Systems");
        Number160 termKey = findReference(peers[20], key1Keyword.xor(key2Keyword));
        
        if(termKey != null) {
        	FutureGet futureGet = peers[10].get(termKey).start();
        	futureGet.awaitUninterruptibly();
        	System.out.println("searched (xor) for [Communication and Systems], found " + futureGet.data().object());
        } else {
        	System.out.println("searched (xor) for [Communication and Systems], nothing found");
        }
    }
    
    private static void exampleBloomfilerSearch(final PeerDHT[] peers) throws IOException, ClassNotFoundException {
    	 Number160 keyTerm = Number160.createHash(TERM);
         String[] keywords = TERM.split(" ");

         // store a keyword
         for (String keyword : keywords) {
             Number160 keyKeyword = Number160.createHash(keyword);
             peers[10].put(keyKeyword).object(keyTerm).start().awaitUninterruptibly();
         }
         
         Number160 termKey = findReference(peers[20], Number160.createHash("Communication"), Number160.createHash("Systems"));
         
         if(termKey != null) {
         	FutureGet futureGet = peers[10].get(termKey).start();
         	futureGet.awaitUninterruptibly();
         	System.out.println("searched (bf) for [Communication and Systems], found " + futureGet.data().object());
         } else {
         	System.out.println("searched (bf) for [Communication and Systems], nothing found");
         }
    }

    /**
     * Finds a reference and returns it.
     * 
     * @param peer
     *            The peer that searches for the reference.
     * @param keyword
     *            The keyword to search.
     * @return The reference to the keyword or null.
     * @throws ClassNotFoundException .
     * @throws IOException .
     */
    private static Number160 findReference(final PeerDHT peer, final String keyword) throws ClassNotFoundException,
            IOException {
        Number160 keyKeyword = Number160.createHash(keyword);
        FutureGet futureGet = peer.get(keyKeyword).start();
        futureGet.awaitUninterruptibly();
        if(futureGet.data() == null) {
        	return null;
        }
        Number160 termKey = (Number160) futureGet.data().object();
        return termKey;
    }
    
    private static Number160 findReference(final PeerDHT peer, Number160 keyKeyword) throws ClassNotFoundException,
    		IOException {
    	FutureGet futureGet = peer.get(keyKeyword).start();
    	futureGet.awaitUninterruptibly();
    	Data data = futureGet.data();
    	if(data != null && data.object()!=null) {
    		return (Number160) futureGet.data().object();
    	} 
    	return null;
    }
    
    private static Number160 findReference(final PeerDHT peer, Number160 key1Keyword, Number160 key2Keyword) throws ClassNotFoundException,
			IOException {
    	FutureDigest futureDigest = peer.digest(key1Keyword).returnAllBloomFilter().start();
    	futureDigest.awaitUninterruptibly();
        // we have the bloom filter for the content keys:
        SimpleBloomFilter<Number160> contentBF = futureDigest.digest().contentBloomFilter();
        System.err.println("bf:"+contentBF.getBitSet());
        
    	FutureGet futureGet = peer.get(key2Keyword).all().contentBloomFilter(contentBF).start();
    	futureGet.awaitUninterruptibly();
    	
    	
    	Data data = futureGet.data();
    	if(data != null && data.object()!=null) {
    		return (Number160) futureGet.data().object();
    	} 
    	return null;
    }
}
