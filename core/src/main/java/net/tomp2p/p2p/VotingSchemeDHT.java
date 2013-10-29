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
package net.tomp2p.p2p;

import io.netty.buffer.ByteBuf;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;

public class VotingSchemeDHT implements EvaluatingSchemeDHT {
    @Override
    public Collection<Number480> evaluate1(Map<PeerAddress, Map<Number480, Number160>> rawKeys480) {
        Map<Number480, Integer> counter = new HashMap<Number480, Integer>();
        Set<Number480> result = new HashSet<Number480>();

        int size = rawKeys480 == null ? 0 : rawKeys480.size();
        int majority = (size + 1) / 2;

        if (rawKeys480 != null) {
            for (PeerAddress address : rawKeys480.keySet()) {
                Collection<Number480> keys480 = rawKeys480.get(address).keySet();
                if (keys480 != null) {
                    for (Number480 key : keys480) {
                        int c = 1;
                        Integer count = counter.get(key);
                        if (count != null)
                            c = count + 1;
                        counter.put(key, c);
                        if (c >= majority)
                            result.add(key);
                    }
                }
            }
        }

        return result;
    }
    
    @Override
    public Collection<Number480> evaluate6(Map<PeerAddress, Collection<Number480>> rawKeys480) {
        Map<Number480, Integer> counter = new HashMap<Number480, Integer>();
        Set<Number480> result = new HashSet<Number480>();

        int size = rawKeys480 == null ? 0 : rawKeys480.size();
        int majority = (size + 1) / 2;

        if (rawKeys480 != null) {
            for (PeerAddress address : rawKeys480.keySet()) {
                Collection<Number480> keys480 = rawKeys480.get(address);
                if (keys480 != null) {
                    for (Number480 key : keys480) {
                        int c = 1;
                        Integer count = counter.get(key);
                        if (count != null)
                            c = count + 1;
                        counter.put(key, c);
                        if (c >= majority)
                            result.add(key);
                    }
                }
            }
        }

        return result;
    }

    @Override
    public Map<Number480, Data> evaluate2(final Map<PeerAddress, Map<Number480, Data>> rawData) {
        if (rawData == null) {
            throw new IllegalArgumentException("cannot evaluate, as no result provided");
        }
        Map<Number160, Integer> counter = new HashMap<Number160, Integer>();
        Map<Number480, Data> result = new HashMap<Number480, Data>();
        int size = rawData.size();
        int majority = (size + 1) / 2;
        for (PeerAddress address : rawData.keySet()) {
            Map<Number480, Data> data = rawData.get(address);
            for (Number480 contentKey : data.keySet()) {
                Data dat = data.get(contentKey);
                Number160 hash = dat.hash().xor(contentKey.getContentKey()).
                        xor(contentKey.getDomainKey()).xor(contentKey.getLocationKey());
                int c = 1;
                Integer count = counter.get(hash);
                if (count != null) {
                    c = count + 1;
                }
                counter.put(hash, c);
                if (c >= majority) {
                    result.put(contentKey, dat);
                }
            }
        }
        return result;
    }

    @Override
    public Object evaluate3(Map<PeerAddress, Object> rawKeys) {
        return evaluate0(rawKeys);
    }

    @Override
    public ByteBuf evaluate4(Map<PeerAddress, ByteBuf> rawKeys) {
        return evaluate0(rawKeys);
    }

    @Override
    public DigestResult evaluate5(Map<PeerAddress, DigestResult> rawDigest) {
        return evaluate0(rawDigest);
    }

    private static <K> K evaluate0(Map<PeerAddress, K> raw) {
        if (raw == null)
            throw new IllegalArgumentException(
                    "cannot evaluate, as no result provided. Most likely you are not using direct messages, but rather put() or add(). For put and add, you have to use getData()");
        Map<K, Integer> counter = new HashMap<K, Integer>();
        K best = null;
        int count = 0;
        for (PeerAddress address : raw.keySet()) {
            K k = raw.get(address);
            if (k != null) {
                Integer c = counter.get(k);
                if (c == null)
                    c = 0;
                c++;
                counter.put(k, c);
                if (c > count) {
                    best = k;
                    count = c;
                }
            }
        }
        return best;
    }
}
