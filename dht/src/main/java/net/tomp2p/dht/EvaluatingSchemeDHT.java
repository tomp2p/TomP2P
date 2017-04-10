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
package net.tomp2p.dht;

import io.netty.buffer.ByteBuf;
import java.util.Collection;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestResult;
import net.tomp2p.storage.Data;

public interface EvaluatingSchemeDHT {
    public Collection<Number640> evaluate1(Map<PeerAddress, Map<Number640, Number160>> rawKeys480);

    public Map<Number640, Data> evaluate2(Map<PeerAddress, Map<Number640, Data>> rawData);

    public Object evaluate3(Map<PeerAddress, Object> rawObjects);

    public ByteBuf evaluate4(Map<PeerAddress, ByteBuf> rawChannels);

    public DigestResult evaluate5(Map<PeerAddress, DigestResult> rawDigest);
    
    public Collection<Number640> evaluate6(Map<PeerAddress, Map<Number640, Byte>> rawKeys480);
    
}
