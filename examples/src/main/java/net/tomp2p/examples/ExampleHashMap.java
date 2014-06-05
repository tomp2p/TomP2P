/*
 * Copyright 2011 Thomas Bocek
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
import java.io.Serializable;
import java.util.Map;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 * This example shows how TomP2P can be used to have Objects instead of
 * Number160. While Number160 are deeply integrated in TomP2P, a wrapper around
 * the {@link Data} object can store the Object for the locationKey and
 * domainKey.
 * 
 * @author Thomas Bocek
 */
public class ExampleHashMap {
	public static void main(String[] args) throws Exception {
		PeerDHT master = null;
		try {
			PeerDHT[] peers = ExampleUtils.createAndAttachPeersDHT(100, 4001);
			master = peers[0];
			MyPeer myPeer1 = new MyPeer(peers[0]);
			ExampleUtils.bootstrap(peers);
			myPeer1.put("This is my location key", "This is my domain", "This is my content key",
			        "And here comes the data").awaitUninterruptibly();
			MyPeer myPeer2 = new MyPeer(peers[5]);
			FutureGet futureGet = myPeer2.get("This is my location key", "This is my domain", "This is my content key");
			futureGet.awaitUninterruptibly();
			Map<Number640, Data> map = futureGet.dataMap();
			for (Data data : map.values()) {
				@SuppressWarnings("unchecked")
                MyData<String> myData = (MyData<String>) data.object();
				System.out.println("key: " + myData.key() + ", domain: " + myData.domain() + ", content: "
				        + myData.content() + ", data: " + myData.data());
			}
		} finally {
			master.shutdown();
		}
	}

	private static class MyPeer {
		final private PeerDHT peer;

		private MyPeer(PeerDHT peer) {
			this.peer = peer;
		}

		private FutureGet get(String key, String domain, String content) {
			Number160 locationKey = Number160.createHash(key);
			Number160 domainKey = Number160.createHash(domain);
			Number160 contentKey = Number160.createHash(content);
			return peer.get(locationKey).domainKey(domainKey).contentKey(contentKey).start();
		}

		private FuturePut put(String key, String domain, String content, String data) throws IOException {
			Number160 locationKey = Number160.createHash(key);
			Number160 domainKey = Number160.createHash(domain);
			Number160 contentKey = Number160.createHash(content);
			MyData<String> myData = new MyData<String>().key(key).domain(domain).content(content).data(data);
			return peer.put(locationKey).domainKey(domainKey).data(contentKey, new Data(myData)).start();
		}
	}

	private static class MyData<K> implements Serializable {
		private static final long serialVersionUID = 2098774660703812030L;

		private K key;

		private K domain;

		private K content;

		private K data;

		public K key() {
			return key;
		}

		public MyData<K> key(K key) {
			this.key = key;
			return this;
		}

		public Object domain() {
			return domain;
		}

		public MyData<K> domain(K domain) {
			this.domain = domain;
			return this;
		}

		public K content() {
			return content;
		}

		public MyData<K> content(K content) {
			this.content = content;
			return this;
		}

		public K data() {
			return data;
		}

		public MyData<K> data(K data) {
			this.data = data;
			return this;
		}
	}
}