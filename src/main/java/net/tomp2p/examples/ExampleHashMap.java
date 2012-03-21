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

import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

/**
 * This example shows how TomP2P can be used to have Objects instead of
 * Number160. While Number160 are deeply integrated in TomP2P, a wrapper around
 * the {@link Data} object can store the Object for the locationKey and
 * domainKey.
 * 
 * @author Thomas Bocek
 * 
 */
public class ExampleHashMap
{
	public static void main(String[] args) throws Exception
	{
		Peer master = null;
		try
		{
			Peer[] peers = Examples.createAndAttachNodes(100, 4001);
			master = peers[0];
			MyPeer myPeer = new MyPeer(master);
			Examples.bootstrap(peers);
			myPeer.put("This is my location key", "This is my domain", "This is my content key",
					"And here comes the data").awaitUninterruptibly();
			FutureDHT futureDHT = myPeer.get("This is my location key", "This is my domain",
					"This is my content key");
			futureDHT.awaitUninterruptibly();
			System.err.println(futureDHT.getFailedReason());
			Map<Number160, Data> map = futureDHT.getDataMap();
			for (Data data : map.values())
			{
				MyData myData = (MyData) data.getObject();
				System.out.println("key: " + myData.getKey() + ", domain: " + myData.getDomain()
						+ ", content: " + myData.getContent() + ", data: " + myData.getData());
			}
		}
		finally
		{
			master.shutdown();
		}
	}
	private static class MyPeer
	{
		final private Peer peer;
		private MyPeer(Peer peer)
		{
			this.peer = peer;
		}

		private FutureDHT get(String key, String domain, String content)
		{
			Number160 locationKey = Number160.createHash(key);
			Number160 domainKey = Number160.createHash(domain);
			Number160 contentKey = Number160.createHash(content);
			ConfigurationGet cg = Configurations.defaultGetConfiguration();
			cg.setDomain(domainKey);
			cg.setContentKey(contentKey);
			return peer.get(locationKey, cg);
		}

		private FutureDHT put(String key, String domain, String content, String data)
				throws IOException
		{
			Number160 locationKey = Number160.createHash(key);
			Number160 domainKey = Number160.createHash(domain);
			Number160 contentKey = Number160.createHash(content);
			MyData myData = new MyData();
			myData.setKey(key);
			myData.setDomain(domain);
			myData.setContent(content);
			myData.setData(data);
			ConfigurationStore cs = Configurations.defaultStoreConfiguration();
			cs.setDomain(domainKey);
			cs.setContentKey(contentKey);
			return peer.put(locationKey, new Data(myData), cs);
		}
	}
	private static class MyData implements Serializable
	{
		private static final long serialVersionUID = 2098774660703812030L;
		private Object key;
		private Object domain;
		private Object content;
		private Object data;

		public Object getKey()
		{
			return key;
		}

		public void setKey(Object key)
		{
			this.key = key;
		}

		public Object getDomain()
		{
			return domain;
		}

		public void setDomain(Object domain)
		{
			this.domain = domain;
		}

		public Object getContent()
		{
			return content;
		}

		public void setContent(Object content)
		{
			this.content = content;
		}

		public Object getData()
		{
			return data;
		}

		public void setData(Object data)
		{
			this.data = data;
		}
	}
}