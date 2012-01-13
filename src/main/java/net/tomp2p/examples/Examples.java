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
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.Bindings.Protocol;
import net.tomp2p.connection.DiscoverNetworks;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;


/**
 * This simple example creates 10 nodes, bootstraps to the first and put and get
 * data from those 10 nodes.
 * 
 * @author draft
 * 
 */
public class Examples
{
	final private static Random rnd = new Random(42L);

	public static void main(String[] args) throws Exception
	{
		exampleDHT();
		exampleNAT();
	}

	public static void exampleDHT() throws Exception
	{
		Peer master = null;
		try
		{
			master = new Peer(new Number160(rnd));
			master.listen(4001, 4001);
			Peer[] nodes = createAndAttachNodes(master, 100);
			bootstrap(master, nodes);
			examplePutGet(nodes);
			exampleAddGet(nodes);
		}
		finally
		{
			master.shutdown();
		}
	}

	public static void exampleNAT() throws Exception
	{
		Peer master = new Peer(new Number160(rnd));
		Bindings b = new Bindings(Protocol.IPv4, Inet4Address.getByName("127.0.0.1"), 4001, 4001);
		b.addInterface("eth0");
		System.out.println("Listening to: " + DiscoverNetworks.discoverInterfaces(b));
		master.listen(4001, 4001, b);
		System.out.println("address visible to outside is " + master.getPeerAddress());
		master.shutdown();
	}

	public static void bootstrap(Peer master, Peer[] nodes)
	{
		List<FutureBootstrap> futures1 = new ArrayList<FutureBootstrap>();
		List<FutureDiscover> futures2 = new ArrayList<FutureDiscover>();
		for (int i = 1; i < nodes.length; i++)
		{
			FutureDiscover tmp=nodes[i].discover(master.getPeerAddress());
			futures2.add(tmp);
		}
		for (FutureDiscover future : futures2)
			future.awaitUninterruptibly();
		for (int i = 1; i < nodes.length; i++)
		{
			FutureBootstrap tmp = nodes[i].bootstrap(master.getPeerAddress());
			futures1.add(tmp);
		}
		for (int i = 1; i < nodes.length; i++)
		{
			FutureBootstrap tmp = master.bootstrap(nodes[i].getPeerAddress());
			futures1.add(tmp);
		}
		for (FutureBootstrap future : futures1)
			future.awaitUninterruptibly();
	}

	public static void examplePutGet(Peer[] nodes) throws IOException
	{
		Number160 nr = new Number160(rnd);
		String toStore = "hallo";
		Data data = new Data(toStore.getBytes());
		FutureDHT futureDHT = nodes[30].put(nr, data);
		futureDHT.awaitUninterruptibly();
		System.out.println("stored: " + toStore + " (" + futureDHT.isSuccess() + ")");
		futureDHT = nodes[77].get(nr);
		futureDHT.awaitUninterruptibly();
		System.out.println("got from put get: "
				+ new String(futureDHT.getRawData().values().iterator().next().values().iterator()
						.next().getData()) + " (" + futureDHT.isSuccess() + ")");
	}

	private static void exampleAddGet(Peer[] nodes) throws IOException, ClassNotFoundException
	{
		Number160 nr = new Number160(rnd);
		String toStore1 = "hallo1";
		String toStore2 = "hallo2";
		Data data1 = new Data(toStore1);
		Data data2 = new Data(toStore2);
		FutureDHT futureDHT = nodes[30].add(nr, data1);
		futureDHT.awaitUninterruptibly();
		System.out.println("added: " + toStore1 + " (" + futureDHT.isSuccess() + ")");
		futureDHT = nodes[50].add(nr, data2);
		futureDHT.awaitUninterruptibly();
		System.out.println("added: " + toStore2 + " (" + futureDHT.isSuccess() + ")");
		futureDHT = nodes[77].getAll(nr);
		futureDHT.awaitUninterruptibly();
		System.out.println("size" + futureDHT.getData().size());
		Iterator<Data> iterator = futureDHT.getData().values().iterator();
		System.out.println("got: " + iterator.next().getObject() + " ("
				+ futureDHT.isSuccess() + ")");
		System.out.println("got: " + iterator.next().getObject() + " ("
				+ futureDHT.isSuccess() + ")");
	}
	
	public static void exampleGetBlocking(Peer[] nodes,  Number160 nr)
        {
	  FutureDHT futureDHT = nodes[77].get(nr);
	  //blocking operation
          futureDHT.awaitUninterruptibly();
          System.out.println("result: "+futureDHT.getObject());
          System.out.println("this may *not* happen before printing the result");
        }
	
	public static void exampleGetNonBlocking(Peer[] nodes,  Number160 nr)
        {
          FutureDHT futureDHT = nodes[77].get(nr);
          //non-blocking operation
          futureDHT.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
              System.out.println("result: "+future.getObject());
            }
          });
          System.out.println("this may happen before printing the result");
        }

	public static Peer[] createAndAttachNodes(Peer master, int nr) throws Exception
	{
		Peer[] nodes = new Peer[nr];
		nodes[0] = master;
		for (int i = 1; i < nr; i++)
		{
			nodes[i] = new Peer(new Number160(rnd));
			nodes[i].listen(master);
		}
		return nodes;
	}
}
