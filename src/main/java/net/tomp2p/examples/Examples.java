/*
 * This file is part of TomP2P.
 * 
 * TomP2P is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * TomP2P is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with TomP2P. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * 
 * TomP2P - A distributed multi map
 * 
 * TomP2P is a Java-based P2P network, which implements a distributed multi map.
 * It implements besides DHT operations (get and put), also add, remove and size
 * operations and further extensions. TomP2P was developed at the University of
 * Zurich, IFI, Communication Systems Group.
 * 
 * Copyright (C) 2009 University of Zurich, Thomas Bocek
 * 
 * @author Thomas Bocek
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
		Bindings b = new Bindings(false);
		b.addInterface("eth0");
		b.addProtocol(Protocol.IPv4);
		b.setOutsideAddress(Inet4Address.getByName("127.0.0.1"), 4001, 4001);
		System.out.println("Listening to: " + b.discoverLocalInterfaces());
		master.listen(4001, 4001, b);
		System.out.println("address visible to outside is " + master.getPeerAddress());
		master.shutdown();
	}

	private static void bootstrap(Peer master, Peer[] nodes)
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

	private static Peer[] createAndAttachNodes(Peer master, int nr) throws Exception
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
