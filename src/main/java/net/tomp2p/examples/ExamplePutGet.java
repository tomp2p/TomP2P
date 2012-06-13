package net.tomp2p.examples;

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
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
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
public class ExamplePutGet
{
	final private static Random rnd = new Random(42L);

	public static void main(String[] args) throws Exception
	{
		Peer master = null;
		try
		{
			Peer[] peers = ExampleUtils.createAndAttachNodes(100, 4001);
			ExampleUtils.bootstrap(peers);
			master = peers[0];
			Number160 nr = new Number160(rnd);
			examplePutGet(peers, nr);
			examplePutGetConfig(peers, nr);
			exampleGetBlocking(peers, nr);
			exampleGetNonBlocking(peers, nr);
			Thread.sleep(250);
			exampleAddGet(peers);
		}
		finally
		{
			master.shutdown();
		}
	}

	public static void examplePutGet(Peer[] peers, Number160 nr) throws IOException, ClassNotFoundException
	{
		FutureDHT futureDHT = peers[30].put(nr).setData(new Data("hallo")).build();
		futureDHT.awaitUninterruptibly();
		System.out.println("peer 30 stored [key: "+nr+", value: \"hallo\"]");
		futureDHT = peers[77].get(nr).build();
		futureDHT.awaitUninterruptibly();
		System.out.println("peer 77 got: \"" + futureDHT.getData().getObject() + "\" for the key "+nr);
		// the output should look like this:
		// peer 30 stored [key: 0x8992a603029824e810fd7416d729ef2eb9ad3cfc, value: "hallo"]
		// peer 77 got: "hallo" for the key 0x8992a603029824e810fd7416d729ef2eb9ad3cfc
	}
	
	public static void examplePutGetConfig(Peer[] peers, Number160 nr2) throws IOException, ClassNotFoundException
	{
		Number160 nr = new Number160(rnd);
		FutureDHT futureDHT = peers[30].put(nr).setData(new Number160(11), new Data("hallo")).setDomainKey(Number160.createHash("my_domain")).build();
		futureDHT.awaitUninterruptibly();
		System.out.println("peer 30 stored [key: "+nr+", value: \"hallo\"]");
		//this will fail, since we did not specify the domain
		futureDHT = peers[77].get(nr).setAll().build();
		futureDHT.awaitUninterruptibly();
		System.out.println("peer 77 got: \"" + futureDHT.getData() + "\" for the key "+nr);
		//this will succeed, since we specify the domain
		futureDHT = peers[77].get(nr).setAll().setDomainKey(Number160.createHash("my_domain")).build().awaitUninterruptibly();
		System.out.println("peer 77 got: \"" + futureDHT.getData().getObject() + "\" for the key "+nr);
		// the output should look like this:
		// peer 30 stored [key: 0x8992a603029824e810fd7416d729ef2eb9ad3cfc, value: "hallo"]
		// peer 77 got: "hallo" for the key 0x8992a603029824e810fd7416d729ef2eb9ad3cfc
	}

	private static void exampleAddGet(Peer[] peers) throws IOException, ClassNotFoundException
	{
		Number160 nr = new Number160(rnd);
		String toStore1 = "hallo1";
		String toStore2 = "hallo2";
		Data data1 = new Data(toStore1);
		Data data2 = new Data(toStore2);
		FutureDHT futureDHT = peers[30].add(nr).setData(data1).build();
		futureDHT.awaitUninterruptibly();
		System.out.println("added: " + toStore1 + " (" + futureDHT.isSuccess() + ")");
		futureDHT = peers[50].add(nr).setData(data2).build();
		futureDHT.awaitUninterruptibly();
		System.out.println("added: " + toStore2 + " (" + futureDHT.isSuccess() + ")");
		futureDHT = peers[77].get(nr).setAll().build();
		futureDHT.awaitUninterruptibly();
		System.out.println("size" + futureDHT.getDataMap().size());
		Iterator<Data> iterator = futureDHT.getDataMap().values().iterator();
		System.out.println("got: " + iterator.next().getObject() + " ("
				+ futureDHT.isSuccess() + ")");
		System.out.println("got: " + iterator.next().getObject() + " ("
				+ futureDHT.isSuccess() + ")");
	}
	
	public static void exampleGetBlocking(Peer[] peers,  Number160 nr) throws ClassNotFoundException, IOException
    {
		FutureDHT futureDHT = peers[77].get(nr).build();
		//blocking operation
        futureDHT.awaitUninterruptibly();
        System.out.println("result: "+futureDHT.getData().getObject());
        System.out.println("this may *not* happen before printing the result");
    }
	
	public static void exampleGetNonBlocking(Peer[] peers,  Number160 nr)
	{
		FutureDHT futureDHT = peers[77].get(nr).build();
        //non-blocking operation
        futureDHT.addListener(new BaseFutureAdapter<FutureDHT>() 
        {
        	@Override
            public void operationComplete(FutureDHT future) throws Exception 
            {
        		System.out.println("result: "+future.getData().getObject());
            }
        });
        System.out.println("this may happen before printing the result");
    }
}

