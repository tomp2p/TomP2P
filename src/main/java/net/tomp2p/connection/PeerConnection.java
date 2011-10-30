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
package net.tomp2p.connection;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import net.tomp2p.peers.PeerAddress;

public class PeerConnection 
{
	final private PeerAddress destination;
	final private ChannelCreator channelCreator;
	final private int idleTCPMillis;
	final private Semaphore oneConnection = new Semaphore(1);
	final private AtomicBoolean closed = new AtomicBoolean(false);
	private volatile Runnable closeRunner;
	public PeerConnection(PeerAddress destination, ChannelCreator channelCreator,
			int idleTCPMillis) 
	{
		this.destination = destination;
		this.channelCreator = channelCreator;
		this.idleTCPMillis = idleTCPMillis;
	}
	public void close()
	{
		if(closed.compareAndSet(false, true))
		{
			channelCreator.tryClose(getDestination());
			channelCreator.releaseOpen();
			closeRunner.run();
		}
	}
	public PeerAddress getDestination() 
	{
		return destination;
	}
	public ChannelCreator getChannelCreator() 
	{
		if(closed.get()) {
			throw new RuntimeException("cannot used a closed channel");
		}
		return channelCreator;
	}
	public boolean isClosed()
	{
		return closed.get();
	}
	public int getIdleTCPMillis() 
	{
		return idleTCPMillis;
	}
	public void aquireSingleConnection() throws InterruptedException 
	{
		oneConnection.acquire();
	}
	public void releaseSingleConnection() 
	{
		oneConnection.release();
	}
	public void setCloseRunner(Runnable closeRunner) 
	{
		this.closeRunner=closeRunner;
	}
}
