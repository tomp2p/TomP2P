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
package net.tomp2p.connection;
import net.tomp2p.p2p.Scheduler;

import org.jboss.netty.channel.group.ChannelGroup;

public class ConnectionBean
{
	final private int p2pID;
	final private ChannelGroup channelGroup;
	final private DispatcherReply dispatcherRequest;
	final private Sender sender;
	final private Scheduler scheduler;
	final private ConnectionReservation reservation;
	final private ConnectionConfiguration configuration;
	//
	public ConnectionBean(int p2pID, DispatcherReply dispatcherRequest, Sender sender, ChannelGroup channelGroup, Scheduler scheduler, ConnectionReservation reservation, ConnectionConfiguration configuration)
	{
		this.p2pID = p2pID;
		this.channelGroup=channelGroup;
		this.dispatcherRequest = dispatcherRequest;
		this.sender = sender;
		this.scheduler = scheduler;
		this.reservation = reservation;
		this.configuration = configuration;
	}

	public Sender getSender()
	{
		return sender;
	}

	public DispatcherReply getDispatcherRequest()
	{
		return dispatcherRequest;
	}

	public int getP2PID()
	{
		return p2pID;
	}

	public ChannelGroup getChannelGroup()
	{
		return channelGroup;
	}

	public Scheduler getScheduler() 
	{
		return scheduler;
	}

	public ConnectionReservation getReservation()
	{
		return reservation;
	}

	public ConnectionConfiguration getConfiguration() {
		return configuration;
	}
}
