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
public class ConnectionConfiguration
{
	private int udpLength = 1400;
	private int defaultPort = 7700;
	// idle needs to be larger than timeout for TCP
	private int idleTCPMillis = 6 * 1000;
	//private int timeoutTCPMillis = 3 * 1000;
	private int idleUDPMillis = 3 * 1000;
	private int connectTimeouMillis = 3 * 1000;
	//
	private int maxOutgoingUDP = 200;
	private int maxOutgoingTCP = 100;
	private int maxIncomingThreads = 100;
	//
	private int maxMessageSize = 2 * 1024 * 1024;
	//
	//bandwith shaping in bytes/s
	private long readLimit=0;
	private long writeLimit=0;
	
	public int getIdleTCPMillis()
	{
		return idleTCPMillis;
	}

	public void setIdleTCPMillis(int idleTCPMillis)
	{
		this.idleTCPMillis = idleTCPMillis;
	}

	public int getIdleUDPMillis()
	{
		return idleUDPMillis;
	}

	public void setIdleUDPMillis(int idleUDPMillis)
	{
		this.idleUDPMillis = idleUDPMillis;
	}

	public int getConnectTimeoutMillis()
	{
		return connectTimeouMillis;
	}

	public void setConnectTimeoutMillis(int connectTimeouMillist)
	{
		this.connectTimeouMillis = connectTimeouMillist;
	}

	public void setUdpLength(int udpLength)
	{
		this.udpLength = udpLength;
	}

	public int getUdpLength()
	{
		return udpLength;
	}

	public void setDefaultPort(int defaultPort)
	{
		this.defaultPort = defaultPort;
	}

	public int getDefaultPort()
	{
		return defaultPort;
	}

	public void setMaxOutgoingUDP(int maxOutgoingUDP)
	{
		this.maxOutgoingUDP = maxOutgoingUDP;
	}

	public int getMaxOutgoingUDP()
	{
		return maxOutgoingUDP;
	}

	public void setMaxOutgoingTCP(int maxOutgoingTCP)
	{
		this.maxOutgoingTCP = maxOutgoingTCP;
	}

	public int getMaxOutgoingTCP()
	{
		return maxOutgoingTCP;
	}



	public void setMaxMessageSize(int maxMessageSize)
	{
		this.maxMessageSize = maxMessageSize;
	}

	public int getMaxMessageSize()
	{
		return maxMessageSize;
	}

	public long getReadLimit()
	{
		return readLimit;
	}

	public long getWriteLimit()
	{
		return writeLimit;
	}

	public void setReadLimit(long readLimit)
	{
		this.readLimit = readLimit;
	}

	public void setWriteLimit(long writeLimit)
	{
		this.writeLimit = writeLimit;
	}

	public void setMaxIncomingThreads(int maxIncomingThreads)
	{
		this.maxIncomingThreads = maxIncomingThreads;
	}

	public int getMaxIncomingThreads()
	{
		return maxIncomingThreads;
	}
}