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
	// timeouts
	private int idleTCPMillis = 4 * 1000;
	private int idleUDPMillis = 2 * 1000;
	private int connectTimeouMillis = 1 * 1000;
	//
	private int maxOutgoingUDP = 20;
	private int maxOutgoingTCP = 20;
	private int maxIncomingUDP = 500;
	private int maxIncomingTCP = 500;
	private int maxMessageSize = 2 * 1024 * 1024;
	private int maxNrBeforeExclude = 2;

	//public ConfigurationConnection(int idleTCPMillis, int idleUDPMillis, int connectTimeouMillis)
	//{
	//	this.idleTCPMillis = idleTCPMillis;
	//	this.idleUDPMillis = idleUDPMillis;
	//	this.connectTimeouMillis = connectTimeouMillis;
	//}

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

	public void setMaxIncomingUDP(int maxIncomingUDP)
	{
		this.maxIncomingUDP = maxIncomingUDP;
	}

	public int getMaxIncomingUDP()
	{
		return maxIncomingUDP;
	}

	public void setMaxIncomingTCP(int maxIncomingTCP)
	{
		this.maxIncomingTCP = maxIncomingTCP;
	}

	public int getMaxIncomingTCP()
	{
		return maxIncomingTCP;
	}

	public void setMaxMessageSize(int maxMessageSize)
	{
		this.maxMessageSize = maxMessageSize;
	}

	public int getMaxMessageSize()
	{
		return maxMessageSize;
	}

	public void setMaxNrBeforeExclude(int maxNrBeforeExclude)
	{
		this.maxNrBeforeExclude = maxNrBeforeExclude;
	}

	public int getMaxNrBeforeExclude()
	{
		return maxNrBeforeExclude;
	}
}
