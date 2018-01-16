/*
 * Copyright 2013 Thomas Bocek
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

import java.net.InetAddress;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;


/**
 * The configuration for the server.
 * 
 * @author Thomas Bocek
 * 
 */
@Getter @Setter @Accessors(fluent = true)
public class ChannelServerConfiguration {

	private boolean behindFirewall = false;
	private boolean disableBind = false;

	private int idleSCTPMillis = ConnectionBean.DEFAULT_TCP_IDLE_MILLIS;
	private int idleUDPMillis = ConnectionBean.DEFAULT_UDP_IDLE_MILLIS;
	private int heartBeatSeconds = ConnectionBean.DEFAULT_HEARTBEAT_SECONDS;

    //interface bindings
	private Bindings bindings = new Bindings();
	private SignatureFactory signatureFactory = new DSASignatureFactory();
	private Ports portsForwarding = new Ports(Ports.DEFAULT_PORT);
	private Ports ports = new Ports(Ports.DEFAULT_PORT);
	private int maxUDPIncomingConnections = 1000;
	private InetAddress fromAddress = null;
	
	//private SctpDataCallback sctpCallback = null;
}
