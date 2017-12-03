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

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.tomp2p.p2p.PeerBuilder;

import java.net.InetAddress;

/**
 * The class that stores the limits for the resource reservation.
 * 
 * @author Thomas Bocek
 * 
 */

@Getter @Setter @Accessors(fluent = true)
public class ChannelClientConfiguration {	
    private int maxPermitsUDP = PeerBuilder.MAX_PERMITS_TCP;
    private int maxPermitsTCP = PeerBuilder.MAX_PERMITS_UDP;
    private SignatureFactory signatureFactory = new DSASignatureFactory();
    private Bindings bindings = new Bindings();
    private InetAddress fromAddress = null;
}