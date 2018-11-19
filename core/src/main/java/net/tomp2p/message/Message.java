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
package net.tomp2p.message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

import lombok.experimental.Accessors;
import net.tomp2p.crypto.Crypto;
import net.tomp2p.peers.Number256;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import org.whispersystems.curve25519.Curve25519;
import org.whispersystems.curve25519.Curve25519KeyPair;

/**
 * The message is in binary format in TomP2P. It has several header and payload fields. Since
 * we do the serialization/encoding manually, we do not need a serialization field.
 * 
 * @author Thomas Bocek
 */
@Accessors(fluent = true, chain = true)
public class Message {

    // used for creating random message id
    private static final transient Random RND = new Random();



    public enum ProtocolType {UDP, KCP, UNUSED1, UNUSED2}; //max 4

    /**
     * 1 x 4 bit.
     */
	public enum Type {
		REQUEST,
        UNUSED,
		ACK,
		OK,
		NOT_FOUND,
		DENIED,
		UNKNOWN_ID,
		EXCEPTION
	};

    // Header:
    private int messageId;
    private int version;
    private Type type;
    private ProtocolType protocolType = ProtocolType.UDP;
    private byte command;
    private PeerAddress sender;
    private PeerAddress recipient;
    private int options = 0;

    private ByteBuffer payload = null;
    
    // this will not be transferred, status variables
    private transient boolean presetContentTypes = false;
    private transient PrivateKey privateKey;
    private transient InetSocketAddress senderSocket;
    private transient InetSocketAddress recipientSocket;
    //@Getter @Setter private transient DatagramChannel datagramChannel;
    private transient boolean done = false;
    private transient boolean sign = false;
    private transient boolean content = false;
    private transient boolean sendSelf = false;

    private transient byte[] ephemeralPublicKey = null;
    private transient Curve25519KeyPair keyPair = null;

    public Message generateEphemeralKeyPair() {
        this.keyPair = Crypto.cipher.generateKeyPair();
        return this;
    }

    public Curve25519KeyPair ephemeralKeyPair() {
        return keyPair;
    }

    public Message ephemeralKeyPair(Curve25519KeyPair keyPair) {
        this.keyPair = keyPair;
        return this;
    }

    public byte[] ephemeralPublicKey() {
        return ephemeralPublicKey;
    }

    public Message ephemeralPublicKey(byte[] ephemeralPublicKey) {
        this.ephemeralPublicKey = ephemeralPublicKey;
        return this;
    }

    /**
     * Creates message with a random ID.
     */
    public Message() {
        this.messageId = RND.nextInt();
    }

    /**
     * Randomly generated message ID.
     * 
     * @return message Id
     */
    public int messageId() {
        return messageId;
    }

    /**
     * For deserialization, we need to set the id.
     * 
     * @param messageId
     *            The message Id
     * @return This class
     */
    public Message messageId(final int messageId) {
        this.messageId = messageId;
        return this;
    }

    /**
     * Returns the version, which is 32bit. Each application can choose and version to not intefere with other
     * applications
     * 
     * @return The application version that uses this P2P framework
     */
    public int version() {
        return version;
    }

    /**
     * For deserialization.
     * 
     * @param version
     *            The 24bit version
     * @return This class
     */
    public Message version(final int version) {
        this.version = version;
        return this;
    }

    /**
     * Determines if its a request or command reply, and what kind of reply (error, warning states).
     * 
     * @return Type of the message
     */
    public Type type() {
        return type;
    }

    /**
     * Set the message type. Either its a request or reply (with error and warning codes).
     * 
     * @param type
     *            Type of the message
     * @return This class
     */
    public Message type(final Type type) {
        this.type = type;
        return this;
    }
    
    public ProtocolType protocolType() {
        return protocolType;
    }
    
    public Message protocolType(final ProtocolType protocolType) {
        this.protocolType = protocolType;
        return this;
    }

    /**
     * Command of the message, such as GET, PING, etc.
     * 
     * @return Command
     */
    public byte command() {
        return command;
    }

    /**
     * Command of the message, such as GET, PING, etc.
     * 
     * @param command
     *            Command
     * @return This class
     */
    public Message command(final byte command) {
        this.command = command;
        return this;
    }

    /**
     * The ID of the sender. Note that the IP is set via the socket.
     * 
     * @return The ID of the sender.
     */
    public PeerAddress sender() {
        return sender;
    }

    /**
     * The ID of the sender. The IP of the sender will *not* be transferred, as this information is in the IP packet.
     * 
     * @param sender
     *            The ID of the sender.
     * @return This class
     */
    public Message sender(final PeerAddress sender) {
        this.sender = sender;
        return this;
    }

    /**
     * The ID of the recipient. Note that the IP is set via the socket.
     * 
     * @return The ID of the recipient
     */
    public PeerAddress recipient() {
        return recipient;
    }

    /**
     * Set the ID of the recipient. The IP is used to connect to the recipient, but the IP is *not* transferred.
     * 
     * @param recipient
     *            The ID of the recipient
     * @return This class
     */
    public Message recipient(final PeerAddress recipient) {
        this.recipient = recipient;
        return this;
    }

    // Types of requests

    /**
     * @return True if this is a request, a regular or a fire and forget
     */
    public boolean isRequest() {
		return type == Type.REQUEST;
    }
    
    public boolean isAck() {
		return type == Type.ACK;
    }

    /**
     * @return True if the message was ok, or at least send partial data
     */
    public boolean isOk() {
        return type == Type.OK;
    }

    /**
     * @return True if the message arrived, but data was not found or access was denied
     */
    public boolean isNotOk() {
        return type == Type.NOT_FOUND || type == Type.DENIED;
    }

    /**
     * @return True if the message contained an unexpected error or behavior
     */
    public boolean isError() {
        return isError(type);
    }

    /**
     * @param type
     *            The type to check
     * @return True if the message contained an unexpected error or behavior
     */
    public static boolean isError(final Type type) {
        return type == Type.UNKNOWN_ID || type == Type.EXCEPTION;
    }

    /**
     * @param options
     *            The option from the last byte of the header
     * @return This class
     */
    public Message options(final int options) {
        this.options = options;
        return this;
    }

    /**
     * @return The options from the last byte of the header
     */
    public int options() {
        return options;
    }

    /**
     * 
     * @param isKeepAlive
     *            True if the connection should remain open. We need to announce this in the header, as otherwise the
     *            other end has an idle handler that will close the connection.
     * @return This class
     */
    public Message keepAlive(final boolean isKeepAlive) {
        if (isKeepAlive) {
            options |= 1;
        } else {
            options &= ~1;
        }
        return this;
    }

    /**
     * @return True if this message was sent on a connection that should be kept alive
     */
    public boolean isKeepAlive() {
        return (options & 1) > 0;
    }
    
    public Message verified() {
        return verified(true);
    }

    public Message verified(boolean verified) {
        if (verified) {
            options |= 2;
        } else {
            options &= ~2;
        }
        return this;
    }

    public boolean isVerified() {
        return (options & 2) > 0;
    }
    
    public Message kcp(boolean kcp) {
        if (kcp) {
            options |= 4;
        } else {
            options &= ~4;
        }
        return this;
    }

    public boolean kcp() {
        return (options & 4) > 0;
    }
    
    public Message target(boolean target) {
        if (target) {
            options |= 8;
        } else {
            options &= ~8;
        }
        return this;
    }

    public boolean target() {
        return (options & 8) > 0;
    }
    

    
    

    // Header data ends here *********************************** static payload starts now

    public Message payload(final ByteBuffer payload) {
        this.payload = payload;
        return this;
    }

    public ByteBuffer payload() {
        return payload;
    }


    

    
    //*************************************** End of content payload ********************

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("msgid=");
        return sb
        		.append(messageId())
        		.append(',')
        		.append(RPC.Commands.find(command).toString())
        		.append('/')
        		.append(type)
        		.append(",s=")
        		.append(sender)
        		.append(",r=")
        		.append(recipient)
        		.append(",o=")
        		.append(options)
        		.append(isVerified() ? "✓": "❌")
        		.toString();        
    }

    // *************************** No transferable objects here *********************************

    /**
     * If we are setting values from the decoder, then the content type is already set.
     * 
     * @param presetContentTypes
     *            True if the content type is already set.
     * @return This class
     */
    public Message presetContentTypes(final boolean presetContentTypes) {
        this.presetContentTypes = presetContentTypes;
        return this;
    }

    /**
     * Store the sender of the packet. This is needed for UDP traffic.
     * 
     * @param senderSocket
     *            The sender as we saw it on the interface
     * @return This class
     */
    public Message senderSocket(final InetSocketAddress senderSocket) {
        this.senderSocket = senderSocket;
        return this;
    }

    /**
     * @return The sender of the packet. This is needed for UDP traffic.
     */
    public InetSocketAddress senderSocket() {
        return senderSocket;
    }
    
    /**
     * Store the recipient of the packet. This is needed for UDP (especially broadcast) packets
     * @param recipientSocket The recipient as we saw it on the interface
     * @return This class
     */
    public Message recipientSocket(InetSocketAddress recipientSocket) {
        this.recipientSocket = recipientSocket;
        return this;
    }
    
    /**
     * @return The recipient as we saw it on the interface. This is needed for UDP (especially broadcast) packets
     */
    public InetSocketAddress recipientSocket() {
        return recipientSocket;
    }


    /**
     * @param done
     *            True if message decoding or encoding is done
     * @return This class
     */
    public Message setDone(final boolean done) {
        this.done = done;
        return this;
    }
    
    /**
     * Set done to true if message decoding or encoding is done.
     * @return This class
     */
    public Message setDone() {
        return setDone(true);
    }

    /**
     * @return True if message decoding or encoding is done
     */
    public boolean isDone() {
        return done;
    }
    
    public Message sendSelf(final boolean sendSelf) {
        this.sendSelf = sendSelf;
        return this;
    }
    
    public Message sendSelf() {
        return sendSelf(true);
    }

    public boolean isSendSelf() {
        return sendSelf;
    }
    
    public Message duplicate() {
    	return duplicate(null);
    }
    
    public Message duplicate(DataFilter dataFilter) {
    	Message message = new Message();
    	
    	// Header
        message.messageId = this.messageId;
        message.version = this.version;
        message.type = this.type;
        message.protocolType = this.protocolType;
        message.command = this.command;
        message.sender = this.sender;
        message.recipient = this.recipient;
        // recipientRelay is not transferred
        message.options = this.options;

        // Payload
        message.payload =  this.payload;
        
        // these are transient, copy anyway
        
        message.presetContentTypes = presetContentTypes; 

        message.privateKey = this.privateKey;
        message.senderSocket = this.senderSocket;
        message.recipientSocket = this.recipientSocket;
        message.done = this.done;
        message.sign = this.sign;
        message.content = this.content;
        message.sendSelf = this.sendSelf;
        
        return message;
    }

	/**
	 * Change the data and make a shallow copy of the data. This means fields
	 * such as TTL or other are copied, the underlying buffer remains the same
	 * 
	 * @param dataFilter
	 *            The filter that will be applied on each data item
	 * @return The filtered data
	 */
	/*private List<DataMap> filter(DataFilter dataFilter) {
		final List<DataMap> dataMapListCopy = new ArrayList<DataMap>(this.dataMapList().size());
		for (DataMap dataMap : this.dataMapList()) {
			final NavigableMap<Number640, Data> dataMapCopy = new TreeMap<Number640, Data>();
			for (Map.Entry<Number640, Data> entry : dataMap.dataMap()
					.entrySet()) {
				Data filteredData = dataFilter.filter(entry.getValue(), dataMap.isConvertMeta(), !isRequest());
				dataMapCopy.put(entry.getKey(), filteredData);
			}
			dataMapListCopy.add(new DataMap(dataMapCopy, dataMap.isConvertMeta()));
		}
		return dataMapListCopy;
	}*/
	
	/**
	 * Returns the estimated message size. If the message contains data, a constant value of 1000bytes is added.
	 */
	/*public int estimateSize() {
		int current = Codec.HEADER_SIZE_STATIC + sender.size();
		
		if(neighborsList != null) {
			for (NeighborSet neighbors : neighborsList) {
				for (PeerAddress address : neighbors.neighbors()) {
					current += address.size() + 1;
				}
			}
		}
		
		if(keyList != null) {
			current += keyList.size() * Number256.BYTE_ARRAY_SIZE;
		}
		
		if(bloomFilterList != null) {
			for (SimpleBloomFilter<Number256> filter : bloomFilterList) {
				current += filter.size();
			}
		}
		
		if(integerList != null) {
			current += integerList.size() * 4;
		}
		
		if(longList != null) {
			current += longList.size() * 8;
		}
		
		if(keyCollectionList != null) {
			for (KeyCollection coll : keyCollectionList) {
				current += 4 + coll.size() * Number640.BYTE_ARRAY_SIZE;
			}
		}
		
		if(keyMap640KeysList != null) {
			for (KeyMap640Keys keys : keyMap640KeysList) {
				current += 4 + keys.size() * Number640.BYTE_ARRAY_SIZE;
			}
		}
		
		if(keyMapByteList != null) {
			for (KeyMapByte keys : keyMapByteList) {
				current += 4 + keys.size();
			}
		}
		
		if(bufferList != null) {
			for (byte[] buffer : bufferList) {
				current += 4 + buffer.length;
			}
		}
		
		if(publicKeyList != null) {
			for (PublicKey key : publicKeyList) {
				current += key.getEncoded().length;
			}
		}
		
		if(peerSocket4AddressList != null) {
			current += (peerSocket4AddressList.size() * PeerSocket4Address.SIZE);
		}
		
		if(peerSocket6AddressList != null) {
			current += (peerSocket6AddressList.size() * PeerSocket6Address.SIZE);
		}
		
		if(signatureEncode != null) {
			current += signatureEncode.signatureSize();
		}
		

		// Here are the estimations to skip CPU intensive calculations
		if(dataMapList != null) {
			current += 1000;
		}
		
		if(trackerDataList != null) {
			current += 1000; // estimated size
		}
		
		return current;
	}*/
}
