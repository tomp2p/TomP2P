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
import java.nio.channels.DatagramChannel;
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

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.sctp4nat.connection.SctpConnection;
import net.sctp4nat.core.SctpChannel;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

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

    public static final int CONTENT_TYPE_LENGTH = 8;

    /**
     * 8 x 4 bit.
     */
    public enum Content {
        EMPTY, KEY, MAP_KEY640_DATA, MAP_KEY640_KEYS, SET_KEY640, SET_NEIGHBORS, BYTE_BUFFER, 
        LONG, INTEGER, PUBLIC_KEY_SIGNATURE, SET_TRACKER_DATA, BLOOM_FILTER, MAP_KEY640_BYTE, 
        PUBLIC_KEY, PEER_SOCKET4, PEER_SOCKET6
    };
    
    public enum ProtocolType {UDP, SCTP};

    /**
     * 1 x 4 bit.
     */
	public enum Type {
		/**
		 * <ul>
		 * <li>the normal request</li>
		 * <li>for {@link Commands#NEIGHBOR} means check for put (no digest) for tracker and storage</li>
		 * <li>for TASK is submit new task</li>
		 * <li>for {@link Commands#RELAY} is for setup</li>
		 * <li>for {@link Commands#RCON} means forward reverse connection to unreachable peer</li>
		 * </ul>
		 */
		REQUEST_1,

		/**
		 * <ul>
		 * <li>for {@link Commands#GET} returns the extended digest (hashes of all stored data)</li>
		 * <li>for {@link Commands#PUT}/{@link Commands#ADD}/COMPARE_PUT means protect domain</li>
		 * <li>for {@link Commands#REMOVE} means send back results</li>
		 * <li>for RAW_DATA means serialize object</li>
		 * <li>for {@link Commands#NEIGHBOR} means check for get (with digest) for storage</li>
		 * <li>for TASK is status</li>
		 * <li>for {@link Commands#RELAY} means send piggybacked message</li>
		 * <li>for {@link Commands#RCON} open TCP channel and transmit </li>
		 * </ul>
		 */
		REQUEST_2,

		/**
		 * <ul>
		 * <li>for {@link Commands#GET} returns a Bloom filter</li>
		 * <li>for {@link Commands#PUT} means put if absent</li>
		 * <li>for COMPARE_PUT means partial (partial means that put those data that match compare, ignore
		 * others)</li>
		 * <li>for TASK is send back result</li>
		 * <li>for {@link Commands#RELAY} means update the routing table</li>
		 * <li>for {@link Commands#RCON} use open  to transmit original message</li>
		 * </ul>
		 */
		REQUEST_3,

		/**
		 * <ul>
		 * <li>for {@link Commands#GET} returns a range (min/max)</li>
		 * <li>for {@link Commands#PUT} for PUT means protect domain and put if absent</li>
		 * <li>for COMPARE_PUT means partial and protect domain</li>
		 * <li>for {@link Commands#NEIGHBOR} means check for put (with digest) for task</li>
		 * <li>for {@link Commands#RELAY} fetch the buffer from the relay peer (Android only)</li>
		 * </ul>
		 */
		REQUEST_4,

		/**
		 * <ul>
		 * <li>for {@link Commands#RELAY} means that a late response arrived at the relay peer (slow peers
		 * only)</li>
		 * </ul>
		 */
		ACK,

		/**
		 * <ul>
		 * <li>for {@link Commands#PEX} means fire and forget, coming from mesh</li>
		 * </ul>
		 */
		REQUEST_FF_1,

		/**
		 * <ul>
		 * <li>for {@link Commands#PEX} means fire and forget, coming from primary</li>
		 * </ul>
		 */
		REQUEST_FF_2,
		
		/**
		 * The request was processed and everything is alright
		 */
		OK,
		
		/**
		 * The request was ok, but we don't know the peer, so request an ack
		 */
		UNUSED,
		
		/**
		 * When the called node has {@link PeerAddress} activated, the relay peer returns a partial ok
		 */
		PARTIALLY_OK,
		NOT_FOUND,
		DENIED,
		UNKNOWN_ID,
		EXCEPTION,
		CANCEL
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

    // Payload:
    // we can send 8 types
    private Content[] contentTypes = new Content[CONTENT_TYPE_LENGTH];
    private final transient Queue<MessageContentIndex> contentReferences = new LinkedList<MessageContentIndex>();

    // ********* Here comes the payload objects ************
    // The content lists:
    private List<NeighborSet> neighborsList = null;
    private List<Number160> keyList = null;
    private List<SimpleBloomFilter<Number160>> bloomFilterList = null;
    private List<DataMap> dataMapList = null;
    // private PublicKey publicKey = null; // there can only be one
    private List<Integer> integerList = null;
    private List<Long> longList = null;
    private List<KeyCollection> keyCollectionList = null;
    private List<KeyMap640Keys> keyMap640KeysList = null;
    private List<KeyMapByte> keyMapByteList = null;
    private List<Buffer> bufferList = null;
    private List<TrackerData> trackerDataList = null;
    private List<PublicKey> publicKeyList = null;
    private List<PeerSocket4Address> peerSocket4AddressList = null;
    private List<PeerSocket6Address> peerSocket6AddressList = null;
    private SignatureCodec signatureEncode = null;
    
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
   

    /**
     * Return content types. Content type can be empty if not set
     * 
     * @return Content type 1
     */
    public Content[] contentTypes() {
        return contentTypes;
    }

    /**
     * Convenient method to set content type. Set first content type 1, if this is set (not empty), then set the second
     * one, etc.
     * 
     * @param contentType
     *            The content type to set
     * @return This class
     */
    public Message contentType(final Content contentType) {
        for (int i = 0, reference = 0; i < CONTENT_TYPE_LENGTH; i++) {
            if (contentTypes[i] == null) {
                if (contentType == Content.PUBLIC_KEY_SIGNATURE && i != 0) {
                    throw new IllegalStateException("The public key needs to be the first to be set.");
                }
                contentTypes[i] = contentType;
                contentReferences.add(new MessageContentIndex(reference, contentType));
                return this;
            } else if (contentTypes[i] == contentType) {
                reference++;
            } else if (contentTypes[i] == Content.PUBLIC_KEY_SIGNATURE || contentTypes[i] == Content.PUBLIC_KEY) {
            	//special handling for public key as we store both in the same list
            	if (contentType == Content.PUBLIC_KEY_SIGNATURE || contentType == Content.PUBLIC_KEY) {
            		 reference++;
            	}
            }
        }
        throw new IllegalStateException("Already set 8 content types.");
    }
    
	/**
	 * Restore the content references if only the content types array is
	 * present. The content references are removed when decoding a message. That
	 * means if a message was received it cannot be used a second time as the
	 * content references are not there anymore. This method restores the
	 * content references based on the content types of the message.
	 */
	public void restoreContentReferences() {
		Map<Content, Integer> refs = new HashMap<Content, Integer>(contentTypes.length * 2);
		for (Content contentType : contentTypes) {
			if (contentType == Content.EMPTY || contentType == null) {
				return;
			}
			
			int index = 0;
			if (contentType == Content.PUBLIC_KEY_SIGNATURE || contentType == Content.PUBLIC_KEY) {
				Integer i1 = refs.get(Content.PUBLIC_KEY_SIGNATURE);
				if(i1 != null) {
					index = i1.intValue();
				} else {
					i1 = refs.get(Content.PUBLIC_KEY);
					if(i1 != null) {
						index = i1.intValue();
					}
				}
			}
			
			if (!refs.containsKey(contentType)) {
				refs.put(contentType, index);
			} else {
				index = refs.get(contentType);
			}
			
			contentReferences.add(new MessageContentIndex(index, contentType));
			refs.put(contentType, index + 1);
		}
	}
	
	/**
	 * Restores all buffers such that they can be re-read (e.g. used for encoding). If the message does not
	 * have any buffer, this method does nothing.
	 */
	public void restoreBuffers() {
		for (Buffer buffer : bufferList()) {
			buffer.reset();
		}
	}

    /**
     * Sets or replaces the content type at a specific index.
     * 
     * @param index
     *            The index
     * @param contentType
     *            The content type
     * @return This class
     */
    public Message contentType(final int index, final Content contentType) {
        contentTypes[index] = contentType;
        return this;
    }

    /**
     * Used for deserialization.
     * 
     * @param contentTypes
     *            The content types that were decoded.
     * @return This class
     */
    public Message contentTypes(final Content[] contentTypes) {
        this.contentTypes = contentTypes;
        return this;
    }

    /**
     * @return The serialized content and references to the respective arrays
     */
    public Queue<MessageContentIndex> contentReferences() {
        return contentReferences;
    }

    /**
     * @return True if we have content and not only the header
     */
    public boolean hasContent() {
        return contentReferences.size() > 0 || content;
    }

    /**
     * @param content
     *            We can set this already in the header to know if we have content or not
     * @return This class
     */
    public Message hasContent(final boolean content) {
        this.content = content;
        return this;
    }

    // Types of requests

    /**
     * @return True if this is a request, a regular or a fire and forget
     */
    public boolean isRequest() {
		return type == Type.REQUEST_1 || type == Type.REQUEST_2 || type == Type.REQUEST_3 || type == Type.REQUEST_4
				|| type == Type.REQUEST_FF_1 || type == Type.REQUEST_FF_2;
    }
    
    public boolean isAck() {
		return type == Type.ACK;
    }

    /**
     * @return True if its a fire and forget, that means we don't expect an answer
     */
    public boolean isFireAndForget() {
        return type == Type.REQUEST_FF_1 || type == Type.REQUEST_FF_2;
    }

    /**
     * @return True if the message was ok, or at least send partial data
     */
    public boolean isOk() {
        return type == Type.OK || type == Type.PARTIALLY_OK ;
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
        return type == Type.UNKNOWN_ID || type == Type.EXCEPTION || type == Type.CANCEL;
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
    
    public Message expectDuplicate(boolean expectDuplicate) {
        if (expectDuplicate) {
            options |= 4;
        } else {
            options &= ~4;
        }
        return this;
    }

    public boolean isExpectDuplicate() {
        return (options & 4) > 0;
    }

    // Header data ends here *********************************** static payload starts now

    public Message key(final Number160 key) {
        if (!presetContentTypes) {
            contentType(Content.KEY);
        }
        if (keyList == null) {
            keyList = new ArrayList<Number160>(1);
        }
        keyList.add(key);
        return this;
    }

    public List<Number160> keyList() {
        if (keyList == null) {
            return Collections.emptyList();
        }
        return keyList;
    }

    public Number160 key(final int index) {
        if (keyList == null || index > keyList.size() - 1) {
            return null;
        }
        return keyList.get(index);
    }

    public Message bloomFilter(final SimpleBloomFilter<Number160> bloomFilter) {
        if (!presetContentTypes) {
            contentType(Content.BLOOM_FILTER);
        }
        if (bloomFilterList == null) {
            bloomFilterList = new ArrayList<SimpleBloomFilter<Number160>>(1);
        }
        bloomFilterList.add(bloomFilter);
        return this;
    }

    public List<SimpleBloomFilter<Number160>> bloomFilterList() {
        if (bloomFilterList == null) {
            return Collections.emptyList();
        }
        return bloomFilterList;
    }

    public SimpleBloomFilter<Number160> bloomFilter(final int index) {
        if (bloomFilterList == null || index > bloomFilterList.size() - 1) {
            return null;
        }
        return bloomFilterList.get(index);
    }

    public Message publicKeyAndSign(KeyPair keyPair) {
        if (!presetContentTypes) {
            contentType(Content.PUBLIC_KEY_SIGNATURE);
        }
        publicKey0(keyPair.getPublic());
        this.privateKey = keyPair.getPrivate();
        return this;
    }

    public Message intValue(final int integer) {
        if (!presetContentTypes) {
            contentType(Content.INTEGER);
        }
        if (integerList == null) {
            integerList = new ArrayList<Integer>(1);
        }
        this.integerList.add(integer);
        return this;
    }

    public List<Integer> intList() {
        if (integerList == null) {
            return Collections.emptyList();
        }
        return integerList;
    }

    public Integer intAt(final int index) {
        if (integerList == null || index > integerList.size() - 1) {
            return null;
        }
        return integerList.get(index);
    }

    public Message longValue(long long0) {
        if (!presetContentTypes) {
            contentType(Content.LONG);
        }
        if (longList == null) {
            longList = new ArrayList<Long>(1);
        }
        this.longList.add(long0);
        return this;
    }

    public List<Long> longList() {
        if (longList == null) {
            return Collections.emptyList();
        }
        return longList;
    }

    public Long longAt(int index) {
        if (longList == null || index > longList.size() - 1) {
            return null;
        }
        return longList.get(index);
    }

    public Message neighborsSet(final NeighborSet neighborSet) {
        if (!presetContentTypes) {
            contentType(Content.SET_NEIGHBORS);
        }
        if (neighborsList == null) {
            neighborsList = new ArrayList<NeighborSet>(1);
        }
        this.neighborsList.add(neighborSet);
        return this;
    }

    public List<NeighborSet> neighborsSetList() {
        if (neighborsList == null) {
            return Collections.emptyList();
        }
        return neighborsList;
    }

    public NeighborSet neighborsSet(final int index) {
        if (neighborsList == null || index > neighborsList.size() - 1) {
            return null;
        }
        return neighborsList.get(index);
    }

    public Message setDataMap(final DataMap dataMap) {
        if (!presetContentTypes) {
            contentType(Content.MAP_KEY640_DATA);
        }
        if (dataMapList == null) {
            dataMapList = new ArrayList<DataMap>(1);
        }
        this.dataMapList.add(dataMap);
        return this;
    }

    public List<DataMap> dataMapList() {
        if (dataMapList == null) {
            return Collections.emptyList();
        }
        return dataMapList;
    }

    public DataMap dataMap(final int index) {
        if (dataMapList == null || index > dataMapList.size() - 1) {
            return null;
        }
        return dataMapList.get(index);
    }

    public Message keyCollection(final KeyCollection key) {
        if (!presetContentTypes) {
            contentType(Content.SET_KEY640);
        }
        if (keyCollectionList == null) {
            keyCollectionList = new ArrayList<KeyCollection>(1);
        }
        keyCollectionList.add(key);
        return this;
    }

    public List<KeyCollection> keyCollectionList() {
        if (keyCollectionList == null) {
            return Collections.emptyList();
        }
        return keyCollectionList;
    }

    public KeyCollection keyCollection(final int index) {
        if (keyCollectionList == null || index > keyCollectionList.size() - 1) {
            return null;
        }
        return keyCollectionList.get(index);
    }
    
    public Message keyMap640Keys(final KeyMap640Keys keyMap) {
        if (!presetContentTypes) {
            contentType(Content.MAP_KEY640_KEYS);
        }
        if (keyMap640KeysList == null) {
            keyMap640KeysList = new ArrayList<KeyMap640Keys>(1);
        }
        keyMap640KeysList.add(keyMap);
        return this;
    }

    public List<KeyMap640Keys> keyMap640KeysList() {
        if (keyMap640KeysList == null) {
            return Collections.emptyList();
        }
        return keyMap640KeysList;
    }

    public KeyMap640Keys keyMap640Keys(final int index) {
        if (keyMap640KeysList == null || index > keyMap640KeysList.size() - 1) {
            return null;
        }
        return keyMap640KeysList.get(index);
    }
    
    public Message keyMapByte(final KeyMapByte keyMap) {
        if (!presetContentTypes) {
            contentType(Content.MAP_KEY640_BYTE);
        }
        if (keyMapByteList == null) {
            keyMapByteList = new ArrayList<KeyMapByte>(1);
        }
        keyMapByteList.add(keyMap);
        return this;
    }

    public List<KeyMapByte> keyMapByteList() {
        if (keyMapByteList == null) {
            return Collections.emptyList();
        }
        return keyMapByteList;
    }

    public KeyMapByte keyMapByte(final int index) {
        if (keyMapByteList == null || index > keyMapByteList.size() - 1) {
            return null;
        }
        return keyMapByteList.get(index);
    }

    public Message publicKey(final PublicKey publicKey) {
    	if (!presetContentTypes) {
            contentType(Content.PUBLIC_KEY);
        }
    	if(publicKeyList == null) {
    		publicKeyList = new ArrayList<PublicKey>(1);
    	}
    	publicKeyList.add(publicKey);
        return this;
    }
    
    private Message publicKey0(final PublicKey publicKey) {
    	if(publicKeyList == null) {
    		publicKeyList = new ArrayList<PublicKey>(1);
    	}
    	publicKeyList.add(publicKey);
        return this;
    }

    public List<PublicKey> publicKeyList() {
    	if (publicKeyList == null) {
            return Collections.emptyList();
        }
        return publicKeyList;
    }
    
    public PublicKey publicKey(final int index) {
        if (publicKeyList == null || index > publicKeyList.size() - 1) {
            return null;
        }
        return publicKeyList.get(index);
    }
    
    public Message peerSocketAddress(final PeerSocketAddress peerSocketAddress) {
    	if(peerSocketAddress instanceof PeerSocket4Address) {
    		return peerSocket4Address((PeerSocket4Address)peerSocketAddress);
    	} else {
    		return peerSocket6Address((PeerSocket6Address)peerSocketAddress);
    	}
    }
    
    public List<PeerSocketAddress> peerSocketAddressList() {
    	List<PeerSocketAddress> retVal = new ArrayList<PeerSocketAddress>();
    	retVal.addAll(peerSocket4AddressList());
    	retVal.addAll(peerSocket6AddressList());
    	return retVal;
    }
    
    public Message peerSocket4Address(final PeerSocket4Address peerSocket4Address) {
    	if (!presetContentTypes) {
            contentType(Content.PEER_SOCKET4);
        }
    	if(peerSocket4AddressList == null) {
    		peerSocket4AddressList = new ArrayList<PeerSocket4Address>(1);
    	}
    	peerSocket4AddressList.add(peerSocket4Address);
        return this;
    }

    public List<PeerSocket4Address> peerSocket4AddressList() {
    	if (peerSocket4AddressList == null) {
            return Collections.emptyList();
        }
        return peerSocket4AddressList;
    }
    
    public PeerSocket4Address peerSocket4Address(final int index) {
        if (peerSocket4AddressList == null || index > peerSocket4AddressList.size() - 1) {
            return null;
        }
        return peerSocket4AddressList.get(index);
    }
    
    public Message peerSocket6Address(final PeerSocket6Address peerSocket6Address) {
    	if (!presetContentTypes) {
            contentType(Content.PEER_SOCKET6);
        }
    	if(peerSocket6AddressList == null) {
    		peerSocket6AddressList = new ArrayList<PeerSocket6Address>(1);
    	}
    	peerSocket6AddressList.add(peerSocket6Address);
        return this;
    }

    public List<PeerSocket6Address> peerSocket6AddressList() {
    	if (peerSocket6AddressList == null) {
            return Collections.emptyList();
        }
        return peerSocket6AddressList;
    }
    
    public PeerSocket6Address peerSocket6Address(final int index) {
        if (peerSocket6AddressList == null || index > peerSocket6AddressList.size() - 1) {
            return null;
        }
        return peerSocket6AddressList.get(index);
    }
    
    

    public PrivateKey privateKey() {
        return privateKey;
    }

    public Message buffer(final Buffer byteBuf) {
        if (!presetContentTypes) {
            contentType(Content.BYTE_BUFFER);
        }
        if (bufferList == null) {
            bufferList = new ArrayList<Buffer>(1);
        }
        bufferList.add(byteBuf);
        return this;
    }

    public List<Buffer> bufferList() {
        if (bufferList == null) {
            return Collections.emptyList();
        }
        return bufferList;
    }

    public Buffer buffer(final int index) {
        if (bufferList == null || index > bufferList.size() - 1) {
            return null;
        }
        return bufferList.get(index);
    }
    
    public Message trackerData(final TrackerData trackerData) {
        if (!presetContentTypes) {
            contentType(Content.SET_TRACKER_DATA);
        }
        if (trackerDataList == null) {
            trackerDataList = new ArrayList<TrackerData>(1);
        }
        this.trackerDataList.add(trackerData);
        return this;
    }

    public List<TrackerData> trackerDataList() {
        if (trackerDataList == null) {
            return Collections.emptyList();
        }
        return trackerDataList;
    }

    public TrackerData trackerData(final int index) {
        if (trackerDataList == null || index > trackerDataList.size() - 1) {
            return null;
        }
        return trackerDataList.get(index);
    }

    public Message receivedSignature(SignatureCodec signatureEncode) {
        this.signatureEncode = signatureEncode;
        return this;
    }
    
    public SignatureCodec receivedSignature() {
        return signatureEncode;
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
     * Set if we have a signed message.
     * 
     * @return This class
     */
    public Message setHintSign() {
        sign = true;
        return this;
    }

    /**
     * @return True if message is or should be signed
     */
    public boolean isSign() {
        /*
         * boolean hasType = false; for (Content type : contentTypes) { if (type == Content.PUBLIC_KEY_SIGNATURE) {
         * hasType = true; } }
         */
        return sign || privateKey != null; // || hasType;
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
        message.contentTypes =  this.contentTypes;
        // contentReferences is transient

        // ********* Here comes the payload objects ************
        // The content lists:
        message.neighborsList = this.neighborsList;
        message.keyList = this.keyList;
        message.bloomFilterList = this.bloomFilterList;
        if(dataFilter == null) {
        	message.dataMapList = this.dataMapList;
        } else {
    		message.dataMapList = filter(dataFilter);
        }
        message.integerList = this.integerList;
        message.longList = this.longList;
        message.keyCollectionList = this.keyCollectionList;
        message.keyMap640KeysList = this.keyMap640KeysList;
        message.keyMapByteList = this.keyMapByteList;
        message.bufferList = this.bufferList;
        message.trackerDataList = this.trackerDataList;
        message.publicKeyList = this.publicKeyList;
        message.peerSocket4AddressList = this.peerSocket4AddressList;
        message.peerSocket6AddressList = this.peerSocket6AddressList;
        message.signatureEncode = this.signatureEncode;
        
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
	private List<DataMap> filter(DataFilter dataFilter) {
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
	}
	
	/**
	 * Returns the estimated message size. If the message contains data, a constant value of 1000bytes is added.
	 */
	public int estimateSize() {
		int current = MessageHeaderCodec.HEADER_SIZE_STATIC + sender.size();
		
		if(neighborsList != null) {
			for (NeighborSet neighbors : neighborsList) {
				for (PeerAddress address : neighbors.neighbors()) {
					current += address.size() + 1;
				}
			}
		}
		
		if(keyList != null) {
			current += keyList.size() * Number160.BYTE_ARRAY_SIZE;
		}
		
		if(bloomFilterList != null) {
			for (SimpleBloomFilter<Number160> filter : bloomFilterList) {
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
			for (Buffer buffer : bufferList) {
				current += 4 + buffer.length();
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
		
		/**
		 * Here are the estimations to skip CPU intensive calculations
		 */
		if(dataMapList != null) {
			current += 1000;
		}
		
		if(trackerDataList != null) {
			current += 1000; // estimated size
		}
		
		return current;
	}

	SctpConnection c;
	public void sctpChannel(SctpConnection c) {
		this.c=c;
		
	}
	
	public SctpConnection sctpChannel() {
		return c;
		
	}

	SctpChannel socket;
	public void sctpSocketAdapter(SctpChannel socket) {
		this.socket = socket;
	}
	
	public SctpChannel sctpSocketAdapter() {
		return socket;
	}
}
