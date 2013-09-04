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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SimpleBloomFilter;

/**
 * The message is in binary format in TomP2P. It is defined as follows and has several header and payload fields. Since
 * we do the serialization manually, we do not need a serialization field.
 * 
 * @author Thomas Bocek
 */
public class Message2 {

    // used for creating random message id
    private static final Random RND = new Random();

    public static final int CONTENT_TYPE_LENGTH = 8;

    /**
     * 8 x 4 bit.
     */
    public enum Content {
        EMPTY, KEY, MAP_KEY480_DATA, MAP_KEY480_KEY, SET_KEY480, SET_NEIGHBORS, BYTE_BUFFER, 
        LONG, INTEGER, PUBLIC_KEY_SIGNATURE, SET_TRACKER_DATA, BLOOM_FILTER, USER1, USER2, USER3, USER4
    };

    /**
     * 1 x 4 bit.
     */
    public enum Type {
        // REQUEST_1 is the normal request
        // REQUEST_2 for GET returns the extended digest (hashes of all stored
        // data)
        // REQUEST_3 for GET returns a Bloom filter
        // REQUEST_4 for GET returns a range (min/max)
        // REQUEST_2 for PUT/ADD/COMPARE_PUT means protect domain
        // REQUEST_3 for PUT means put if absent
        // REQUEST_3 for COMPARE_PUT means partial (partial means that put those
        // data that match compare, ignore others)
        // REQUEST_4 for PUT means protect domain and put if absent
        // REQUEST_4 for COMPARE_PUT means partial and protect domain
        // REQUEST_2 for REMOVE means send back results
        // REQUEST_2 for RAW_DATA means serialize object
        // *** NEIGHBORS has four different cases
        // REQUEST_1 for NEIGHBORS means check for put (no digest) for tracker
        // and storage
        // REQUEST_2 for NEIGHBORS means check for get (with digest) for storage
        // REQUEST_3 for NEIGHBORS means check for get (with digest) for tracker
        // REQUEST_4 for NEIGHBORS means check for put (with digest) for task
        // REQUEST_FF_1 for PEX means fire and forget, coming from mesh
        // REQUEST_FF_1 for PEX means fire and forget, coming from primary
        // REQUEST_1 for TASK is submit new task
        // REQUEST_2 for TASK is status
        // REQUEST_3 for TASK is send back result
        REQUEST_1, REQUEST_2, REQUEST_3, REQUEST_4, REQUEST_FF_1, REQUEST_FF_2, OK, 
        PARTIALLY_OK, NOT_FOUND, DENIED, UNKNOWN_ID, EXCEPTION, CANCEL, USER1, USER2
    };

    // Header:
    private int messageId;
    private int version;
    private Type type;
    // commands so far:
    // 0: PING
    // 1: PUT
    // 2: GET
    // 3: ADD
    // 4: REMOVE
    // 5: NEIGHBORS
    // 6: QUIT
    // 7: DIRECT_DATA
    // 8: TRACKER_ADD
    // 9: TRACKER_GET
    // 10: PEX
    // 11: TASK
    // 12: BROADCAST_DATA
    private byte command;
    private PeerAddress sender;
    private PeerAddress recipient;
    private int options = 0;

    // Payload:
    // we can send 8 types
    private Content[] contentTypes = new Content[CONTENT_TYPE_LENGTH];
    private final Queue<NumberType> contentRefencencs = new LinkedList<NumberType>();

    // ********* Here comes the payload objects ************
    // The content lists:
    private List<NeighborSet> neighborsList = null;
    private List<Number160> keyList = null;
    private List<SimpleBloomFilter<Number160>> bloomFilterList = null;
    private List<DataMap> dataMapList = null;
    // private PublicKey publicKey = null; // there can only be one
    private AtomicReference<PublicKey> publicKeyReference = null;
    private List<Integer> integerList = null;
    private List<Long> longList = null;
    private List<Keys> keysList = null;
    private List<KeysMap> keysMapList = null;
    private List<Buffer> bufferList = null;
    private List<TrackerData> trackerDataList = null;

    // this will not be transferred, status variables
    private transient boolean presetContentTypes = false;
    private transient PrivateKey privateKey;
    private transient InetSocketAddress senderSocket;
    private transient InetSocketAddress recipientSocket;
    private transient boolean udp = false;
    private transient boolean done = false;
    private transient boolean sign = false;
    private transient boolean content = false;
    private transient Signature signature = null;
    private transient SHA1Signature signatureEncode = null;
    private transient PublicKey receivedPublicKey = null;

    /**
     * Creates message with a random ID.
     */
    public Message2() {
        this.messageId = RND.nextInt();
    }

    /**
     * Randomly generated message ID.
     * 
     * @return message Id
     */
    public int getMessageId() {
        return messageId;
    }

    /**
     * For deserialization, we need to set the id.
     * 
     * @param messageId
     *            The message Id
     * @return This class
     */
    public Message2 setMessageId(final int messageId) {
        this.messageId = messageId;
        return this;
    }

    /**
     * Returns the version, which is 32bit. Each application can choose and version to not intefere with other
     * applications
     * 
     * @return The application version that uses this P2P framework
     */
    public int getVersion() {
        return version;
    }

    /**
     * For deserialization.
     * 
     * @param version
     *            The 24bit version
     * @return This class
     */
    public Message2 setVersion(final int version) {
        this.version = version;
        return this;
    }

    /**
     * Determines if its a request oCommandr reply, and what kind of reply (error, warning states).
     * 
     * @return Type of the message
     */
    public Type getType() {
        return type;
    }

    /**
     * Set the message type. Either its a request or reply (with error and warning codes).
     * 
     * @param type
     *            Type of the message
     * @return This class
     */
    public Message2 setType(final Type type) {
        this.type = type;
        return this;
    }

    /**
     * Command of the message, such as GET, PING, etc.
     * 
     * @return Command
     */
    public byte getCommand() {
        return command;
    }

    /**
     * Command of the message, such as GET, PING, etc.
     * 
     * @param command
     *            Command
     * @return This class
     */
    public Message2 setCommand(final byte command) {
        this.command = command;
        return this;
    }

    /**
     * The ID of the sender. Note that the IP is set via the socket.
     * 
     * @return The ID of the sender.
     */
    public PeerAddress getSender() {
        return sender;
    }

    /**
     * The ID of the sender. The IP of the sender will *not* be transferred, as this information is in the IP packet.
     * 
     * @param sender
     *            The ID of the sender.
     * @return This class
     */
    public Message2 setSender(final PeerAddress sender) {
        this.sender = sender;
        return this;
    }

    /**
     * The ID of the recipient. Note that the IP is set via the socket.
     * 
     * @return The ID of the recipient
     */
    public PeerAddress getRecipient() {
        return recipient;
    }

    /**
     * Set the ID of the recipient. The IP is used to connect to the recipient, but the IP is *not* transferred.
     * 
     * @param recipient
     *            The ID of the recipient
     * @return This class
     */
    public Message2 setRecipient(final PeerAddress recipient) {
        this.recipient = recipient;
        return this;
    }

    /**
     * Return content types. Content type can be empty if not set
     * 
     * @return Content type 1
     */
    public Content[] getContentTypes() {
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
    public Message2 setContentType(final Content contentType) {
        for (int i = 0, reference = 0; i < CONTENT_TYPE_LENGTH; i++) {
            if (contentTypes[i] == null) {
                if (contentType == Content.PUBLIC_KEY_SIGNATURE && i != 0) {
                    throw new IllegalStateException("The public key needs to be the first to be set");
                }
                contentTypes[i] = contentType;
                contentRefencencs.add(new NumberType(reference, contentType));
                return this;
            }
            if (contentTypes[i] == contentType) {
                reference++;
            }
        }
        throw new IllegalStateException("Already set 8 content types");
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
    public Message2 setContentType(final int index, final Content contentType) {
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
    public Message2 setContentTypes(final Content[] contentTypes) {
        this.contentTypes = contentTypes;
        return this;
    }

    /**
     * @return The serialized content and references to the respective arrays
     */
    public Queue<NumberType> contentRefencencs() {
        return contentRefencencs;
    }

    /**
     * @return True if we have content and not only the header
     */
    public boolean hasContent() {
        return contentRefencencs.size() > 0 || content;
    }

    /**
     * @param content
     *            We can set this already in the header to know if we have content or not
     * @return This class
     */
    public Message2 hasContent(final boolean content) {
        this.content = content;
        return this;
    }

    // Types of requests

    /**
     * @return True if this is a request, a regural or a fire and forget
     */
    public boolean isRequest() {
        return type == Type.REQUEST_1 || type == Type.REQUEST_2 || type == Type.REQUEST_3
                || type == Type.REQUEST_4 || type == Type.REQUEST_FF_1 || type == Type.REQUEST_FF_2;
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
        return type == Type.OK || type == Type.PARTIALLY_OK;
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
    public Message2 setOptions(final int options) {
        this.options = options;
        return this;
    }

    /**
     * @return The option from the last byte of the header
     */
    public int getOptions() {
        return options;
    }

    /**
     * 
     * @param isKeepAlive
     *            True if the connection should remain open. We need to announce this in the header, as otherwise the
     *            other end has an idle handler that will close the connection.
     * @return This class
     */
    public Message2 setKeepAlive(final boolean isKeepAlive) {
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
    
    public Message2 setStreaming() {
        return streaming(true);
    }

    public Message2 streaming(boolean streaming) {
        if (streaming) {
            options |= 2;
        } else {
            options &= ~2;
        }
        return this;
    }

    public boolean isStreaming() {
        return (options & 2) > 0;
    }

    // Header data ends here *********************************** static payload starts now

    public Message2 setKey(final Number160 key) {
        if (!presetContentTypes) {
            setContentType(Content.KEY);
        }
        if (keyList == null) {
            keyList = new ArrayList<Number160>(1);
        }
        keyList.add(key);
        return this;
    }

    public List<Number160> getKeyList() {
        if (keyList == null) {
            return Collections.emptyList();
        }
        return keyList;
    }

    public Number160 getKey(final int index) {
        if (keyList == null || index > keyList.size() - 1) {
            return null;
        }
        return keyList.get(index);
    }

    public Message2 setBloomFilter(final SimpleBloomFilter<Number160> bloomFilter) {
        if (!presetContentTypes) {
            setContentType(Content.BLOOM_FILTER);
        }
        if (bloomFilterList == null) {
            bloomFilterList = new ArrayList<SimpleBloomFilter<Number160>>(1);
        }
        bloomFilterList.add(bloomFilter);
        return this;
    }

    public List<SimpleBloomFilter<Number160>> getBloomFilterList() {
        if (bloomFilterList == null) {
            return Collections.emptyList();
        }
        return bloomFilterList;
    }

    public SimpleBloomFilter<Number160> getBloomFilter(final int index) {
        if (bloomFilterList == null || index > bloomFilterList.size() - 1) {
            return null;
        }
        return bloomFilterList.get(index);
    }

    public Message2 setPublicKeyAndSign(KeyPair keyPair) {
        if (!presetContentTypes) {
            setContentType(Content.PUBLIC_KEY_SIGNATURE);
        }
        this.publicKeyReference = new AtomicReference<PublicKey>(keyPair.getPublic());
        this.privateKey = keyPair.getPrivate();
        return this;
    }

    public Message2 setInteger(final int integer) {
        if (!presetContentTypes) {
            setContentType(Content.INTEGER);
        }
        if (integerList == null) {
            integerList = new ArrayList<Integer>(1);
        }
        this.integerList.add(integer);
        return this;
    }

    public List<Integer> getIntegerList() {
        if (integerList == null) {
            return Collections.emptyList();
        }
        return integerList;
    }

    public Integer getInteger(final int index) {
        if (integerList == null || index > integerList.size() - 1) {
            return null;
        }
        return integerList.get(index);
    }

    public Message2 setLong(long long0) {
        if (!presetContentTypes) {
            setContentType(Content.LONG);
        }
        if (longList == null) {
            longList = new ArrayList<Long>(1);
        }
        this.longList.add(long0);
        return this;
    }

    public List<Long> getLongList() {
        if (longList == null) {
            return Collections.emptyList();
        }
        return longList;
    }

    public Long getLong(int index) {
        if (longList == null || index > longList.size() - 1) {
            return null;
        }
        return longList.get(index);
    }

    public Message2 setNeighborsSet(final NeighborSet neighborSet) {
        if (!presetContentTypes) {
            setContentType(Content.SET_NEIGHBORS);
        }
        if (neighborsList == null) {
            neighborsList = new ArrayList<NeighborSet>(1);
        }
        this.neighborsList.add(neighborSet);
        return this;
    }

    public List<NeighborSet> getNeighborsSetList() {
        if (neighborsList == null) {
            return Collections.emptyList();
        }
        return neighborsList;
    }

    public NeighborSet getNeighborsSet(final int index) {
        if (neighborsList == null || index > neighborsList.size() - 1) {
            return null;
        }
        return neighborsList.get(index);
    }

    public Message2 setDataMap(final DataMap dataMap) {
        if (!presetContentTypes) {
            setContentType(Content.MAP_KEY480_DATA);
        }
        if (dataMapList == null) {
            dataMapList = new ArrayList<DataMap>(1);
        }
        this.dataMapList.add(dataMap);
        return this;
    }

    public List<DataMap> getDataMapList() {
        if (dataMapList == null) {
            return Collections.emptyList();
        }
        return dataMapList;
    }

    public DataMap getDataMap(final int index) {
        if (dataMapList == null || index > dataMapList.size() - 1) {
            return null;
        }
        return dataMapList.get(index);
    }

    public Message2 setKeys(final Keys key) {
        if (!presetContentTypes) {
            setContentType(Content.SET_KEY480);
        }
        if (keysList == null) {
            keysList = new ArrayList<Keys>(1);
        }
        keysList.add(key);
        return this;
    }

    public List<Keys> getKeysList() {
        if (keysList == null) {
            return Collections.emptyList();
        }
        return keysList;
    }

    public Keys getKeys(final int index) {
        if (keysList == null || index > keysList.size() - 1) {
            return null;
        }
        return keysList.get(index);
    }

    public Message2 setKeysMap(final KeysMap keyMap) {
        if (!presetContentTypes) {
            setContentType(Content.MAP_KEY480_KEY);
        }
        if (keysMapList == null) {
            keysMapList = new ArrayList<KeysMap>(1);
        }
        keysMapList.add(keyMap);
        return this;
    }

    public List<KeysMap> getKeysMapList() {
        if (keysMapList == null) {
            return Collections.emptyList();
        }
        return keysMapList;
    }

    public KeysMap getKeysMap(final int index) {
        if (keysMapList == null || index > keysMapList.size() - 1) {
            return null;
        }
        return keysMapList.get(index);
    }

    public Message2 setPublicKey(final PublicKey publicKey) {
        publicKeyReference();
        publicKeyReference.set(publicKey);
        return this;
    }

    public PublicKey getPublicKey() {
        if (publicKeyReference == null) {
            return null;
        }
        return publicKeyReference.get();
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    public Message2 setBuffer(final Buffer byteBuf) {
        if (!presetContentTypes) {
            setContentType(Content.BYTE_BUFFER);
        }
        if (bufferList == null) {
            bufferList = new ArrayList<Buffer>(1);
        }
        bufferList.add(byteBuf);
        return this;
    }

    public List<Buffer> getBufferList() {
        if (bufferList == null) {
            return Collections.emptyList();
        }
        return bufferList;
    }

    public Buffer getBuffer(final int index) {
        if (bufferList == null || index > bufferList.size() - 1) {
            return null;
        }
        return bufferList.get(index);
    }
    
    public Message2 setTrackerData(final TrackerData trackerData) {
        if (!presetContentTypes) {
            setContentType(Content.SET_TRACKER_DATA);
        }
        if (trackerDataList == null) {
            trackerDataList = new ArrayList<TrackerData>(1);
        }
        this.trackerDataList.add(trackerData);
        return this;
    }

    public List<TrackerData> getTrackerDataList() {
        if (trackerDataList == null) {
            return Collections.emptyList();
        }
        return trackerDataList;
    }

    public TrackerData getTrackerData(final int index) {
        if (trackerDataList == null || index > trackerDataList.size() - 1) {
            return null;
        }
        return trackerDataList.get(index);
    }
    
    public AtomicReference<PublicKey> publicKeyReference() {
        if (publicKeyReference == null) {
            publicKeyReference = new AtomicReference<PublicKey>();
        }
        return publicKeyReference;
    }

    public Message2 signatureForVerification(Signature signature, PublicKey receivedPublicKey) {
        this.signature = signature;
        this.receivedPublicKey = receivedPublicKey;
        return this;
    }
    
    public Signature signatureForVerification() {
        return signature;
    }
    
    public PublicKey receivedPublicKey() {
        return receivedPublicKey;
    }

    public Message2 receivedSignature(SHA1Signature signatureEncode) {
        this.signatureEncode = signatureEncode;
        return this;
    }
    
    public SHA1Signature receivedSignature() {
        return signatureEncode;
    }
    
    //*************************************** End of content payload ********************
    
    

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Message:id=");
        sb.append(getMessageId()).append(",t=").append(type.toString()).append(",c=").append(command);
        /*
         * sb.append(",c=").append(getCommand().toString()).append(",t=").append(type.toString()).append(",l=")
         * .append(getLength() + MessageCodec.HEADER_SIZE).append(",s=").append(getSender()).append(",r=")
         * .append(getRecipient()); if (LOG.isDebugEnabled()) { if (dataMap != null) { sb.append(",m={"); for
         * (Map.Entry<Number160, Data> entry : dataMap.entrySet()) { sb.append("k:"); sb.append(entry.getKey());
         * sb.append("v:"); sb.append(entry.getValue().getHash()); } sb.append("}"); } }
         */
        return sb.toString();
    }

    // *************************** No transferable objects here *********************************

    /**
     * If we are setting values from the decoder, then the content type is already set.
     * 
     * @param presetContentTypes
     *            True if the content type is already set.
     * @return This class
     */
    public Message2 presetContentTypes(final boolean presetContentTypes) {
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
    public Message2 senderSocket(final InetSocketAddress senderSocket) {
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
    public Message2 recipientSocket(InetSocketAddress recipientSocket) {
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
    public Message2 setHintSign() {
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
     * @param udp
     *            True if connection is UDP
     * @return This class
     */
    public Message2 udp(final boolean udp) {
        this.udp = udp;
        return this;
    }

    /**
     * @return True if connection is UDP
     */
    public boolean isUdp() {
        return udp;
    }

    /**
     * @param done
     *            True if message decoding or encoding is done
     * @return This class
     */
    public Message2 done(final boolean done) {
        this.done = done;
        return this;
    }
    
    /**
     * Set done to true if message decoding or encoding is done.
     * @return This class
     */
    public Message2 setDone() {
        this.done = true;
        return this;
    }

    /**
     * @return True if message decoding or encoding is done
     */
    public boolean isDone() {
        return done;
    }

    public static boolean decodeSignature(Signature signature, Message2 message, ByteBuf buffer)
            throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, IOException {
        if (buffer.readableBytes() < 20 + 20)
            return false;

        byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
        buffer.readBytes(me);
        Number160 number1 = new Number160(me);
        buffer.readBytes(me);
        Number160 number2 = new Number160(me);
        SHA1Signature signatureEncode = new SHA1Signature(number1, number2);
        byte[] signatureReceived = signatureEncode.encode();
        if (!signature.verify(signatureReceived)) {
            // set public key only if signature is correct
            message.setPublicKey(null);
        }
        // set data maps
        return true;
    }

   

    
}
