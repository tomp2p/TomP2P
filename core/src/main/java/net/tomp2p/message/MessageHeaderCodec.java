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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import net.tomp2p.message.Message.Content;
import net.tomp2p.message.Message.ProtocolType;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.IP;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

/**
 * Encodes and decodes the header of a {@code Message} sing a Netty Buffer.
 * 
 * @author Thomas Bocek
 * 
 */
public final class MessageHeaderCodec {

    private static final Logger LOG = LoggerFactory.getLogger(MessageHeaderCodec.class);

    /**
     * Empty constructor.
     */
    private MessageHeaderCodec() {
    }

    public static final int HEADER_SIZE_STATIC = 34;
    public static final int HEADER_SIZE_MIN = HEADER_SIZE_STATIC + PeerAddress.MIN_SIZE_HEADER; //58

    /**
     * Encodes a message object.
     * 
     * The format looks as follows: 
     *  - 2bit protocol type
     *  - 26bit p2p version 
     *  - 4bit message type 	//4 bytes
     *  - 32bit message id  	//8 bytes
     *  - 8bit message command  //9 bytes
     *  - 160bit recipient id 	//29 bytes
     *  - 32bit content types   //33 bytes
     *  - 8bit message options. //34 bytes
     *  - sender peeraddress, without current IP (variable). The size is 
     *    determined by the first 3 byte. Minimun size is 25 bytes
     *    //total minimun 57 bytes
     *  
     *  It total,
     * the header is of size 57 bytes.
     * 
     * @param buffer
     *            The buffer to encode to
     * @param message
     *            The message with the header that will be encoded
     * @return The buffer passed as an argument
     */
    public static void encodeHeader(final ByteBuf buf, final Message message) {
    	final int versionAndType = message.protocolType().ordinal() << 30 | message.version() << 4 | (message.type().ordinal() & Utils.MASK_0F);
    	buf.writeInt(versionAndType); // 4
    	buf.writeInt(message.messageId()); // 8
    	buf.writeByte(message.command()); // 9
    	message.recipient().peerId().encode(buf); //29
    	buf.writeInt(encodeContentTypes(message.contentTypes())); // 33
        // three bits for the message options, 5 bits for the sender options
    	buf.writeByte(message.options()); // 34
    	message.sender().encode(buf);
    }
    
    public static ProtocolType peekProtocolType(int versionAndType) {
    	return ProtocolType.values()[versionAndType >>> 30];
    }
    
    public static ProtocolType peekProtocolType(byte version) {
    	return ProtocolType.values()[version >>> 6];
    }

    /**
     * Decodes a message object.
     * 
     * The format looks as follows: 28bit p2p version - 4bit message type - 32bit message id - 8bit message command - 160bit
     * sender id - 16bit sender tcp port - 16bit sender udp port - 160bit recipient id - 32bit content types - 8bit options. It total,
     * the header is of size 58 bytes.
     * 
     * @param buffer
     *            The buffer to decode from
     * @param recipientSocket
     *            The recipient of the message
     * @param senderSocket
     *            The sender of the packet, which has been set in the socket class
     * @return The partial message where only the header fields are set
     */
    public static boolean decodeHeader(final ByteBuf buffer, final InetSocketAddress recipientSocket,
            final InetSocketAddress senderSocket, final Message message) {
        LOG.debug("Decode message. Recipient: {}, Sender:{}.", recipientSocket, senderSocket);
        final int versionAndType = buffer.readInt();
        message.version(versionAndType >>> 4);
        message.type(Type.values()[(versionAndType & Utils.MASK_0F)]);
        message.protocolType(ProtocolType.values()[versionAndType >>> 30]);
        message.messageId(buffer.readInt());
        final int command = buffer.readUnsignedByte();
        message.command((byte) command);
        final Number160 recipientID = Number160.decode(buffer);
        
        //we only get the id for the recipient, the rest we already know
        final PeerAddress recipient = PeerAddress.builder().peerId(recipientID).build();
        message.recipient(recipient);
        
        
        final int contentTypes = buffer.readInt();
        message.hasContent(contentTypes != 0);
        message.contentTypes(decodeContentTypes(contentTypes, message));
        // set the address as we see it, important for port forwarding
        // identification
        final int messageOptions = buffer.readUnsignedByte();
        message.options(messageOptions);
        
        final int header = buffer.readUnsignedShort();
        final int peerAddressSize = PeerAddress.size(header);
        if(PeerAddress.HEADER_SIZE + buffer.readableBytes() < peerAddressSize) {
        	return false;
        }
        
        final PeerAddress sender = PeerAddress.decode(header, buffer);
        if(senderSocket.getAddress() instanceof Inet4Address) {
        	PeerSocket4Address psa4 = sender.ipv4Socket().withIpv4(IP.fromInet4Address((Inet4Address)senderSocket.getAddress()));
        	message.sender(sender.withIpv4Socket(psa4));	
        } else {
        	PeerSocket6Address psa6 = sender.ipv6Socket().withIpv6(IP.fromInet6Address((Inet6Address)senderSocket.getAddress()));
        	message.sender(sender.withIpv6Socket(psa6));	
        }
        
        //keep the original sockets
        message.senderSocket(senderSocket);
        message.recipientSocket(recipientSocket);
        
        return true;
    }

    /**
     * Encodes the 8 content types to an integer (32 bit).
     * 
     * @param contentTypes
     *            The 8 content types to be encoded. Null means Content.Empty
     * @return The encoded 32bit integer
     */
    public static int encodeContentTypes(final Content[] contentTypes) {
        int result = 0;
        for (int i = 0; i < Message.CONTENT_TYPE_LENGTH / 2; i++) {
            if (contentTypes[i * 2] != null) {
                result |= (contentTypes[i * 2].ordinal() << (i * 8));
            }
            if (contentTypes[(i * 2) + 1] != null) {
                result |= ((contentTypes[(i * 2) + 1].ordinal() << 4) << (i * 8));
            }
        }
        return result;
    }

    /**
     * Decodes the 8 content types from an integer (32 bit).
     * 
     * @param contentTypes
     *            The 8 content types to be decoded. No null values are returned
     * @param message 
     * @return The decoded content types
     */
    public static Content[] decodeContentTypes(int contentTypes, Message message) {
        Content[] result = new Content[Message.CONTENT_TYPE_LENGTH];
        for (int i = 0; i < Message.CONTENT_TYPE_LENGTH; i++) {
            Content content = Content.values()[contentTypes & Utils.MASK_0F];
            result[i] = content;
            if(content == Content.PUBLIC_KEY_SIGNATURE) {
                message.setHintSign();
            }
            contentTypes >>>= 4;
        }
        return result;
    }
}
