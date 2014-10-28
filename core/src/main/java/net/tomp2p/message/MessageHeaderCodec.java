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

import java.net.InetSocketAddress;

import net.tomp2p.message.Message.Content;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static final int HEADER_SIZE = 58;

    /**
     * Encodes a message object.
     * 
     * The format looks as follows: 28bit p2p version - 4bit message type - 32bit message id - 8bit message command - 160bit
     * sender id - 16bit sender tcp port - 16bit sender udp port - 160bit recipient id - 32bit content types - 8bit options. It total,
     * the header is of size 58 bytes.
     * 
     * @param buffer
     *            The buffer to encode to
     * @param message
     *            The message with the header that will be encoded
     * @return The buffer passed as an argument
     */
    public static void encodeHeader(final ByteBuf buffer, final Message message) {
        
    	final int versionAndType = message.version() << 4 | (message.type().ordinal() & Utils.MASK_0F);
        buffer.writeInt(versionAndType); // 4
        buffer.writeInt(message.messageId()); // 8
        buffer.writeByte(message.command()); // 9
        buffer.writeBytes(message.sender().peerId().toByteArray()); // 29
        buffer.writeShort((short) message.sender().tcpPort()); // 31
        buffer.writeShort((short) message.sender().udpPort()); // 33
        buffer.writeBytes(message.recipient().peerId().toByteArray()); // 53
        buffer.writeInt(encodeContentTypes(message.contentTypes())); // 57
        buffer.writeByte((message.sender().options() << 4) | message.options()); // 58
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
    public static Message decodeHeader(final ByteBuf buffer, final InetSocketAddress recipientSocket,
            final InetSocketAddress senderSocket) {
        LOG.debug("Decode message. Recipient: {}, Sender:{}.", recipientSocket, senderSocket);
        final Message message = new Message();
        final int versionAndType = buffer.readInt();
        message.version(versionAndType >>> 4);
        message.type(Type.values()[(versionAndType & Utils.MASK_0F)]);
        message.messageId(buffer.readInt());
        final int command = buffer.readUnsignedByte();
        message.command((byte) command);
        final Number160 senderID = readID(buffer);
        final int tcpPort = buffer.readUnsignedShort();
        final int udpPort = buffer.readUnsignedShort();
        final Number160 recipientID = readID(buffer);
        message.recipient(new PeerAddress(recipientID, recipientSocket));
        final int contentTypes = buffer.readInt();
        message.hasContent(contentTypes != 0);
        message.contentTypes(decodeContentTypes(contentTypes, message));
        // set the address as we see it, important for port forwarding
        // identification
        final int options = buffer.readUnsignedByte();
        message.options(options & Utils.MASK_0F);
        final int senderOptions = options >>> 4;
        final PeerAddress peerAddress = new PeerAddress(senderID, senderSocket.getAddress(), tcpPort, udpPort,
                senderOptions);
        message.sender(peerAddress);
        message.senderSocket(senderSocket);
        message.recipientSocket(recipientSocket);
        return message;
    }

    /**
     * Reads a {@code Number160} number from a Netty buffer. I did not want to include ChannelBuffer in the class Number160.
     * 
     * @param buffer
     *            The Netty buffer
     * @return A 160bit number from the Netty buffer (deserialized)
     */
    private static Number160 readID(final ByteBuf buffer) {
        byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
        buffer.readBytes(me);
        return new Number160(me);
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
