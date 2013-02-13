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
import java.security.Signature;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The decoder first decodes the header. If this has been successful, then if
 * there is payload, then the payload is decoded. The TomP2P decoder can be used
 * to decode several messages in the same session as long as they are
 * sequential. This class is not thread safe.
 * 
 * @author Thomas Bocek
 */
public class TomP2PDecoderTCP extends FrameDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TomP2PDecoderTCP.class);

    private final int maxMessageSize;

    // This var keeps the state of the decoding. If null, header not complete,
    // if not null, header is complete
    private volatile Message message = null;

    private volatile byte[] rawHeader = new byte[MessageCodec.HEADER_SIZE];

    private volatile Signature signature = null;

    private volatile int step = 0;

    /**
     * Default constructor with no message size limit.
     */
    public TomP2PDecoderTCP() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Limits the message size
     * 
     * @param maxMessageSize
     *            The limit of a message size.
     */
    public TomP2PDecoderTCP(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    protected Object decode(final ChannelHandlerContext ctx, final Channel channel, final ChannelBuffer buffer)
            throws Exception {
        if (buffer.readableBytes() > maxMessageSize) {
            throw new DecoderException("Message size larger than " + maxMessageSize);
        }
        // read header if possible and not already read
        if (message == null && buffer.readableBytes() >= MessageCodec.HEADER_SIZE) {
            // in case we want to check the signature, we need to keep the
            // header for a while
            buffer.getBytes(buffer.readerIndex(), rawHeader);
            final InetSocketAddress remoteSocket = (InetSocketAddress) channel.getRemoteAddress();
            final InetSocketAddress localSocket = (InetSocketAddress) channel.getLocalAddress();
            message = MessageCodec.decodeHeader(buffer, localSocket, remoteSocket);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("got header in decoder " + message);
            }
            // set that we did not read data, otherwise it gets lost.
            if (!message.hasContent()) {
                return cleanupAndReturnMessage();
            }
        }
        // go for the content. Since we don't have the length anymore, we decide
        // on the fly when we are finished
        if (message != null && message.hasContent()) {
            int readerIndex = buffer.readerIndex();
            if (step == 0 && !MessageCodec.decodePayload(message.getContentType1(), buffer, message)) {
                buffer.readerIndex(readerIndex);
                return null;
            } else if (step == 0) {
                step++;
                if (message.isHintSign()) {
                    signature = Signature.getInstance("SHA1withDSA");
                    signature.initVerify(message.getPublicKey());
                    signature.update(rawHeader);
                    int read = buffer.readerIndex() - readerIndex;
                    if (read > 0) {
                        ByteBuffer[] tmp = buffer.toByteBuffers(readerIndex, read);
                        for (int i = 0; i < tmp.length; i++) {
                            signature.update(tmp[i]);
                        }
                    }
                }
                readerIndex = buffer.readerIndex();
            }

            if (step == 1 && !MessageCodec.decodePayload(message.getContentType2(), buffer, message)) {
                buffer.readerIndex(readerIndex);
                return null;
            } else if (step == 1) {
                step++;
                if (signature != null) {
                    int read = buffer.readerIndex() - readerIndex;
                    if (read > 0) {
                        ByteBuffer[] tmp = buffer.toByteBuffers(readerIndex, read);
                        for (int i = 0; i < tmp.length; i++) {
                            signature.update(tmp[i]);
                        }
                    }
                }
                readerIndex = buffer.readerIndex();
            }

            if (step == 2 && !MessageCodec.decodePayload(message.getContentType3(), buffer, message)) {
                buffer.readerIndex(readerIndex);
                return null;
            } else if (step == 2) {
                step++;
                if (signature != null) {
                    int read = buffer.readerIndex() - readerIndex;
                    if (read > 0) {
                        ByteBuffer[] tmp = buffer.toByteBuffers(readerIndex, read);
                        for (int i = 0; i < tmp.length; i++) {
                            signature.update(tmp[i]);
                        }
                    }
                }
                readerIndex = buffer.readerIndex();
            }

            if (step == 3 && !MessageCodec.decodePayload(message.getContentType4(), buffer, message)) {
                buffer.readerIndex(readerIndex);
                return null;
            } else if (step == 3) {
                step++;
                if (signature != null) {
                    int read = buffer.readerIndex() - readerIndex;
                    if (read > 0) {
                        ByteBuffer[] tmp = buffer.toByteBuffers(readerIndex, read);
                        for (int i = 0; i < tmp.length; i++) {
                            signature.update(tmp[i]);
                        }
                    }
                }
                readerIndex = buffer.readerIndex();
            }

            if (step == 4 && signature != null && !MessageCodec.decodeSignature(signature, message, buffer)) {
                buffer.readerIndex(readerIndex);
                return null;
            }
            return cleanupAndReturnMessage();
        } else {
            return null;
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            e.getCause().printStackTrace();
        }
        ctx.sendUpstream(e);
    }

    /**
     * {@inheritDoc} decodeLast is called from cleanup within FrameDecoder,
     * which is triggered by a close. This can be called by a timer if and
     * IdleHandler has been defined.
     */
    /*
     * @Override protected Object decodeLast(ChannelHandlerContext ctx, Channel
     * channel, ChannelBuffer buffer) throws Exception { return null; }
     */

    /**
     * After successfully reception of the message, we need to set message to
     * null in order to reuse the connection. If connection is closed, then this
     * cleanup would not be necessary as the annotation
     * ChannelPipelineCoverage("one") creates a new TomP2PDecoder. Thus, we do
     * not need to cleanup in the close() or disconnect() method.
     * 
     * @return The message, which reference has been set to null
     */
    private Message cleanupAndReturnMessage() {
        final Message tmp = message;
        message = null;
        step = 0;
        signature = null;
        // set finished time at the end since the sender starts its timer after
        // sending the last packet
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("cleanupAndReturnMessage " + tmp);
        }
        tmp.setTCP();
        tmp.finished();
        return tmp;
    }

}
