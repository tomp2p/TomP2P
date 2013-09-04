/*
 * Copyright 2012 Thomas Bocek
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

package net.tomp2p.utils;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;

/**
 * Used to decode Java objects from channel buffers.
 * 
 * @author Thomas Bocek
 * 
 */
public class MultiByteBufferInputStream extends InputStream {
    private final ByteBuf channelBuffer;

    /**
     * Create a stream from a channel buffer.
     * 
     * @param channelBuffer
     *            The channel buffer where to read the data from
     */
    public MultiByteBufferInputStream(final ByteBuf channelBuffer) {
        this.channelBuffer = channelBuffer;
    }

    @Override
    public int read() throws IOException {
        if (!channelBuffer.isReadable()) {
            return -1;
        }
        return channelBuffer.readUnsignedByte();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        int len2 = channelBuffer.readableBytes();
        int read = Math.min(len, len2);
        channelBuffer.readBytes(b, off, read);
        return read;
    }
}
