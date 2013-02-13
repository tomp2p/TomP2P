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

import java.io.IOException;
import java.io.InputStream;

import org.jboss.netty.buffer.ChannelBuffer;

public class MultiByteBufferInputStream extends InputStream {
    private final ChannelBuffer channelBuffer;

    public MultiByteBufferInputStream(ChannelBuffer channelBuffer) {
        this.channelBuffer = channelBuffer;
    }

    @Override
    public int read() throws IOException {
        if (!channelBuffer.readable()) {
            return -1;
        }
        return channelBuffer.readUnsignedByte();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int len2 = channelBuffer.readableBytes();
        int read = Math.min(len, len2);
        channelBuffer.readBytes(b, off, read);
        return read;
    }
}
