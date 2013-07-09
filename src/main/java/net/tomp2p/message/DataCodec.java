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

package net.tomp2p.message;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import org.jboss.netty.buffer.ChannelBuffer;

import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

/**
 * Encoder and decoder for {@link Data}.
 * 
 * @author Thomas Bocek
 */
public class DataCodec {
    public static int encodeData(ProtocolChunked input, Data data) throws ClassNotFoundException, IOException {
        int count = 4 + 4;
        // encode entry protection in millis as the sign bit. Thus the max value
        // of seconds is 2^30, which is more than enough
        int seconds = data.getTTLSeconds();
        seconds = data.isProtectedEntry() ? seconds | 0x80000000 : seconds & 0x7FFFFFFF;
        seconds = data.isFileReference() ? seconds | 0x40000000 : seconds & 0xBFFFFFFF;
        input.copyToCurrent(seconds);

        // the real data
        if (data.isFileReference()) {
            File file = (File) data.getObject();
            long len = file.length();
            input.copyToCurrent((int) len);
            // cannot close inFile, since we still need to transfer it!
            @SuppressWarnings("resource")
            FileInputStream inFile = new FileInputStream(file);
            FileChannel inChannel = inFile.getChannel();
            input.transferToCurrent(inChannel, file.length());
            count += len;
        } else {
            input.copyToCurrent(data.getLength());
            input.copyToCurrent(data.getData(), data.getOffset(), data.getLength());
            count += data.getLength();
        }
        return count;
    }

    public static Data decodeData(final ChannelBuffer buffer, PeerAddress originator) throws InvalidKeyException,
            NoSuchAlgorithmException, InvalidKeySpecException {
        // mini header for data, 8 bytes ttl and data length
        if (buffer.readableBytes() < 4 + 4)
            return null;
        int ttl = buffer.readInt();
        boolean protectedEntry = (ttl & 0x80000000) != 0;
        boolean fileReference = (ttl & 0x40000000) != 0;
        ttl &= 0x3FFFFFFF;
        int dateLength = buffer.readInt();
        //
        if (buffer.readableBytes() < dateLength)
            return null;
        ByteBuffer[] byteBuffers = buffer.toByteBuffers(buffer.readerIndex(), dateLength);

        final Data data = createData(byteBuffers, dateLength, ttl, protectedEntry, fileReference, originator);
        buffer.skipBytes(dateLength);
        return data;
    }

    public static Data createData(ByteBuffer[] byteBuffers, final int length, int ttl, boolean protectedEntry,
            boolean fileReference, PeerAddress originator) {
        Data data;
        // length may be 0 if data is only used for expiration
        if (length == 0) {
            data = new Data(MessageCodec.EMPTY_BYTE_ARRAY, originator.getPeerId());
        } else {
            data = new Data(byteBuffers, length, originator.getPeerId());
        }
        return data.setTTLSeconds(ttl).setProtectedEntry(protectedEntry).setFileReference(fileReference);
    }
}
