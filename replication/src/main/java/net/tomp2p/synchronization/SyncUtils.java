/*
 * Copyright 2013 Thomas Bocek, Maxat Pernebayev
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

package net.tomp2p.synchronization;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import net.tomp2p.peers.Number160;

public class SyncUtils {
	
	public static List<Instruction> decodeInstructions(ByteBuf buf) {
		List<Instruction> result = new ArrayList<Instruction>();
	    while(buf.isReadable()) {
	    	final int header = buf.readInt();
	    	if((header & 0x80000000) != 0) {
	    		//first bit set, we have a reference
	    		final int reference = header & 0x7FFFFFFF;
	    		result.add(new Instruction(reference));
	    	} else {
	    		//otherwise the header is the length
	    		final int length = header;
	    		final int remaining = Math.min(length, buf.readableBytes());
	    		ByteBuf literal = buf.slice(buf.readerIndex(), remaining);
	    		buf.skipBytes(remaining);
	    		result.add(new Instruction(new RArray(literal)));
	    	}
	    }
	    return result;
    }

	public static Number160 decodeHeader(ByteBuf buf) {
		byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
    	buf.readBytes(me);
    	return new Number160(me);
    }
	
	public static int encodeInstructions(List<Instruction> instructions, Number160 versionKey, 
			Number160 hash, ByteBuf buf) {
		int size = 0;
	    buf.writeBytes(versionKey.toByteArray());
	    buf.writeBytes(hash.toByteArray());
		//first bit to 1 means reference, otherwise length of the literal
		for(Instruction instruction:instructions) {
			int header = instruction.reference();
			size +=4;
			if(header != -1) {
				header = header | (1<<31);
				buf.writeInt(header);
			} else {
				header = instruction.length();
				size += header;
				//no need to clear flag
				buf.writeInt(header);
				instruction.literal().transferTo(buf);
			}
		}
		return size;
    }
	
	public static ByteBuf encodeChecksum(List<Checksum> checksums, Number160 versionKey, Number160 hash, ByteBuf buf) {
		buf.writeBytes(versionKey.toByteArray());
		buf.writeBytes(hash.toByteArray());
        for(Checksum checksum:checksums) {
        	buf.writeInt(checksum.weakChecksum());
        	buf.writeBytes(checksum.strongChecksum());
        }
        return buf;
	}
	
	public static List<Checksum> decodeChecksums(ByteBuf buf) {
		final List<Checksum> result = new ArrayList<Checksum>();
		while(buf.isReadable()) {
			//16 bytes as its a MD5
			final byte[] me = new byte[16];
			final int weak = buf.readInt();
			buf.readBytes(me);
			result.add(new Checksum(weak, me));
		}
		return result;
	}
}
