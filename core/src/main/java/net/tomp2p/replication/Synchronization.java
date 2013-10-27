/*
 * Copyright 2013 Maxat Pernebayev, Thomas Bocek
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

package net.tomp2p.replication;

import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;

import net.tomp2p.message.Buffer;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

/**
 * Synchronization class is responsible for efficient and optimal synchronization of data resources between responsible
 * peer and replica peers. If one of replicas goes offline, the responsible peer transfers the value completely to the
 * new replica peer. In case the values at responsible peer and replica peer are the same, then no data is transmitted.
 * If the values are different, then only differences are sent to the replica peer.
 * 
 * @author Maxat Pernebayev
 * @author Thomas Bocek
 * 
 */
final public class Synchronization {
    public static final int SIZE = 5;

    /**
     * It returns rolling checksum for the offset. The checksum is based on Adler-32 algorithm
     * 
     * @param start
     *            The start index of offset
     * @param end
     *            The end index of offset
     * @param buffer
     *            The offset of the value
     * @return The weak checksum
     */
    static int getAdler(byte[] buffer, int start, int end) {
        return getAdlerInternal(buffer, start, end)[2];
    }

    private static int[] getAdlerInternal(byte[] buffer, int start, int end) {
        int len = end - start + 1;
        int a = 0, b = 0;
        for (int i = 0; i < len; i++) {
            a += buffer[start + i];
            b += (len - i) * buffer[start + i];
        }
        a = a % 65536;
        b = b % 65536;

        int[] retval = new int[3];
        retval[0] = a;
        retval[1] = b;
        retval[2] = a + 65536 * b;
        return retval;

    }

    /**
     * It returns an array of weak and strong checksums for the value.
     * 
     * @param value
     *            The value
     * @param size
     *            The offset size
     * @return The array of checksums
     * @throws NoSuchAlgorithmException
     */
    public static ArrayList<Checksum> getChecksums(byte[] value, int blockSize) {
        int numberOfBlocks = (value.length + blockSize - 1) / blockSize;
        ArrayList<Checksum> checksums = new ArrayList<Checksum>(numberOfBlocks);
        for (int i = 0; i < numberOfBlocks; i++) {
            int remaining = blockSize;
            if (i == numberOfBlocks - 1) {
                remaining = value.length % blockSize;
            }

            Checksum checksum = new Checksum();
            checksum.setWeakChecksum(getAdler(value, i * blockSize, i * blockSize + remaining - 1));
            checksum.setStrongChecksum(Utils.makeMD5Hash(value, i * blockSize, remaining));
            checksums.add(checksum);
        }
        return checksums;
    }

    /**
     * It checks whether a match is found or not. If it is found returns instruction otherwise null.
     * 
     * @param wcs
     *            The weak checksum of offset
     * @param offset
     *            The offset
     * @param checksums
     *            The checksums
     * @return either instruction or null
     * @throws NoSuchAlgorithmException
     */
    static Instruction matches(int wcs, byte[] buffer, int offset, int length, ArrayList<Checksum> checksums) {
        int checksumSize = checksums.size();
        for (int i = 0; i < checksumSize; i++) {
            int weakChecksum = checksums.get(i).getWeakChecksum();
            if (weakChecksum == wcs) {
                byte[] md5 = Utils.makeMD5Hash(buffer, offset, length);
                byte[] strongChecksum = checksums.get(i).getStrongChecksum();
                if (Arrays.equals(strongChecksum, md5)) {
                    Instruction instruction = new Instruction();
                    instruction.setReference(i);
                    return instruction;
                }
            }
        }
        // no match found, content is different
        return null;
    }

    /**
     * @param newValue
     *            The value at responsible peer
     * @param start
     *            The start index
     * @param end
     *            The end index
     * @return The instruction which contains literal data
     */
    static Instruction getDiff(byte[] newValue, int start, int end) {
        int len = end - start + 1;
        byte[] literal = new byte[len];
        System.arraycopy(newValue, start, literal, 0, len);
        Instruction instruction = new Instruction();
        instruction.setLiteral(literal);
        return instruction;
    }

    private static int[] jump(int offset, int blockSize, byte[] newValue) {
        if (offset + blockSize >= newValue.length) {
            return getAdlerInternal(newValue, offset - 1, newValue.length - 1);
        } else {
            return getAdlerInternal(newValue, offset - 1, offset + blockSize - 2);
        }
    }

    /**
     * It returns the sequence of instructions each of which contains either reference to a block or literal data.
     * 
     * @param newValue
     *            The value at responsible peer
     * @param checksums
     *            The array of checksums
     * @param size
     *            The offset size
     * @return The sequence of instructions
     * @throws NoSuchAlgorithmException
     */
    public static ArrayList<Instruction> getInstructions(byte[] newValue, ArrayList<Checksum> checksums,
            int blockSize) {
        ArrayList<Instruction> result = new ArrayList<Instruction>();
        int[] adler = getAdlerInternal(newValue, 0, blockSize - 1);
        int a = adler[0];
        int b = adler[1];
        int wcs = adler[2];

        int offset = 0;
        int diff = 0;
        Instruction instruction = matches(wcs, newValue, offset, blockSize, checksums);
        if (instruction != null) {
            result.add(instruction);
            offset = blockSize;
            diff = blockSize;
            int[] jumpVal = jump(offset, blockSize, newValue);
            a = jumpVal[0];
            b = jumpVal[1];
        } else {
            offset = 1;
        }
        result = getInstructions(result, diff, offset, a, b, newValue, checksums, blockSize);
        return result;
    }

    public static ArrayList<Instruction> getInstructions(ArrayList<Instruction> result, int diff, int offset, int a,
            int b, byte[] newValue, ArrayList<Checksum> checksums, int blockSize) {
        int wcs;
        if (offset + blockSize >= newValue.length) {
            wcs = getAdlerInternal(newValue, offset, newValue.length - 1)[2];
            Instruction instruction1 = matches(wcs, newValue, offset, newValue.length - offset, checksums);
            if (instruction1 != null) {
                if (diff < offset) {
                    result.add(getDiff(newValue, diff, offset - 1));
                }
                result.add(instruction1);
            } else {
                offset++;
                if (offset >= newValue.length) {
                    if (diff < offset) {
                        result.add(getDiff(newValue, diff, newValue.length - 1));
                    }
                    return result;
                } else
                    getInstructions(result, diff, offset, a, b, newValue, checksums, blockSize);
            }
            return result;
        }

        a = (a - newValue[offset - 1] + newValue[offset + blockSize - 1]) % 65536;
        b = (b - blockSize * newValue[offset - 1] + a) % 65536;
        wcs = a + 65536 * b;

        Instruction instruction1 = matches(wcs, newValue, offset, blockSize, checksums);
        if (instruction1 != null) {
            if (diff < offset) {
                result.add(getDiff(newValue, diff, offset - 1));
            }
            result.add(instruction1);
            diff = offset + blockSize;
            offset = offset + blockSize;
            a = jump(offset, blockSize, newValue)[0];
            b = jump(offset, blockSize, newValue)[1];
            getInstructions(result, diff, offset, a, b, newValue, checksums, blockSize);
        } else {
            offset++;
            getInstructions(result, diff, offset, a, b, newValue, checksums, blockSize);
        }

        return result;

    }

    /**
     * It reconstructs the copy of responsible peer's value using instructions and the replica's value.
     * 
     * @param oldValue
     *            The value at replica
     * @param instructions
     *            The sequence of instructions
     * @param blockSize
     *            The offset size
     * @return The value which is identical to the responsible peer's value
     */
    public static byte[] getReconstructedValue(byte[] oldValue, ArrayList<Instruction> instructions, int blockSize) {

        final int numberOfBlocks = (oldValue.length + blockSize - 1) / blockSize;
        final int remainigSize = oldValue.length % blockSize;

        // calculate the new size of the data
        int newSize = 0;
        for (Instruction instruction : instructions) {
            if (instruction.getReference() == -1) {
                newSize += instruction.getLiteral().length;
            } else {
                newSize += (instruction.getReference() == numberOfBlocks - 1) ? remainigSize : blockSize;
            }
        }
        byte[] reconstructedValue = new byte[newSize];

        int offset = 0;
        for (Instruction instruction : instructions) {
            final int len;
            if (instruction.getReference() == -1) {
                len = instruction.getLiteral().length;
                System.arraycopy(instruction.getLiteral(), 0, reconstructedValue, offset, len);

            } else {
                len = (instruction.getReference() == numberOfBlocks - 1) ? remainigSize : blockSize;
                int reference = instruction.getReference();
                System.arraycopy(oldValue, reference * blockSize, reconstructedValue, offset, len);
            }
            offset += len;
        }
        return reconstructedValue;
    }

    public Buffer getBuffer(Object object) throws IOException {
        return new Buffer(Unpooled.wrappedBuffer(Utils.encodeJavaObject(object)));
    }

    public Object getObject(Buffer buffer) throws IOException, ClassNotFoundException {
        return buffer.object();
    }

    public static byte[] intToByteArray(int value) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            int offset = (b.length - 1 - i) * 8;
            b[i] = (byte) ((value >>> offset) & 0xFF);
        }
        return b;
    }
    
    public static int byteArrayToInt(byte[] b){
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (b[i] & 0x000000FF) << shift;
        }
        return value;
    }
    
    public static byte[] encodeChecksumList(ArrayList<Checksum> checksums) {
        int size = checksums.size();
        byte[] array = new byte[4+size*20];
        byte[] info = intToByteArray(size);
        System.arraycopy(info, 0, array, 0, 4);
        for(int i=0; i<size; i++){
            byte[] weakChecksum = intToByteArray(checksums.get(i).getWeakChecksum());
            System.arraycopy(weakChecksum, 0, array, 20*i+4, 4);
            System.arraycopy(checksums.get(i).getStrongChecksum(), 0, array, 20*i+8, 16);
        }
        return array;
    }

    public static ArrayList<Checksum> decodeChecksumList(byte[] bytes) {
        ArrayList<Checksum> checksums = new ArrayList<Checksum>();
        byte[] info = new byte[4];
        System.arraycopy(bytes, 0, info, 0, 4);
        int size = byteArrayToInt(info);
        for(int i=0; i<size; i++){
            Checksum checksum = new Checksum();
            byte[] weakChecksum = new byte[4];
            System.arraycopy(bytes, 20*i+4, weakChecksum, 0, 4);
            checksum.setWeakChecksum(byteArrayToInt(weakChecksum));
            byte[] strongChecksum = new byte[16];
            System.arraycopy(bytes, 20*i+8, strongChecksum, 0, 16);
            checksum.setStrongChecksum(strongChecksum);
            checksums.add(checksum);
        }
        return checksums;
    }
    
    public static byte[] encodeInstructionList(ArrayList<Instruction> instructions, Number160 number160) {
        int size = instructions.size();
        int length = 0;
        ArrayList<Integer> literalSize = new ArrayList<Integer>();
        for(int i=0; i<size; i++) {
            int temp = instructions.get(i).literalSize();
            length += temp;
            literalSize.add(temp);
        }
        
        byte[] array = new byte[20+4+8*size+length]; // 20 - Number160, 4 - number of instructions, 8 - size of each  instruction and reference, length - all literals
        byte[] hash = number160.toByteArray();
        System.arraycopy(hash, 0, array, 0, 20);
        byte[] info = intToByteArray(size);
        System.arraycopy(info, 0, array, 20, 4);
        
        for(int i=0; i<size; i++) {
            System.arraycopy(intToByteArray(literalSize.get(i)), 0, array, 4*i+24, 4);
        }
        
        int nextPosition = 4*size+24;
        
        int shift = 0;
        for(int i=0; i<size; i++) {
            byte[] reference =intToByteArray(instructions.get(i).getReference());
            System.arraycopy(reference, 0, array, nextPosition+shift, 4);
            if(literalSize.get(i)!=0)
                System.arraycopy(instructions.get(i).getLiteral(), 0, array, nextPosition+shift+4, literalSize.get(i));
            shift += literalSize.get(i)+4;
        }
        
        return array;
    }

    public static ArrayList<Instruction> decodeInstructionList(byte[] bytes) {
        ArrayList<Instruction> instructions = new ArrayList<Instruction>();
        byte[] info = new byte[4];
        System.arraycopy(bytes, 20, info, 0, 4);
        int size = byteArrayToInt(info);
        
        ArrayList<Integer> literalSize = new ArrayList<Integer>();
        for(int i=0; i<size; i++) {
            byte[] temp = new byte[4];
            System.arraycopy(bytes, 4*i+24, temp, 0, 4);
            literalSize.add(byteArrayToInt(temp));
        }
        
        int nextPosition = 4*size+24;
        int shift = 0;
        
        for(int i=0; i<size; i++) {
            Instruction instruction = new Instruction();
            byte[] reference = new byte[4];
            System.arraycopy(bytes, nextPosition+shift, reference, 0, 4);
            int referenceValue = byteArrayToInt(reference);
            if(referenceValue!=-1){
                instruction.setReference(referenceValue);
            }
            else {
                byte[] literal = new byte[literalSize.get(i)];
                System.arraycopy(bytes, nextPosition+shift+4, literal, 0, literalSize.get(i));
                instruction.setLiteral(literal);
            }
            shift += literalSize.get(i)+4;
            instructions.add(instruction);          
        }
        
        return instructions;    
    }

    public static Number160 decodeHash(byte[] bytes) {
        byte[] number160 = new byte[20];
        System.arraycopy(bytes, 0, number160, 0, 20);
        
        return new Number160(number160);
    }

}
