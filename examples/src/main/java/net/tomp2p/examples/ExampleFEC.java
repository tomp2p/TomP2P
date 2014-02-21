/*
 * Copyright 2013 Thomas Bocek
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

package net.tomp2p.examples;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import ch.qos.logback.core.pattern.Converter;

/**
 * Shows an FEC example based on XOR.
 * 
 * @author Thomas Bocek
 * 
 */
public class ExampleFEC {
    // n packet of size m
    final private static int n = 9;
    final private static int m = 100;
    // lost = overhead
    final private static int lost = 4;
    final private static byte[][] input = new byte[n][m];
    final private static byte[][] output = new byte[n][m];
    final private static byte[][] output2 = new byte[n - lost][m];
    final private static Random RND = new Random(5);
    
    public static void main(String[] args) {
        int b1 = RND.nextInt();
        int b2 = RND.nextInt();
        int b3 = RND.nextInt();
        int b4 = RND.nextInt();
        //
        int r1 = RND.nextInt(31)+1;
        int r2 = RND.nextInt(31)+1;
        int r3 = RND.nextInt(31)+1;
        int r4 = RND.nextInt(31)+1;
        int r5 = RND.nextInt(31)+1;
        int r6 = RND.nextInt(31)+1;
        int r7 = RND.nextInt(31)+1;
        int r8 = RND.nextInt(31)+1;
        //int r8 = 4;
        
        int a = rot(b1, r1) ^ rot (b2, r2) ^ rot (b3, r3) ^ rot (b4, r4);
        int b = rot(b1, r5) ^ rot (b2, r6) ^ rot (b3, r7) ^ rot (b4, r8);
        
        boolean parity = (b1 + b2 + b3 + b4) % 2 == 0;
        
        System.err.println("A="+a+", B="+b);
        
        //
        a = a ^ rot (b1, r1) ^ rot (b2, r2);
        b = b ^ rot (b1, r5) ^ rot (b2, r6);
        
        System.err.println("still got A="+a+", B="+b);
        
        //now we still have
        //a=rot (b3, r3) ^ rot (b4, r4);
        //b=rot (b3, r7) ^ rot (b4, r8);
        
        int left = a;
        int right = rot (b3, r3) ^ rot (b4, r4);
        
        System.err.println("we have on the left "+left+" and on the right "+right);
        
        //we can transform to
        //a ^ rot(b3, r3) = rot (b4, r4);
        
        left = a ^ rot(b3, r3);
        right = rot (b4, r4);
        System.err.println("we have on the left "+left+" and on the right "+right);
        
        //isolate b4
        //rot(a ^ rot(b3, r3),-r4) = b4
        
        left = rot(a ^ rot(b3, r3), -r4);
        right = b4;
        System.err.println("we have on the left "+left+" and on the right "+right);
        
        //put into other 
        //b=rot (b3, r7) ^ rot (rot(a ^ rot(b3, r3),-r4), r8);
        //simplify
        //b=rot (b3, r7) ^ rot(a ^ rot(b3, r3),-r4 + r8);
        //b=rot (b3, r7) ^ rot(a,-r4 + r8) ^ rot(b3, r3 + -r4 + r8);
        
        left = b;
        right = rot (b3, r7) ^ rot(a,-r4 + r8) ^ rot(b3, r3 + -r4 + r8);
        System.err.println("we have on the left "+left+" and on the right "+right);
        
        //now we can solve it:
        //b ^ rot(a,-r4 + r8) = rot (b3, r7) ^ rot(b3, r3 + -r4 + r8);
        left = b ^ rot(a,-r4 + r8);
        //even more simplified:
        //rot(b ^ rot(a,-r4 + r8),-r7) = b3 ^ rot(b3, r3 + -r4 + r8 - r7);
        
        left = rot(b ^ rot(a,-r4 + r8),-r7);
        right = b3 ^ rot(b3, r3 + -r4 + r8 - r7);
        int r=r3 - r4 + r8 - r7;
        System.err.println("we have on the left "+left+" and on the right "+right);
        
        //figure out the bits:
        //start with the lowest bit, since we have there the parity
        //guess 0/1 -> go for 1 
        //left = b3 ^ (b3 rot r)
        //
       
        
        BitSet leftBS = convert(left);
        BitSet b3Real = convert(b3);

        
        
        BitSet b3BST = guess(leftBS, r, true);
        BitSet b3BSF = guess(leftBS, r, false);
            
        System.err.println("guessed "+ (int)b3BST.toLongArray()[0]+" expected "+(int)b3Real.toLongArray()[0] + ",r="+r);
        System.err.println("guessed "+ (int)b3BSF.toLongArray()[0]+" expected "+(int)b3Real.toLongArray()[0] + ",r="+r);
        System.err.println("guessed "+ b3BST+" expected "+b3Real + ",r="+r);
        System.err.println("guessed "+ b3BSF+" expected "+b3Real + ",r="+r);
        
        int dr = rot(a,2) ^ rot(a, 3);
        
        System.err.println("drot "+(dr));
        int rr = rot (rot(dr, 1) ^ dr, 1);
        System.err.println("rrot "+(rr)+", a="+a);
    }
    
    public static BitSet guess(BitSet leftBS, int r, boolean pre) {
        BitSet b3BS = new BitSet(32);
        int start = 0;
        //first set to 0;
        int pos = start;
        b3BS.set(pos, pre);
        for(int i=0;i<32;i++) {
            boolean prev = b3BS.get(pos);
            int oldpos = pos; 
            pos = (pos + r) % 32;
            if(pos<0) {
                pos = 32 + pos;
            }
            b3BS.set(pos, prev ^ leftBS.get(oldpos));
        }
        return b3BS;
    }
    
    public static int rxor(int nr, int n2) {
        return rot(nr, 1) ^ n2;
    }
    
    public static int rrxor(int nr, int n2) {
        return rot(nr ^ n2, -1);
    }
    
    
    public static int rot(int i, int k) {
        //go backwards
        if(k<0) {
            k = 32 + k;   
        }
        if(k>32) {
            k = k % 32;
        }
        return (i >>> k) | (i << (32 -k));
    }
    
    public static BitSet convert(int value) {
        BitSet result = new BitSet(32);
        int index = 0;
        while (value !=0) {
            if (value % 2 !=0) {
                result.set(index);
            }
            ++index;
            value = value >>>1;
        }
        return result;
    }
    

    public static void main1(String[] args) {
        // fill random data
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                input[i][j] = (byte) RND.nextInt();
            }
        }
        // now calculate xor
        for (int i = 0; i < n; i++) {
            // k can be optimized to n/2
            for (int k = 0; k <= i; k++) {
                for (int j = 0; j < m; j++) {
                    output[n][j] ^= input[k][j];
                }
            }
        }
        // we can transfer the data
        List<Integer> packetNrs = dropRandom();
        // reconstruct
    }

    private static List<Integer> dropRandom() {
        List<Integer> tmp = new ArrayList<Integer>();
        for (int i = 0; i < n; i++) {
            tmp.add(i);
        }
        for (int i = 0; i < lost; i++) {
            tmp.remove(RND.nextInt(tmp.size()));
        }
        for (int i = 0; i < n - lost; i++) {
            output2[i] = output[tmp.get(i)];
        }
        return tmp;
    }
}
