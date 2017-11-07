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

package net.tomp2p.peers;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import net.tomp2p.Utils2;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

/**
 * Test the serialization and deserialization of PeerAddress.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestPeerAddress {
    private static final int SEED = 1;
    private static final int BIT_16 = 256 * 256;
    private static final Random RND = new Random(SEED);
    
    @Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };
    
    @Test
    public void testLongConversion() throws UnknownHostException {
    	long hi = 0x1122334455667788l;
    	long lo = 0x8877665544332211l;
    	byte[] me = new byte[16];
    	Utils.longToByteArray(hi, lo, me, 0);
    	
    	long hi2 = Utils.byteArrayToLong(me, 0);
    	long lo2 = Utils.byteArrayToLong(me, 8);
    	
    	Assert.assertEquals(hi, hi2);
    	Assert.assertEquals(lo, lo2);
    }
    
    @Test
    public void testIPv6Coding() throws UnknownHostException {
    	PeerAddress pa = Utils2.createPeerAddress(new Number160("0x857e35a42e4677675644522456"),
                InetAddress.getByName("0123:4567:89ab:cdef:1111:2222:3333:4444"), RND.nextInt(BIT_16));
    	byte[] me = pa.encode();
    	PeerAddress pa2 = PeerAddress.decode(me).element0();
        compare(pa, pa2);
    }

    /**
     * Test serialization and deserialization of PeerAddress.
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress() throws UnknownHostException {
        Number160 id = new Number160(RND.nextInt());
        InetAddress address = InetAddress.getByName("127.0.0.1");
        int portUDP = RND.nextInt(BIT_16);
        PeerAddress pa = Utils2.createPeerAddress(id, address, portUDP);
        byte[] me = pa.encode();
        PeerAddress pa2 = PeerAddress.decode(me).element0();
        compare(pa, pa2);
    }

    /**
     * Test serialization and deserialization of PeerAddress.
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress2() throws UnknownHostException {
        Number160 id = new Number160("0x657435a424444522456");
        InetAddress address = InetAddress.getByName("192.168.240.230");
        int portUDP = RND.nextInt(BIT_16);
        PeerAddress pa = Utils2.createPeerAddress(id, address, portUDP);
        byte[] me = pa.encode();
        PeerAddress pa2 = PeerAddress.decode(me).element0();
        compare(pa, pa2);
    }

    /**
     * Test serialization and deserialization of PeerAddress.
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress3() throws UnknownHostException {
        PeerAddress pa1 = Utils2.createPeerAddress(new Number160("0x857e35a42e444522456"),
                InetAddress.getByName("192.168.230.230"), RND.nextInt(BIT_16));
        PeerAddress pa2 = Utils2.createPeerAddress(new Number160("0x657435a424444522456"),
                InetAddress.getByName("192.168.240.230"), RND.nextInt(BIT_16));
        final int length = 200;
        byte[] me = new byte[length];
        final int offset = 50;
        int offset2 = pa1.encode(me, offset);
        pa2.encode(me, offset2);
        //
        Pair<PeerAddress, Integer> pair = PeerAddress.decode(me, offset); 
        PeerAddress pa3 = pair.element0();
        int offset4 = pair.element1();
        PeerAddress pa4 = PeerAddress.decode(me, offset4).element0();
        compare(pa1, pa3);
        compare(pa2, pa4);
    }

    /**
     * Test serialization and deserialization of PeerAddress. Test also IPv6
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress4() throws UnknownHostException {
        PeerAddress pa1 = Utils2.createPeerAddress(new Number160("0x857e35a42e444522456"),
                InetAddress.getByName("0123:4567:89ab:cdef:0123:4567:89ab:cdef"), RND.nextInt(BIT_16));
        PeerAddress pa2 = Utils2.createPeerAddress(new Number160("0x657435a424444522456"),
                InetAddress.getByName("f123:4567:89ab:cdef:0123:4567:89ab:cdef"), RND.nextInt(BIT_16));

        final int length = 200;
        byte[] me = new byte[length];
        final int offset = 50;
        int offset2 = pa1.encode(me, offset);
        pa2.encode(me, offset2);
        //
        Pair<PeerAddress, Integer> pair = PeerAddress.decode(me, offset); 
        PeerAddress pa3 = pair.element0();
        int offset4 = pair.element1();
        PeerAddress pa4 = PeerAddress.decode(me, offset4).element0();
        compare(pa1, pa3);
        compare(pa2, pa4);
    }

    /**
     * Test serialization and deserialization of PeerAddress. Test maximum size.
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress5() throws UnknownHostException {

        Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("192.168.230.230"), RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("2123:4567:89ab:cdef:0123:4567:89ab:cde2"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("192.168.230.231"), RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("4123:4567:89ab:cdef:0123:4567:89ab:cde4"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("192.168.230.232"), RND.nextInt(BIT_16)));
        
        PeerAddress pa3 = Utils2.createPeerAddress(new Number160("0x657435a424444522456"),
                InetAddress.getByName("f123:4567:89ab:cdef:0123:4567:89ab:cdef"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16), psa);

        final int length = 200;
        byte[] me = new byte[length];
        final int offset = 50;
        pa3.encode(me, offset);
        PeerAddress pa4 = PeerAddress.decode(me, offset).element0();

        compare(pa3, pa4);
    }

    /**
     * Test serialization and deserialization of PeerAddress. Test mix of IPv4 and IPv6.
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress6() throws UnknownHostException {

        Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("1123:4567:89ab:cdef:0123:4567:89ab:cde1"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("2123:4567:89ab:cdef:0123:4567:89ab:cde2"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("3123:4567:89ab:cdef:0123:4567:89ab:cde3"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("4123:4567:89ab:cdef:0123:4567:89ab:cde4"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("5123:4567:89ab:cdef:0123:4567:89ab:cde5"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("6123:4567:89ab:cdef:0123:4567:89ab:cde5"),
                RND.nextInt(BIT_16)));
        psa.add(Utils2.creatPeerSocket(InetAddress.getByName("7123:4567:89ab:cdef:0123:4567:89ab:cde5"),
                RND.nextInt(BIT_16)));
        
        PeerAddress pa3 = Utils2.createPeerAddress(new Number160("0x657435a424444522456"),
        		(Inet6Address)Inet6Address.getByName("f123:4567:89ab:cdef:0123:4567:89ab:cdef"), RND.nextInt(BIT_16),
                psa, Utils2.creatPeerSocket4((Inet4Address)Inet4Address.getByName("192.168.230.231"), RND.nextInt(BIT_16)));

        final int length = 400;
        byte[] me = new byte[length];
        final int offset = 50;
        int offset2 = pa3.encode(me, offset);
        int len = offset2 - offset;
        // 142 is the
        Assert.assertEquals(PeerAddress.MAX_SIZE, PeerAddress.size(Utils.byteArrayToShort(me, offset)));
        Assert.assertEquals(PeerAddress.MAX_SIZE, len);
        //
        PeerAddress pa4 = PeerAddress.decode(me, offset).element0();

        compare(pa3, pa4);
    }

    /**
     * Compare two PeerAddress.
     * 
     * @param pa1
     *            The first PeerAddress
     * @param pa2
     *            The second PeerAddress
     */
    public static void compare(final PeerAddress pa1, final PeerAddress pa2) {
        Assert.assertEquals(pa1.peerId(), pa2.peerId());
        Assert.assertEquals(pa1.ipInternalNetworkPrefix(), pa2.ipInternalNetworkPrefix());
        Assert.assertEquals(pa1.holePunching(), pa2.holePunching());
        Assert.assertEquals(pa1.ipv4Flag(), pa2.ipv4Flag());
        Assert.assertEquals(pa1.ipv4Socket(), pa2.ipv4Socket());
        Assert.assertEquals(pa1.ipv6Flag(), pa2.ipv6Flag());
        Assert.assertEquals(pa1.ipv6Socket(), pa2.ipv6Socket());
        Assert.assertEquals(pa1.reachable4UDP(), pa2.reachable4UDP());
        Assert.assertEquals(pa1.reachable6UDP(), pa2.reachable6UDP());
        Assert.assertEquals(pa1.relays(), pa2.relays());
        Assert.assertEquals(pa1.relaySize(), pa2.relaySize());
        Assert.assertEquals(pa1.relayTypes(), pa2.relayTypes());
        Assert.assertEquals(pa1.size(), pa2.size());
    }
}
