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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

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

    /**
     * Test serialization and deserialization of PeerAddress.
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress() throws UnknownHostException {
        Number160 id = new Number160(RND.nextInt());
        InetAddress address = InetAddress.getByName("127.0.0.1");
        int portTCP = RND.nextInt(BIT_16);
        int portUDP = RND.nextInt(BIT_16);
        PeerAddress pa = new PeerAddress(id, address, portTCP, portUDP);
        byte[] me = pa.toByteArray();
        PeerAddress pa2 = new PeerAddress(me);
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
        int portTCP = RND.nextInt(BIT_16);
        int portUDP = RND.nextInt(BIT_16);
        PeerAddress pa = new PeerAddress(id, address, portTCP, portUDP);
        byte[] me = pa.toByteArray();
        PeerAddress pa2 = new PeerAddress(me);
        compare(pa, pa2);
    }

    /**
     * Test serialization and deserialization of PeerAddress.
     * 
     * @throws UnknownHostException .
     */
    @Test
    public void testPeerAddress3() throws UnknownHostException {
        PeerAddress pa1 = new PeerAddress(new Number160("0x857e35a42e444522456"),
                InetAddress.getByName("192.168.230.230"), RND.nextInt(BIT_16), RND.nextInt(BIT_16));
        PeerAddress pa2 = new PeerAddress(new Number160("0x657435a424444522456"),
                InetAddress.getByName("192.168.240.230"), RND.nextInt(BIT_16), RND.nextInt(BIT_16));
        final int length = 200;
        byte[] me = new byte[length];
        final int offset = 50;
        int offset2 = pa1.toByteArray(me, offset);
        pa2.toByteArray(me, offset2);
        //
        PeerAddress pa3 = new PeerAddress(me, offset);
        int offset4 = pa3.offset();
        PeerAddress pa4 = new PeerAddress(me, offset4);
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
        PeerAddress pa1 = new PeerAddress(new Number160("0x857e35a42e444522456"),
                InetAddress.getByName("0123:4567:89ab:cdef:0123:4567:89ab:cdef"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16));
        PeerAddress pa2 = new PeerAddress(new Number160("0x657435a424444522456"),
                InetAddress.getByName("0123:4567:89ab:cdef:0123:4567:89ab:cdef"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16));

        final int length = 200;
        byte[] me = new byte[length];
        final int offset = 50;
        int offset2 = pa1.toByteArray(me, offset);
        pa2.toByteArray(me, offset2);
        //
        PeerAddress pa3 = new PeerAddress(me, offset);
        int offset4 = pa3.offset();
        PeerAddress pa4 = new PeerAddress(me, offset4);
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
        int i = 0;
        psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.230"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("2123:4567:89ab:cdef:0123:4567:89ab:cde2"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.231"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("4123:4567:89ab:cdef:0123:4567:89ab:cde4"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("192.168.230.232"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16)));
        PeerAddress pa3 = new PeerAddress(new Number160("0x657435a424444522456"), new PeerSocketAddress(
                InetAddress.getByName("192.168.230.236"), RND.nextInt(BIT_16), RND.nextInt(BIT_16)), true, true, true,
                psa);

        final int length = 200;
        byte[] me = new byte[length];
        final int offset = 50;
        pa3.toByteArray(me, offset);
        PeerAddress pa4 = new PeerAddress(me, offset);

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
        int i = 0;
        psa.add(new PeerSocketAddress(InetAddress.getByName("1123:4567:89ab:cdef:0123:4567:89ab:cde1"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("2123:4567:89ab:cdef:0123:4567:89ab:cde2"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("3123:4567:89ab:cdef:0123:4567:89ab:cde3"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("4123:4567:89ab:cdef:0123:4567:89ab:cde4"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("5123:4567:89ab:cdef:0123:4567:89ab:cde5"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        PeerAddress pa3 = new PeerAddress(new Number160("0x657435a424444522456"), new PeerSocketAddress(
                InetAddress.getByName("1123:4567:89ab:cdef:0123:4567:89ab:cde0"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16)), true, true, true, psa);

        final int length = 200;
        byte[] me = new byte[length];
        final int offset = 50;
        int offset2 = pa3.toByteArray(me, offset);
        int len = offset2 - offset;
        // 142 is the
        Assert.assertEquals(PeerAddress.MAX_SIZE, PeerAddress.size(pa3.getOptions(), pa3.getRelays()));
        Assert.assertEquals(PeerAddress.MAX_SIZE, len);
        //
        PeerAddress pa4 = new PeerAddress(me, offset);

        compare(pa3, pa4);
    }
    
    @Test
    public void testPeerAddress7() throws UnknownHostException {

        Collection<PeerSocketAddress> psa = new ArrayList<PeerSocketAddress>();
        int i = 0;
        psa.add(new PeerSocketAddress(InetAddress.getByName("1123:4567:89ab:cdef:0123:4567:89ab:cde1"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("2123:4567:89ab:cdef:0123:4567:89ab:cde2"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("3123:4567:89ab:cdef:0123:4567:89ab:cde3"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("4123:4567:89ab:cdef:0123:4567:89ab:cde4"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        psa.add(new PeerSocketAddress(InetAddress.getByName("5123:4567:89ab:cdef:0123:4567:89ab:cde5"),
                RND.nextInt(BIT_16), RND.nextInt(BIT_16)));
        PeerAddress pa3 = new PeerAddress(new Number160("0x657435a424444522456"), new PeerSocketAddress(
                InetAddress.getByName("1123:4567:89ab:cdef:0123:4567:89ab:cde0"), RND.nextInt(BIT_16),
                RND.nextInt(BIT_16)), true, true, true, psa);
        
        Assert.assertEquals(142, pa3.toByteArray().length);

    }

    /**
     * Compare two PeerAddress.
     * 
     * @param pa1
     *            The first PeerAddress
     * @param pa2
     *            The second PeerAddress
     */
    private void compare(final PeerAddress pa1, final PeerAddress pa2) {
        Assert.assertEquals(pa1.getPeerId(), pa2.getPeerId());
        Assert.assertEquals(pa1.createSocketTCP().getPort(), pa2.createSocketTCP().getPort());
        Assert.assertEquals(pa1.createSocketTCP(), pa2.createSocketTCP());
        Assert.assertEquals(pa1.createSocketUDP().getPort(), pa2.createSocketUDP().getPort());
        Assert.assertEquals(pa1.createSocketUDP(), pa2.createSocketUDP());
        Assert.assertEquals(pa1.isFirewalledTCP(), pa2.isFirewalledTCP());
        Assert.assertEquals(pa1.isFirewalledUDP(), pa2.isFirewalledUDP());
        Assert.assertEquals(pa1.isIPv6(), pa2.isIPv6());
        Assert.assertEquals(pa1.isRelayed(), pa2.isRelayed());
        Assert.assertEquals(pa1.getOptions(), pa2.getOptions());
        Assert.assertEquals(pa1.getRelays(), pa2.getRelays());
        List<PeerSocketAddress> psa1 = new ArrayList<PeerSocketAddress>(pa1.getPeerSocketAddresses());
        List<PeerSocketAddress> psa2 = new ArrayList<PeerSocketAddress>(pa2.getPeerSocketAddresses());
        Assert.assertEquals(psa1.size(), psa2.size());
        for (int i = 0; i < psa1.size(); i++) {
            Assert.assertEquals(psa1.get(i), psa2.get(i));
        }

    }
}
