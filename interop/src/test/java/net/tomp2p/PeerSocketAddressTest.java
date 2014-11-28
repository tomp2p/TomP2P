package net.tomp2p;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;

import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.junit.Test;

public class PeerSocketAddressTest {

	@Test
	public void encodeDecodeTest() throws Exception {
		
		InetAddress sampleAddress1 = InetAddress.getByName("192.168.1.1");
		InetAddress sampleAddress2 = InetAddress.getByName("255.255.255.255");
		InetAddress sampleAddress3 = InetAddress.getByName("127.0.0.1");
		InetAddress sampleAddress4 = InetAddress.getByName("0:1:2:3:4:5:6:7");
		InetAddress sampleAddress5 = InetAddress.getByName("7:6:5:4:3:2:1:0");
		
		PeerSocketAddress psa1 = new PeerSocketAddress(sampleAddress1, 0, 0);
		PeerSocketAddress psa2 = new PeerSocketAddress(sampleAddress2, 65535, 65535);
		PeerSocketAddress psa3 = new PeerSocketAddress(sampleAddress3, 1, 1);
		PeerSocketAddress psa4 = new PeerSocketAddress(sampleAddress4, 2, 2);
		PeerSocketAddress psa5 = new PeerSocketAddress(sampleAddress5, 30, 40);
		PeerSocketAddress psa6 = new PeerSocketAddress(sampleAddress1, 88, 88);
		PeerSocketAddress psa7 = new PeerSocketAddress(sampleAddress2, 177, 177);
		PeerSocketAddress psa8 = new PeerSocketAddress(sampleAddress3, 60000, 65000);
		PeerSocketAddress psa9 = new PeerSocketAddress(sampleAddress4, 99, 100);
		PeerSocketAddress psa10 = new PeerSocketAddress(sampleAddress5, 13, 1234);
		
		// encode
		byte[] psa1_e = psa1.toByteArray();
		byte[] psa2_e = psa2.toByteArray();
		byte[] psa3_e = psa3.toByteArray();
		byte[] psa4_e = psa4.toByteArray();
		byte[] psa5_e = psa5.toByteArray();
		byte[] psa6_e = psa6.toByteArray();
		byte[] psa7_e = psa7.toByteArray();
		byte[] psa8_e = psa8.toByteArray();
		byte[] psa9_e = psa9.toByteArray();
		byte[] psa10_e = psa10.toByteArray();
		
		// decode
		AlternativeCompositeByteBuf buf1 = AlternativeCompositeByteBuf.compBuffer();
		buf1.writeBytes(psa1_e);
		PeerSocketAddress psa1_d = PeerSocketAddress.create(buf1, true);
		
		AlternativeCompositeByteBuf buf2 = AlternativeCompositeByteBuf.compBuffer();
		buf2.writeBytes(psa2_e);
		PeerSocketAddress psa2_d = PeerSocketAddress.create(buf2, true);
		
		AlternativeCompositeByteBuf buf3 = AlternativeCompositeByteBuf.compBuffer();
		buf3.writeBytes(psa3_e);
		PeerSocketAddress psa3_d = PeerSocketAddress.create(buf3, true);
		
		AlternativeCompositeByteBuf buf4 = AlternativeCompositeByteBuf.compBuffer();
		buf4.writeBytes(psa4_e);
		PeerSocketAddress psa4_d = PeerSocketAddress.create(buf4, false);
		
		AlternativeCompositeByteBuf buf5 = AlternativeCompositeByteBuf.compBuffer();
		buf5.writeBytes(psa5_e);
		PeerSocketAddress psa5_d = PeerSocketAddress.create(buf5, false);
		
		AlternativeCompositeByteBuf buf6 = AlternativeCompositeByteBuf.compBuffer();
		buf6.writeBytes(psa6_e);
		PeerSocketAddress psa6_d = PeerSocketAddress.create(buf6, true);
		
		AlternativeCompositeByteBuf buf7 = AlternativeCompositeByteBuf.compBuffer();
		buf7.writeBytes(psa7_e);
		PeerSocketAddress psa7_d = PeerSocketAddress.create(buf7, true);
		
		AlternativeCompositeByteBuf buf8 = AlternativeCompositeByteBuf.compBuffer();
		buf8.writeBytes(psa8_e);
		PeerSocketAddress psa8_d = PeerSocketAddress.create(buf8, true);
		
		AlternativeCompositeByteBuf buf9 = AlternativeCompositeByteBuf.compBuffer();
		buf9.writeBytes(psa9_e);
		PeerSocketAddress psa9_d = PeerSocketAddress.create(buf9, false);
		
		AlternativeCompositeByteBuf buf10 = AlternativeCompositeByteBuf.compBuffer();
		buf10.writeBytes(psa10_e);
		PeerSocketAddress psa10_d = PeerSocketAddress.create(buf10, false);
		
		
		// compare
		assertEquals(psa1, psa1_d);
		assertEquals(psa2, psa2_d);
		assertEquals(psa3, psa3_d);
		assertEquals(psa4, psa4_d);
		assertEquals(psa5, psa5_d);
		assertEquals(psa6, psa6_d);
		assertEquals(psa7, psa7_d);
		assertEquals(psa8, psa8_d);
		assertEquals(psa9, psa9_d);
		assertEquals(psa10, psa10_d);
	}
}
