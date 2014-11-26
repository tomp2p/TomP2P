package net.tomp2p;

import static org.junit.Assert.*;

import java.net.InetAddress;

import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.junit.Test;

public class PeerSocketAddressTest {

	@Test
	public void encodeDecodeTest() throws Exception {
		
		InetAddress sampleIp = InetAddress.getByName("192.168.1.1");
		
		int samplePort1 = 0;		// min
		int samplePort2 = 65535;	// max
		int samplePort3 = -1;		// invalid, but used as default in PeerAddress constructors
		
		PeerSocketAddress psa1 = new PeerSocketAddress(sampleIp, samplePort1, samplePort1);
		PeerSocketAddress psa2 = new PeerSocketAddress(sampleIp, samplePort2, samplePort2);
		PeerSocketAddress psa3 = new PeerSocketAddress(sampleIp, samplePort3, samplePort3);
		
		// encode
		byte[] psa1_e = psa1.toByteArray();
		byte[] psa2_e = psa2.toByteArray();
		byte[] psa3_e = psa3.toByteArray();
		
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
		
		// compare
		assertEquals(psa1, psa1_d);
		assertEquals(psa2, psa2_d);
		assertEquals(psa3, psa3_d);
	}
}
