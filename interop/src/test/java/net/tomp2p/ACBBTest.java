package net.tomp2p;

import io.netty.buffer.ByteBuf;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.junit.Test;

public class ACBBTest {

	@Test
	public void clearTest()
	{
		ByteBuf acbb = AlternativeCompositeByteBuf.compBuffer();
		acbb.clear();
	}
}
