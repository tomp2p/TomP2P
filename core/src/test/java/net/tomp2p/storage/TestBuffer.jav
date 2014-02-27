package net.tomp2p.storage;


import static io.netty.buffer.Unpooled.directBuffer;
import static io.netty.util.ReferenceCountUtil.releaseLater;
import static org.junit.Assert.assertEquals;
import io.netty.buffer.AbstractByteBufTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestBuffer extends AbstractByteBufTest{
	
	@Test
	public void testWriteOrig() {
		CompositeByteBuf cbb = Unpooled.compositeBuffer();
		byte[] me = new byte[]{1,2,3,4,5};
		
		for(int i=0;i<1000;i++) {
			if(i==8) {
				System.out.println("here");
			}
			cbb.writeInt(-2);
			cbb.writeBytes(me);
		}
		
		for(int i=0;i<1000;i++) {
			int read = cbb.readInt();
			byte[] readme = new byte[5];
			cbb.readBytes(readme);
		
			Assert.assertEquals(-2, read);
			Assert.assertEquals(true, Arrays.equals(readme, me));
		}
	}
	
	@Test
	public void testWrite() {
		AlternativeCompositeByteBuf cbb = AlternativeCompositeByteBuf.compBuffer();
		byte[] me = new byte[]{1,2,3,4};
		
		for(int i=0;i<1000;i++) {
			if(i==8) {
				System.out.println("here");
			}
			cbb.writeInt(-2);
			cbb.writeBytes(me);
		}
		
		for(int i=0;i<1000;i++) {
			int read = cbb.readInt();
			byte[] readme = new byte[4];
			cbb.readBytes(readme);
		
			Assert.assertEquals(-2, read);
			Assert.assertEquals(true, Arrays.equals(readme, me));
		}
	}
	
	@Test
	public void testWrite2() {
		int countr = 0;
		AlternativeCompositeByteBuf cbb = AlternativeCompositeByteBuf.compBuffer();
		byte[] me = new byte[]{1,2,3,4,5};
		
		for(int i=0;i<1000;i++) {
			if(i==14) {
				System.out.println("here");
			}
			if(!cbb.sync()) {
				System.err.println("nooo!");
			}
			cbb.writeInt(-2);
			countr +=4;
			System.err.println("wrote1 "+countr+ "/ "+i);
			cbb.writeBytes(me);
			countr +=5;
			System.err.println("wrote1 "+countr+ "/ "+i+ "==> "+ cbb.writerIndex());
		}
		
		for(int i=0;i<1000;i++) {
			if(i==999) {
				System.err.println("aoeu "+countr+ "/ "+i);
			}
			int read = cbb.readInt();
			byte[] readme = new byte[5];
			cbb.readBytes(readme);
		
			Assert.assertEquals(-2, read);
			Assert.assertEquals(true, Arrays.equals(readme, me));
		}
	}
	
	@Test
	public void testWrite3() {
		AlternativeCompositeByteBuf cbb = AlternativeCompositeByteBuf.compBuffer();
		cbb.writeInt(-2);
		ByteBuf b = Unpooled.buffer();
		b.writeInt(-3);
		cbb.addComponent(b);
		cbb.writeInt(-4);
		ByteBuf b1 = Unpooled.buffer();
		b1.writeInt(-5);
		cbb.addComponent(b1);
		//b.writeInt(-6);
		
		Assert.assertEquals(-2, cbb.readInt());
		Assert.assertEquals(-3, cbb.readInt());
		//yes we can insert
		//Assert.assertEquals(-6, cbb.readInt());
		Assert.assertEquals(-4, cbb.readInt());
		Assert.assertEquals(-5, cbb.readInt());
		
	}

	@Override
	protected ByteBuf newBuffer(int capacity) {
		ByteBuf init = Unpooled.buffer();
		//init.capacity(capacity).writerIndex(capacity);
		init.capacity(capacity);
		return AlternativeCompositeByteBuf.compBuffer(init);
	}

	@Override
	protected ByteBuf[] components() {
		return new ByteBuf[]{AlternativeCompositeByteBuf.compBuffer()};
	}
	
	@Override
	@Test
    public void getDirectByteBufferState() {
        
		AlternativeCompositeByteBuf buffer = AlternativeCompositeByteBuf.compBuffer();
        buffer.capacity(4).writerIndex(4);
		
		ByteBuffer dst = ByteBuffer.allocateDirect(4);
        dst.position(1);
        dst.limit(3);

        buffer.setByte(0, (byte) 1);
        buffer.setByte(1, (byte) 2);
        buffer.setByte(2, (byte) 3);
        buffer.setByte(3, (byte) 4);
        buffer.getBytes(1, dst);

        assertEquals(3, dst.position());
        assertEquals(3, dst.limit());

        dst.clear();
        assertEquals(0, dst.get(0));
        assertEquals(2, dst.get(1));
        assertEquals(3, dst.get(2));
        assertEquals(0, dst.get(3));
    }
	
	@Override
	@Test
    public void getByteBufferState() {
		
		AlternativeCompositeByteBuf buffer = AlternativeCompositeByteBuf.compBuffer();
        buffer.capacity(4).writerIndex(4);
		
        ByteBuffer dst = ByteBuffer.allocate(4);
        dst.position(1);
        dst.limit(3);

        buffer.setByte(0, (byte) 1);
        buffer.setByte(1, (byte) 2);
        buffer.setByte(2, (byte) 3);
        buffer.setByte(3, (byte) 4);
        buffer.getBytes(1, dst);

        assertEquals(3, dst.position());
        assertEquals(3, dst.limit());

        dst.clear();
        assertEquals(0, dst.get(0));
        assertEquals(2, dst.get(1));
        assertEquals(3, dst.get(2));
        assertEquals(0, dst.get(3));
    }
	
	@Override
	@Test
    public void testRandomByteBufferTransfer() {
		
		int BLOCK_SIZE = 128;
		AlternativeCompositeByteBuf buffer = AlternativeCompositeByteBuf.compBuffer();
        buffer.capacity(4096);
        long seed = 42;
        Random random = new Random(seed);
		
        ByteBuffer value = ByteBuffer.allocate(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value.array());
            value.clear().position(random.nextInt(BLOCK_SIZE));
            value.limit(value.position() + BLOCK_SIZE);
            buffer.setBytes(i, value);
        }
        
        //increase writer index
        buffer.writerIndex(4096);

        random.setSeed(seed);
        ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue.array());
            int valueOffset = random.nextInt(BLOCK_SIZE);
            value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
            buffer.getBytes(i, value);
            assertEquals(valueOffset + BLOCK_SIZE, value.position());
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.get(j), value.get(j));
            }
        }
    }
	
	@Test
    public void testRandomDirectBufferTransfer() {
		
		int BLOCK_SIZE = 128;
		AlternativeCompositeByteBuf buffer = AlternativeCompositeByteBuf.compBuffer();
        buffer.capacity(4096);
        long seed = 42;
        Random random = new Random(seed);
		
        byte[] tmp = new byte[BLOCK_SIZE * 2];
        ByteBuf value = releaseLater(directBuffer(BLOCK_SIZE * 2));
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(tmp);
            value.setBytes(0, tmp, 0, value.capacity());
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }
        
      //increase writer index
        buffer.writerIndex(4096);

        random.setSeed(seed);
        ByteBuf expectedValue = releaseLater(directBuffer(BLOCK_SIZE * 2));
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(tmp);
            expectedValue.setBytes(0, tmp, 0, expectedValue.capacity());
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), value.getByte(j));
            }
        }
    }
}
