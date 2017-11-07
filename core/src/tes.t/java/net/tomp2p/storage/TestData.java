package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Random;
import java.util.UUID;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestData {
	
	private static final DSASignatureFactory factory = new DSASignatureFactory();
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };
	
    @Test
    public void testData1() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data("test");
        CompositeByteBuf transfer = Unpooled.compositeBuffer();
        data.encodeHeader(transfer, factory);
        //no need to call encodeBuffer with Data(object) or Data(buffer)
        data.encodeBuffer(transfer);
        data.encodeDone(transfer, factory);
        
        //for the decoding we need a flat bytebuf
        ByteBuf transfer2 = Unpooled.buffer();
        transfer2.writeBytes(transfer);

        Data newData = Data.decodeHeader(transfer2, new DSASignatureFactory());
        newData.decodeBuffer(transfer2);
        newData.decodeDone(transfer2, null, factory);

        Assert.assertEquals(data, newData);
        Object test = newData.object();
        Assert.assertEquals("test", test);
        transfer.release();
        transfer2.release();
    }
    
    @Test	
    public void clearTest()	{		
    	ByteBuf acbb =  Unpooled.compositeBuffer();	
    	acbb.clear();
    	Assert.assertEquals(0, acbb.readerIndex());
    	Assert.assertEquals(0, acbb.writerIndex());
    	acbb.release();
    }
    
    @Test
    public void testData2Copy() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data(1, 100000);
        CompositeByteBuf transfer = Unpooled.compositeBuffer();
        data.encodeHeader(transfer, factory);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.decodeBuffer(pa);
        Assert.assertEquals(false, done);
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.decodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        ByteBuf writeBuf = data.buffer();
        transfer.writeBytes(writeBuf);
        data.encodeDone(transfer, factory);

        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
        
        transfer.release();
        pa.release();
        pa1.release();
        test.release();
        writeBuf.release();
    }
    
    @Test
    public void testData2NoCopy() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data(1, 100000);
        CompositeByteBuf transfer = Unpooled.compositeBuffer();
        data.encodeHeader(transfer, factory);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.decodeBuffer(pa);
        Assert.assertEquals(false, done);
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.decodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        
        data.encodeBuffer(transfer);
        data.encodeDone(transfer, factory);

        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
        
        transfer.release();
        pa.release();
        pa1.release();
        test.release();
    }
    
    @Test
    public void testData3() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data(1, 100000);
        CompositeByteBuf transfer = Unpooled.compositeBuffer();
        data.encodeHeader(transfer, factory);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.decodeBuffer(pa);
        Assert.assertEquals(false, done);
        
        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.decodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        ByteBuf writeBuf = data.buffer();
        transfer.writeBytes(writeBuf);
        data.encodeDone(transfer, factory);

        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
        
        transfer.release();
        pa.release();
        pa1.release();
        test.release();
        writeBuf.release();
    }
    
    @Test
    public void testData4() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data(1, 100000);
        Data newData = encodeDecode(data);
        Assert.assertEquals(data, newData);
    }
    
    @Test
    public void testData5() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
    	
    	KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair keyPair1 = gen.generateKeyPair();
    	
        Data data = new Data(1, 100000);
        data.publicKey(keyPair1.getPublic());
        Data newData = encodeDecode(data);
        Assert.assertEquals(data, newData);
    }
    
    @Test
    public void testData6() throws IOException, ClassNotFoundException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
    	
    	KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair keyPair1 = gen.generateKeyPair();
        KeyPair keyPair2 = gen.generateKeyPair();
        
    	Data data = new Data(new byte[10000]);
        data.signNow(keyPair1, factory);
        Data newData = encodeDecode(data);
        Assert.assertTrue(newData.verify(keyPair1.getPublic(), factory));
        Assert.assertFalse(newData.verify(keyPair2.getPublic(), factory));
        Assert.assertEquals(data, newData);
    }
    
    @Test
    public void testData7() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data().flag1();
        Data newData = encodeDecode(data);
        Assert.assertEquals(data, newData);
        Assert.assertEquals(true, newData.isFlag1());
    }
    
    @Test
    public void testData8() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data().flag2();
        Data newData = encodeDecode(data);
        Assert.assertEquals(-1, newData.ttlSeconds());
        Assert.assertEquals(data, newData);
        Assert.assertEquals(true, newData.isFlag2());
    }

    @Test
    public void testDataBasedOn1() throws IOException, ClassNotFoundException, InvalidKeyException,
            SignatureException {
        Data data = new Data();
        data.addBasedOn(Number160.ZERO);
        Data newData = encodeDecode(data);
        Assert.assertEquals(data.basedOnSet(), newData.basedOnSet());
        Assert.assertEquals(data, newData);

        data = new Data();
        data.addBasedOn(Number160.ONE);
        newData = encodeDecode(data);
        Assert.assertEquals(data.basedOnSet(), newData.basedOnSet());
        Assert.assertEquals(data, newData);

        data = new Data();
        data.addBasedOn(Number160.MAX_VALUE);
        newData = encodeDecode(data);
        Assert.assertEquals(data.basedOnSet(), newData.basedOnSet());
        Assert.assertEquals(data, newData);
    }

    @Test
    public void testDataBasedOn2() throws IOException, ClassNotFoundException, InvalidKeyException,
            SignatureException {
        Data data = new Data();
        Random random = new Random();
        for (int i = 0; i < 255; i++)
            data.addBasedOn(new Number160(random));
        Data newData = encodeDecode(data);
        Assert.assertEquals(data.basedOnSet(), newData.basedOnSet());
        Assert.assertEquals(data, newData);
    }

    @Test
    public void testDataBasedOn3() throws IOException, ClassNotFoundException, InvalidKeyException,
            SignatureException, NoSuchAlgorithmException {
        Random random = new Random();
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair keyPair1 = gen.generateKeyPair();
        KeyPair keyPair2 = gen.generateKeyPair();

        Data data = new Data(UUID.randomUUID().toString());
        data.ttlSeconds(random.nextInt());
        for (int i = 0; i < 255; i++) {
            data.addBasedOn(new Number160(random));
        }
        // data.setProtectedEntry().publicKey(gen.generateKeyPair().getPublic());
        data.signNow(keyPair1, factory);
        Data newData = encodeDecode(data);

        Assert.assertEquals(data.object(), newData.object());
        Assert.assertEquals(data.ttlSeconds(), newData.ttlSeconds());
        Assert.assertEquals(data.basedOnSet(), newData.basedOnSet());
        Assert.assertTrue(newData.verify(keyPair1.getPublic(), factory));
        Assert.assertFalse(newData.verify(keyPair2.getPublic(), factory));
        Assert.assertEquals(data, newData);
    }

    @Test
    public void testDataBasedOn4() throws IOException, ClassNotFoundException, InvalidKeyException,
            SignatureException, NoSuchAlgorithmException {
        Random random = new Random();
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair keyPair1 = gen.generateKeyPair();

        Data data = new Data(UUID.randomUUID().toString());
        data.ttlSeconds(random.nextInt());
        for (int i = 0; i < 255; i++) {
            data.addBasedOn(new Number160(random));
        }
        data.protectEntry(keyPair1).publicKey(keyPair1.getPublic());
        Data newData = encodeDecode(data);

        Assert.assertEquals(data.object(), newData.object());
        Assert.assertEquals(data.ttlSeconds(), newData.ttlSeconds());
        Assert.assertEquals(data.basedOnSet(), newData.basedOnSet());
        Assert.assertEquals(data, newData);
    }
    
    @Test
    public void testDataDeleted() throws IOException, ClassNotFoundException, InvalidKeyException,
            SignatureException, NoSuchAlgorithmException {
        Data data = new Data(UUID.randomUUID().toString());
        data.deleted();
        Data newData = encodeDecode(data);

        Assert.assertEquals(data.isDeleted(), newData.isDeleted());
        Assert.assertEquals(data, newData);
    }
    
	@Test
	public void testDataDeleted1() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException,
	        NoSuchAlgorithmException, IllegalArgumentException {
		Data data = new Data(UUID.randomUUID().toString());
		data.deleted();
		try {
			data.flag1();
			Assert.fail("should throw an exception");
		} catch (IllegalArgumentException e) {}
	}
	
	@Test
	public void testDataDeleted2() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException,
	        NoSuchAlgorithmException, IllegalArgumentException {
		Data data = new Data(UUID.randomUUID().toString());
		data.flag2();
		try {
			data.deleted();
			Assert.fail("should throw an exception");
		} catch (IllegalArgumentException e) {}
	}

	private Data encodeDecode(Data data) throws InvalidKeyException, SignatureException, IOException {
	    
	CompositeByteBuf transfer = Unpooled.compositeBuffer();
        data.encodeHeader(transfer, factory);
        data.encodeBuffer(transfer);
        data.encodeDone(transfer, factory);
        //
        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);
        transfer.release();
        return newData;
    }
}
