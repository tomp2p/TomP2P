package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

import net.tomp2p.connection.DSASignatureFactory;

import org.junit.Assert;
import org.junit.Test;

public class TestData {
	
	private static final DSASignatureFactory factory = new DSASignatureFactory();
    @Test
    public void testData1() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data("test");
        AlternativeCompositeByteBuf transfer = AlternativeCompositeByteBuf.compBuffer();
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
    }
    
    @Test
    public void testData2Copy() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data(1, 100000);
        AlternativeCompositeByteBuf transfer = AlternativeCompositeByteBuf.compBuffer();
        data.encodeHeader(transfer, factory);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.decodeBuffer(pa);
        Assert.assertEquals(false, done);
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.decodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        transfer.writeBytes(data.buffer());
        data.encodeDone(transfer, factory);

        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
    }
    
    @Test
    public void testData2NoCopy() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data(1, 100000);
        AlternativeCompositeByteBuf transfer = AlternativeCompositeByteBuf.compBuffer();
        data.encodeHeader(transfer, factory);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.decodeBuffer(pa);
        Assert.assertEquals(false, done);
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.decodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        //now we need to reset, since our data is complete now
        data.resetAlreadyTransferred();
        
        data.encodeBuffer(transfer);
        data.encodeDone(transfer, factory);

        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
    }
    
    @Test
    public void testData3() throws IOException, ClassNotFoundException, InvalidKeyException, SignatureException {
        Data data = new Data(1, 100000);
        AlternativeCompositeByteBuf transfer = AlternativeCompositeByteBuf.compBuffer();
        data.encodeHeader(transfer, factory);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.decodeBuffer(pa);
        Assert.assertEquals(false, done);
        
        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.decodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        transfer.writeBytes(data.buffer());
        data.encodeDone(transfer, factory);

        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
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
        data.sign(keyPair1, factory);
        Data newData = encodeDecode(data);
        Assert.assertTrue(newData.verify(keyPair1.getPublic(), factory));
        Assert.assertFalse(newData.verify(keyPair2.getPublic(), factory));
        Assert.assertEquals(data, newData);
    }

	private Data encodeDecode(Data data) throws InvalidKeyException, SignatureException, IOException {
	    
		AlternativeCompositeByteBuf transfer = AlternativeCompositeByteBuf.compBuffer();
        data.encodeHeader(transfer, factory);
        data.encodeBuffer(transfer);
        data.encodeDone(transfer, factory);
        //
        Data newData = Data.decodeHeader(transfer, new DSASignatureFactory());
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer, null, factory);
        return newData;
    }
}
