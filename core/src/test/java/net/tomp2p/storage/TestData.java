package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class TestData {
    @Test
    public void testData1() throws IOException, ClassNotFoundException {
        Data data = new Data("test", 12, 34, true, true);
        ByteBuf transfer = Unpooled.buffer();
        data.encode(transfer);
        //no need to call encodeBuffer with Data(object) or Data(buffer)
        //data.encodeBuffer(transfer);
        data.encodeDone(transfer);

        Data newData = Data.decodeHeader(transfer);
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer);

        Assert.assertEquals(data, newData);
        Object test = newData.object();
        Assert.assertEquals("test", test);
    }
    
    @Test
    public void testData2() throws IOException, ClassNotFoundException {
        Data data = new Data(100000, 12, 34, true, true);
        ByteBuf transfer = Unpooled.buffer();
        data.encode(transfer);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.encodeBuffer(pa);
        Assert.assertEquals(false, done);
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.encodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        transfer.writeBytes(data.buffer());
        data.encodeDone(transfer);

        Data newData = Data.decodeHeader(transfer);
        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
    }
    
    @Test
    public void testData3() throws IOException, ClassNotFoundException {
        Data data = new Data(100000, 12, 34, true, true);
        ByteBuf transfer = Unpooled.buffer();
        data.encode(transfer);
        ByteBuf pa = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done = data.encodeBuffer(pa);
        Assert.assertEquals(false, done);
        
        Data newData = Data.decodeHeader(transfer);
        
        ByteBuf pa1 = Unpooled.wrappedBuffer(new byte[50000]);
        boolean done1 = data.encodeBuffer(pa1);
        Assert.assertEquals(true, done1);
        transfer.writeBytes(data.buffer());
        data.encodeDone(transfer);

        newData.decodeBuffer(transfer);
        newData.decodeDone(transfer);

        Assert.assertEquals(data, newData);
        ByteBuf test = newData.buffer();
        Assert.assertEquals(100000, test.readableBytes());
    }
}
