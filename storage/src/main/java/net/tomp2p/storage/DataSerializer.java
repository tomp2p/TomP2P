package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.peers.Number160;

import org.mapdb.Serializer;

public class DataSerializer implements Serializer<Data>, Serializable {

    private static final long serialVersionUID = 1428836065493792295L;
    //TODO: test the performance impact
    private static final int MAX_SIZE = 10 * 1024;
    
    final private File path;
    final private SignatureFactory signatureFactory;
    
    public DataSerializer(File path, SignatureFactory signatureFactory) {
    	this.path = path;
    	this.signatureFactory = signatureFactory;
    }

	@Override
    public void serialize(DataOutput out, Data value) throws IOException {
	    if(value.length() > MAX_SIZE) {
	    	//header, 1 means stored on disk in a file
	    	out.writeByte(1);
	    	Number160 hash = value.hash();
	    	//store file name
	    	out.write(hash.toByteArray());
	    	//store as external file, create path
	    	RandomAccessFile file = new RandomAccessFile(new File(path, hash.toString()), "rw");
	    	FileChannel rwChannel = file.getChannel();
	    	ByteBuffer[] buffers = value.toByteBuffers();
	    	final int length =buffers.length;
	    	//store data to disk
	    	for(int i=0;i < length; i++) {
	    		rwChannel.write(buffers[i]);
	    	}
	    	rwChannel.close();
	    	file.close();
	    } else {
	    	//header, 0 means stored on disk with MapDB
	    	out.writeByte(0);
	    	//value.encodeHeader(buf, signatureFactory);
	    	ByteBuffer[] buffers = value.toByteBuffers();
	    	final int length = buffers.length; 
	    	for(int i=0;i < length; i++) {
	    		int remaining = buffers[i].remaining();
	    		if(buffers[i].hasArray()) {
	    			out.write(buffers[i].array(), buffers[i].arrayOffset(), remaining);
	    		} else {
	    			byte[] me = new byte[remaining];
	    			buffers[i].get(me);
	    			out.write(me);
	    		}
	    	}
	    }
    }

	@Override
    public Data deserialize(DataInput in, int available) throws IOException {
		//if(available < 1) {
		//	throw new IOException("need to be able to read 1 byte, I could read: " + available);
		//}
	    int header = in.readByte();
	    if(header == 1) {
	    	//if(available != 21) {
	    	//	throw new IOException("need to be able to read 21 bytes, I could read: " + available);
	    	//}
	    	byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
	    	in.readFully(me);
	    	Number160 hash = new Number160(me);
	    	RandomAccessFile file = new RandomAccessFile(new File(path, hash.toString()), "r");
	    	FileChannel inChannel = file.getChannel();
	        MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
	        buffer.load();
	        ByteBuf buf = Unpooled.wrappedBuffer(buffer);
	        Data data = Data.decodeHeader(buf, signatureFactory);
	        data.decodeBuffer(buf);
	        data.decodeDone(buf, signatureFactory);
	        file.close();
	        return data;
	    } else if(header == 0) {
	    	ByteBuf buf = Unpooled.buffer();
	    	Data data = null;
	    	while(data == null) {
	    		buf.writeByte(in.readByte());
	    		data = Data.decodeHeader(buf, signatureFactory);
	    	}
	    	int len = data.length();
	    	byte me[] = new byte[len];
	    	buf = Unpooled.wrappedBuffer(me);
	        boolean retVal = data.decodeBuffer(buf);
	        if(!retVal) {
	        	throw new IOException("data could not be read");
	        }
	        retVal = data.decodeDone(buf, signatureFactory);
	        if(!retVal) {
	        	throw new IOException("signature could not be read");
	        }
	        
	    	return data;
	    } else {
	    	throw new IOException("unexpected header: " + header);
	    }
    }

	@Override
    public int fixedSize() {
	    return -1;
    }

}
