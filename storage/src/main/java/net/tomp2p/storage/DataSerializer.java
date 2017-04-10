package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;
import java.security.SignatureException;

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
		if (value.length() > MAX_SIZE) {
			// header, 1 means stored on disk in a file
			out.writeByte(1);
			serializeFile(out, value);
		} else {
			// header, 0 means stored on disk with MapDB
			out.writeByte(0);
			serializeMapDB(out, value);
		}
	}

	private void serializeMapDB(DataOutput out, Data value) throws IOException {
	    ByteBuf acb = Unpooled.buffer();
	    // store data to disk
	    // header first
	    value.encodeHeader(acb, signatureFactory);
	    write(out, acb.nioBuffers());
	    acb.skipBytes(acb.writerIndex());
	    // next data - no need to copy to another buffer, just take the data
	    // from memory
	    write(out, value.toByteBuffers());
	    // rest
	    try {
	    	value.encodeDone(acb, signatureFactory);
	    	write(out, acb.nioBuffers());
	    } catch (InvalidKeyException e) {
	    	throw new IOException(e);
	    } catch (SignatureException e) {
	    	throw new IOException(e);
	    }
    }

	private void serializeFile(DataOutput out, Data value) throws IOException, FileNotFoundException {
	    Number160 hash = value.hash();
	    // store file name
	    out.write(hash.toByteArray());
	    // store as external file, create path
	    RandomAccessFile file = null;
	    FileChannel rwChannel = null;
	    ByteBuf acb = null;
	    try {
	    	file = new RandomAccessFile(new File(path, hash.toString()), "rw");
	    	rwChannel = file.getChannel();
	    	acb = Unpooled.buffer();
	    	// store data to disk
	    	// header first
	    	value.encodeHeader(acb, signatureFactory);
	    	rwChannel.write(acb.nioBuffers());
	    	// next data - no need to copy to another buffer, just take the
	    	// data from memory
	    	rwChannel.write(value.toByteBuffers());
	    	// rest
	    	try {
	    		value.encodeDone(acb, signatureFactory);
	    		rwChannel.write(acb.nioBuffers());
	    	} catch (InvalidKeyException e) {
	    		throw new IOException(e);
	    	} catch (SignatureException e) {
	    		throw new IOException(e);
	    	}
	    } finally {
	    	if (acb!=null) {
	    		acb.release();
	    	}
	    	if (rwChannel != null) {
	    		rwChannel.close();
	    	}
	    	if (file != null) {
	    		file.close();
	    	}
	    }
    }

	private void write(DataOutput out, ByteBuffer[] nioBuffers) throws IOException {
		final int length = nioBuffers.length; 
    	for(int i=0;i < length; i++) {
    		int remaining = nioBuffers[i].remaining();
    		if(nioBuffers[i].hasArray()) {
    			out.write(nioBuffers[i].array(), nioBuffers[i].arrayOffset(), remaining);
    		} else {
    			byte[] me = new byte[remaining];
    			nioBuffers[i].get(me);
    			out.write(me);
    		}
    	} 
    }

	@Override
    public Data deserialize(DataInput in, int available) throws IOException {
	    int header = in.readByte();
	    if(header == 1) {
	    	return deserializeFile(in);
	    } else if(header == 0) {
	    	return deserializeMapDB(in);
	    } else {
	    	throw new IOException("unexpected header: " + header);
	    }
    }

	private Data deserializeMapDB(DataInput in) throws IOException {
	    ByteBuf buf = Unpooled.buffer();
	    Data data = null;
	    while(data == null) {
	    	buf.writeByte(in.readByte());
	    	data = Data.decodeHeader(buf, signatureFactory);
	    }
	    int len = data.length();
	    byte me[] = new byte[len];
	    in.readFully(me);
	    buf = Unpooled.wrappedBuffer(me);
	    boolean retVal = data.decodeBuffer(buf);
	    if(!retVal) {
	    	throw new IOException("data could not be read");
	    }
	    if(data.isSigned()) {
	    	me = new byte[signatureFactory.signatureSize()];
	    	in.readFully(me);
	    	buf = Unpooled.wrappedBuffer(me);
	    }
	    retVal = data.decodeDone(buf, signatureFactory);
	    if(!retVal) {
	    	throw new IOException("signature could not be read");
	    }
	    return data;
    }

	private Data deserializeFile(DataInput in) throws IOException, FileNotFoundException {
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
    }

	@Override
    public int fixedSize() {
	    return -1;
    }
}
