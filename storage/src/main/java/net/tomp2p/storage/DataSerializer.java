package net.tomp2p.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.mapdb.Serializer;

public class DataSerializer implements Serializer<Data>, Serializable {

    private static final long serialVersionUID = 1428836065493792295L;
    //TODO: test the performance impact
    private static final int MAX_SIZE = 10 * 1024;
    
    final private File path;
    
    public DataSerializer(File path) {
    	this.path = path;
    }

	@Override
    public void serialize(DataOutput out, Data value) throws IOException {
	    if(value.length() > MAX_SIZE) {
	    	//store as external file, create path
	    	
	    	//get length of path, store it
	    	
	    	//store data to disk
	    	
	    	//store file name
	    	
	    } else {
	    	//store internally
	    	out.writeByte(0);
	    	ByteBuffer[] buffers = value.toByteBuffers();
	    	final int length =buffers.length; 
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
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    public int fixedSize() {
	    return -1;
    }

}
