package net.tomp2p.dht;

public class UtilsDHT {
	
    
    public static int dataSize(RemoveBuilder builder) {
	    if (builder.contentKeys()!=null) {
	    	return builder.contentKeys().size();
	    }
	    //we don't know how much, at least one.
	    return 1;
    }
}
