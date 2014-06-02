package net.tomp2p.dht;

public class UtilsDHT {
	public static int dataSize(PutBuilder putBuilder) {
    	if(putBuilder.isPutMeta() && putBuilder.changePublicKey()!=null) {
    		//we only send a marker
    		return 1;
    	} else if(putBuilder.getDataMap()!=null) {
            return putBuilder.getDataMap().size();
        } else { 
            return putBuilder.getDataMapContent().size();
        }
    }
    
    public static int dataSize(RemoveBuilder builder) {
	    if (builder.contentKeys()!=null) {
	    	return builder.contentKeys().size();
	    }
	    //we don't know how much, at least one.
	    return 1;
    }
}
