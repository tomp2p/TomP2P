package net.tomp2p.rpc;

public class RPC {
	//Max. 255 Commands
	public enum Commands{
		PING(), 
		PUT(), 
		GET(), 
		ADD(), 
		REMOVE(), 
		NEIGHBOR(), 
		QUIT(), 
		DIRECT_DATA(), 
		TRACKER_ADD(), 
		TRACKER_GET(), 
		PEX(), 
		DIGEST(), 
		BROADCAST(),
		PUT_META(), 
		DIGEST_BLOOMFILTER(),
		RELAY(),
		DIGEST_META_VALUES(),
		SYNC(),
		SYNC_INFO(),
		PUT_CONFIRM();
	
	public byte getNr() {
		return (byte) ordinal();
	}
	
	public static Commands find(int nr) {
		return values()[nr];
	}
		
}
}
