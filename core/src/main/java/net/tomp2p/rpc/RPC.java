package net.tomp2p.rpc;

public class RPC {
	public enum Commands{
		PING(0), 
		PUT(1), 
		GET(2), 
		ADD(3), 
		REMOVE(4), 
		NEIGHBOR(5), 
		QUIT(6), 
		DIRECT_DATA(7), 
		TRACKER_ADD(8), 
		TRACKER_GET(9), 
		PEX(10), 
		DIGEST(11), 
		BROADCAST(12),
		PUT_META(13);

	private final byte nr; 
	Commands(int nr) {
		this.nr = (byte)nr;
	}
	
	public byte getNr() {
		return nr;
	}
}
}