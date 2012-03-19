package net.tomp2p.p2p;

import java.io.File;
import java.io.IOException;

import net.tomp2p.connection.Bindings;

public class PeerInitializer
{
	final private Peer peer;
	final private int udpPort;
	final private int tcpPort;
	final private Bindings bindings;
	final private File fileMessageLogger;
	
	// enabled - disabled
	boolean trackerEnabled = true;
		
	public PeerInitializer(Peer peer, int udpPort, int tcpPort, Bindings bindings, File fileMessageLogger)
	{
		this.peer = peer;
		this.udpPort = udpPort;
		this.tcpPort = tcpPort;
		this.bindings = bindings;
		this.fileMessageLogger = fileMessageLogger;
	}
	
	public PeerInitializer enableTracker()
	{
		peer.initTracker();
		return this;
	}
	
	public PeerInitializer setAllEnabled(boolean enabled)
	{
		setTrackerEnabled(enabled);
		return this;
	}
	
	public Peer initialize() throws IOException 
	{
		peer.listen(udpPort, tcpPort, bindings, fileMessageLogger);
		return peer;
	}
}