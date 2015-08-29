package net.tomp2p.holep.manual;

import java.util.concurrent.CountDownLatch;

public class CommandSync {
	
	final private int nrPeers;
	
	private CountDownLatch[] latches;
	
	public CommandSync(int nrPeers) {
		this.nrPeers = nrPeers;
	}
	
	public void init(int nrCmds) {
		if(latches != null) {
			if(latches.length != nrCmds) {
				throw new RuntimeException("commands length mismatch");
			}
			return;
		}
		latches = new CountDownLatch[nrCmds];
		for(int i=0;i<nrCmds;i++) {
			latches[i] = new CountDownLatch(nrPeers);
		}
	}
	
	public void waitFor(int cmd) throws InterruptedException {
		latches[cmd].countDown();
		latches[cmd].await();
	}
}
