package net.tomp2p.holep.manual;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class RemotePeer {

	private final Process process;
	private final CountDownLatch cl;
	private final Command[] cmd;
	private final AtomicReferenceArray<Object> results;
	public RemotePeer(Process process, CountDownLatch cl, Command[] cmd,
			AtomicReferenceArray<Object> results) {
		this.process = process;
		this.cl = cl;
		this.cmd = cmd;
		this.results = results;
	}
	public Process process() {
		return process;
	}
	public void waitFor() {
		try {
			cl.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	public Object getResult(int i) {
		return results.get(i);
	}
	public Command getCmd(int i) {
		return cmd[i];
	}
	public int resultSize() {
		return results.length();
	}
}
