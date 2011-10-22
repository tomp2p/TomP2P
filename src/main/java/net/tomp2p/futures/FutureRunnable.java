package net.tomp2p.futures;

public interface FutureRunnable extends Runnable
{

	void failed(String reason);

}
