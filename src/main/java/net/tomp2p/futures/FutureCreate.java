package net.tomp2p.futures;

public interface FutureCreate<K extends BaseFuture>
{
	public void repeated(K future);
}
