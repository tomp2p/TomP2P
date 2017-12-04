package net.tomp2p.utils;

public interface ExpirationHandler<V> {

	void expired(V oldValue);

}
