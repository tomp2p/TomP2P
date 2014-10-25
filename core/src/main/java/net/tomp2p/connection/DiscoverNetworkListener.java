package net.tomp2p.connection;

public interface DiscoverNetworkListener {

	void dicoverNetwork(DiscoverResults discoverResults);

	void exception(Throwable throwable);

}
