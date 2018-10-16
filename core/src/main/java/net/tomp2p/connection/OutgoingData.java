package net.tomp2p.connection;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public interface OutgoingData {
    CompletableFuture<Integer> send(InetSocketAddress address, byte[] data, int offset, int length);
    InetSocketAddress localSocket();
}
