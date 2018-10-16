package net.tomp2p.network;

public interface KCPListener {
    void output(byte[] buffer, int offset, int length);
}
