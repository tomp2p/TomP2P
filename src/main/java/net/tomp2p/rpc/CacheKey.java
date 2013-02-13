package net.tomp2p.rpc;

import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

public class CacheKey {

    private final PeerAddress remotePeer;

    private final Type type;

    private final boolean signMessage;

    public CacheKey(PeerAddress remotePeer, Type type, boolean signMessage) {
        Utils.nullCheck(remotePeer, type);

        this.remotePeer = remotePeer;
        this.type = type;
        this.signMessage = signMessage;
    }

    public PeerAddress getRemotePeer() {
        return remotePeer;
    }

    public Type getType() {
        return type;
    }

    public boolean isSignMessage() {
        return signMessage;
    }

    @Override
    public int hashCode() {
        return remotePeer.hashCode() ^ type.hashCode() ^ Boolean.valueOf(signMessage).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CacheKey)) {
            return false;
        }
        CacheKey o = (CacheKey) obj;
        return remotePeer.equals(o.remotePeer) && type == o.type && signMessage == o.signMessage;
    }

}
