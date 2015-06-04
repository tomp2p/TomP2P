package net.tomp2p.rpc;

import java.security.KeyPair;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.storage.DataBuffer;

public interface SendDirectBuilderI extends ConnectionConfiguration {

    boolean isRaw();

    boolean isSign();

    boolean isStreaming();

    DataBuffer dataBuffer();

    Object object();

    KeyPair keyPair();

}
