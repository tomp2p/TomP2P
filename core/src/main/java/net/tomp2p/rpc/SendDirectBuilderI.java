package net.tomp2p.rpc;

import java.security.KeyPair;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.ProgressListener;
import net.tomp2p.message.Buffer;

public interface SendDirectBuilderI extends ConnectionConfiguration {

    boolean isRaw();

    ProgressListener progressListener();

    boolean isSign();

    boolean isStreaming();

    Buffer buffer();

    Object object();

    KeyPair keyPair();

}
