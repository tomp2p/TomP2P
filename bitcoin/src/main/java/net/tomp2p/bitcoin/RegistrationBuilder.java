package net.tomp2p.bitcoin;

import net.tomp2p.peers.Number160;

import java.security.KeyPair;
import java.util.concurrent.ExecutionException;

public class RegistrationBuilder {
    private RegistrationService rs;
    private final KeyPair keyPair;

    public RegistrationBuilder(RegistrationService rs, KeyPair keyPair) {
        this.rs = rs;
        this.keyPair = keyPair;
    }

    public Registration start() throws InterruptedException, ExecutionException {
        // start registration
        Registration reg = rs.registerPeer(keyPair).object();
        // get peerId from new registration
        return reg;
    }
}
