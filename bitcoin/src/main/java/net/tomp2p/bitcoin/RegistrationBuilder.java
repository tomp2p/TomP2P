package net.tomp2p.bitcoin;

import java.security.KeyPair;
import java.util.concurrent.ExecutionException;

public class RegistrationBuilder {
    private RegistrationService rs;
    private final KeyPair keyPair;

    public RegistrationBuilder(RegistrationService rs, KeyPair keyPair) {
        this.rs = rs;
        this.keyPair = keyPair;
    }

    public RegistrationBitcoin start() throws InterruptedException, ExecutionException {
        // start registration
        RegistrationBitcoin reg = rs.registerPeer(keyPair).object();
        // get peerId from new registration
        return reg;
    }
}
