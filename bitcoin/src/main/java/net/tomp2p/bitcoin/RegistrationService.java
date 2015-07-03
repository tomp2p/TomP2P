package net.tomp2p.bitcoin;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;

import java.security.KeyPair;
import java.security.PublicKey;
import java.util.concurrent.ExecutionException;

/**
 * Interface for Registration Service
 *
 * @author Alexander Mülli
 *
 */
public interface RegistrationService<K> {
    RegistrationService start();

    void stop();

    FutureDone<Registration> registerPeer(KeyPair keyPair);

    boolean verify(K registration);

    boolean authenticate(K registration, Message message);

    Number160 generatePeerId(PublicKey publicKey, Long blockNonce);
}
