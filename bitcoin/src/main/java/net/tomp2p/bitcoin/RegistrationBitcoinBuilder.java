package net.tomp2p.bitcoin;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.Registration;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * The builder for a List of {@link RegistrationBitcoin} objects.
 *
 * @author Alexander Mülli
 *
 */
public class RegistrationBitcoinBuilder {
    private RegistrationBitcoinService rs;
    private final List<KeyPair> keyPairs;

    public RegistrationBitcoinBuilder(RegistrationBitcoinService rs, List<KeyPair> keyPairs) {
        this.rs = rs;
        this.keyPairs = keyPairs;
    }

    /**
     * Facade to create RegistrationBuilder for one single Registration
     * @param rs
     * @param keyPair
     */
    public RegistrationBitcoinBuilder(RegistrationBitcoinService rs, KeyPair keyPair) {
        this.rs = rs;
        this.keyPairs = new ArrayList<KeyPair>();
        this.keyPairs.add(keyPair);
    }

    public List<Registration> start() throws InterruptedException, ExecutionException {
        List<FutureDone<?>> registrationsFutureDone = new ArrayList<FutureDone<?>>();
        // start registration
        for(KeyPair keyPair : keyPairs) {
            FutureDone<Registration> reg = rs.registerPeer(keyPair);
            registrationsFutureDone.add(reg);
        }
        // wait until all registrations are complete
        FutureDone.whenAll(registrationsFutureDone).await();
        List<Registration> registrations = new ArrayList<Registration>();
        for(FutureDone<?> reg : registrationsFutureDone) {
            registrations.add((RegistrationBitcoin) reg.object());
        }
        return registrations;
    }
}
