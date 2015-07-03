package net.tomp2p.bitcoin;

import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;

import java.io.File;
import java.util.*;

/**
 * Registration storage
 * read/writes verified registrations from local file
 *
 * @author Alexander Mülli
 *
 */
public class RegistrationStorageFile implements RegistrationStorage{

    //TODO: use mapDB for storage
//    private Set<Registration> registrations = new HashSet<Registration>();
    private Map<Number160,Registration> registrations = new HashMap<Number160,Registration>();

    //TODO implement file storage
    public RegistrationStorageFile(File dir, String filename) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    /**
     * lookup registration in local storage and add public key if it was already verified
     * @param registration
     * @return true if found locally
     */
    @Override
    public boolean lookup(Registration registration) {
        //look for previously verified registration for the same peerId in hashMap
        Registration verifiedRegistration = registrations.get(registration.getPeerId());
        // check if equal to current registration
        if(registration.equals(verifiedRegistration)) {
            //add public key from previously verified up transaction data
            registration.setPublicKey(verifiedRegistration.getPublicKey());
            return true;
        }
        return false;
    }

    @Override
    public void store(Registration registration) {
        registrations.put(registration.getPeerId(), registration);
    }
}
